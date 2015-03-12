# Copyright 2015, Pinterest, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Validation tests for the analyzer."""
import collections
import pickle
import unittest

from pinball.master.thrift_lib.ttypes import Token
from pinball.workflow.analyzer import Analyzer
from pinball.workflow.event import Event
from pinball.workflow.job import ShellJob
from pinball.workflow.job_executor import ExecutionRecord
from pinball.workflow.name import Name
from tests.pinball.persistence.ephemeral_store import EphemeralStore


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class AnalyzerTestCase(unittest.TestCase):
    _NUM_LEVELS = 3

    def setUp(self):
        self._store = EphemeralStore()

    def _add_active_workflow_tokens(self):
        """Add some active workflow tokens.

        The job dependencies form a complete binary tree turned upside down.
        I.e., each job has two parents.
        """
        self._store = EphemeralStore()
        version = 1
        for level in range(AnalyzerTestCase._NUM_LEVELS):
            jobs_at_level = 2 ** (AnalyzerTestCase._NUM_LEVELS - level - 1)
            for job_index in range(jobs_at_level):
                job_name = 'job_%d_%d' % (level, job_index)
                event_name = Name(workflow='some_workflow',
                                  instance='123',
                                  job=job_name,
                                  event='some_event')
                if level == 0:
                    inputs = [Name.WORKFLOW_START_INPUT,
                              Name.WORKFLOW_START_INPUT + '_prime']
                    event_name.input = Name.WORKFLOW_START_INPUT
                else:
                    inputs = ['job_%d_%d' % (level - 1, 2 * job_index),
                              'job_%d_%d' % (level - 1, 2 * job_index + 1)]
                    event_name.input = 'job_%d_%d' % (level - 1, 2 * job_index)
                if level == AnalyzerTestCase._NUM_LEVELS - 1:
                    outputs = []
                else:
                    outputs = ['job_%d_%d' % (level + 1, job_index / 2)]
                job = ShellJob(name=job_name,
                               inputs=inputs,
                               outputs=outputs,
                               command='some_command')
                job.history.append(ExecutionRecord())
                name = Name(workflow='some_workflow', instance='123',
                            job_state=Name.WAITING_STATE, job=job_name)
                job_token = Token(version=version,
                                  name=name.get_job_token_name(),
                                  priority=10,
                                  data=pickle.dumps(job))
                version += 1
                event = Event('some_event')
                event_token = Token(version=version,
                                    name=event_name.get_event_token_name(),
                                    priority=10,
                                    data=pickle.dumps(event))
                self._store.commit_tokens([job_token, event_token])

    def _archive_tokens(self):
        tokens = self._store.read_active_tokens()
        self._store.archive_tokens(tokens)
        return tokens

    def _simulate(self):
        """Simulate execution of active jobs."""
        tokens = self._store.read_tokens()
        satisfied_deps = set()
        executed_jobs = []
        jobs = {}
        for token in tokens:
            event_name = Name.from_event_token_name(token.name)
            if event_name.event:
                satisfied_deps.add((event_name.input, event_name.job))
            else:
                job_name = Name.from_job_token_name(token.name)
                if job_name.job:
                    job = pickle.loads(token.data)
                    jobs[job.name] = job
        dep_counts = collections.defaultdict(int)
        while satisfied_deps:
            last_satisfied_deps = satisfied_deps
            satisfied_deps = set()
            for (_, job_name) in last_satisfied_deps:
                dep_counts[job_name] += 1
                if dep_counts[job_name] == 2:
                    executed_jobs.append(job_name)
                    job = jobs[job_name]
                    for output in job.outputs:
                        satisfied_deps.add((job_name, output))
        return executed_jobs

    def test_change_instance(self):
        self._add_active_workflow_tokens()
        self._archive_tokens()
        analyzer = Analyzer.from_store(self._store, 'some_workflow', '123')
        analyzer.change_instance('321')
        tokens = analyzer.get_tokens()
        self.assertLess(0, len(tokens))
        for token in tokens:
            name = Name.from_job_token_name(token.name)
            self.assertEqual('321', name.instance)

    def test_change_job_histories(self):
        self._add_active_workflow_tokens()
        self._archive_tokens()
        analyzer = Analyzer.from_store(self._store, 'some_workflow', '123')
        analyzer.clear_job_histories()
        tokens = analyzer.get_tokens()
        self.assertLess(0, len(tokens))
        for token in tokens:
            job = pickle.loads(token.data)
            self.assertEqual([], job.history)

    def test_poison_no_tokens(self):
        analyzer = Analyzer.from_store(self._store, 'some_workflow', '123')
        analyzer.poison([])

    def test_poison_no_roots(self):
        self._add_active_workflow_tokens()
        self._archive_tokens()
        analyzer = Analyzer.from_store(self._store, 'some_workflow', '123')
        analyzer.poison([])
        tokens = analyzer.get_tokens()
        self._store.commit_tokens(updates=tokens)
        executed_jobs = self._simulate()
        self.assertEqual([], executed_jobs)

    def test_poison_all(self):
        """Poison all top level jobs."""
        self._add_active_workflow_tokens()
        self._archive_tokens()
        analyzer = Analyzer.from_store(self._store, 'some_workflow', '123')

        roots = []
        for job_index in range(0, 2 ** (AnalyzerTestCase._NUM_LEVELS - 1)):
            roots.append('job_0_%d' % job_index)

        analyzer.poison(roots)
        tokens = analyzer.get_tokens()
        self._store.commit_tokens(updates=tokens)
        executed_jobs = self._simulate()
        # We expect that every job has run.
        expected_num_executed_jobs = 2 ** (AnalyzerTestCase._NUM_LEVELS) - 1
        self.assertEqual(expected_num_executed_jobs, len(executed_jobs))

    def test_poison_subset(self):
        """Poison every second top level job."""
        self._add_active_workflow_tokens()
        self._archive_tokens()
        analyzer = Analyzer.from_store(self._store, 'some_workflow', '123')

        roots = []
        for job_index in range(0, 2 ** (AnalyzerTestCase._NUM_LEVELS - 1), 2):
            roots.append('job_0_%d' % job_index)

        analyzer.poison(roots)
        tokens = analyzer.get_tokens()
        self._store.commit_tokens(updates=tokens)
        executed_jobs = self._simulate()
        # We expect that every second job at the top level and every job at
        # a lower level was run.
        expected_num_executed_jobs = (
            2 ** (AnalyzerTestCase._NUM_LEVELS - 1) - 1 +
            2 ** (AnalyzerTestCase._NUM_LEVELS - 1) / 2)
        self.assertEqual(expected_num_executed_jobs, len(executed_jobs))

    def test_poison_get_new_event_tokens(self):
        """Poison all top level jobs and get new event tokens."""
        self._add_active_workflow_tokens()
        tokens = self._archive_tokens()
        analyzer = Analyzer.from_store(self._store, 'some_workflow', '123')
        analyzer._filter_event_tokens(tokens)

        roots = []
        for job_index in range(0, 2 ** (AnalyzerTestCase._NUM_LEVELS - 1)):
            roots.append('job_0_%d' % job_index)

        analyzer.poison(roots)
        tokens = analyzer.get_new_event_tokens()
        expected_num_new_event_tokens = 2 ** AnalyzerTestCase._NUM_LEVELS
        self.assertEqual(expected_num_new_event_tokens, len(tokens))
