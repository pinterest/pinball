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

"""Analyzer allows to manipulate workflow instance tokens.

Only archived workflow instances can be analyzed.  Tokens of an archived
workflow instance are immutable so we don't need to worry that the workflow
state will change while we manipulate the graph.
"""
import pickle

from pinball.config.pinball_config import PinballConfig
from pinball.master.thrift_lib.ttypes import Query
from pinball.master.thrift_lib.ttypes import QueryRequest
from pinball.master.thrift_lib.ttypes import Token
from pinball.parser.config_parser import ParserCaller
from pinball.parser.utils import load_parser_with_caller
from pinball.workflow.name import Name
from pinball.workflow.event import Event


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class Analyzer(object):
    def __init__(self, workflow, instance):
        self._workflow = workflow
        self._instance = instance
        self._jobs = {}
        self._existing_events = {}
        self._new_events = {}
        self._job_priorities = {}

    @staticmethod
    def from_store(store, workflow, instance):
        """Construct analyzer from tokens in a store.

        Args:
            store: The store to read tokens from.
            workflow: The workflow whose tokens should be read.
            instance: The instance whose tokens should be read.

        Returns:
            Analyzer initialized with tokens read from the store.
        """
        analyzer = Analyzer(workflow, instance)
        analyzer._read_tokens_from_store(store)
        return analyzer

    @staticmethod
    def from_parser_params(workflow):
        """Construct analyzer from tokens of a workflow according
        to the parser params configuration.

        Args:
            workflow: The workflow whose tokens should be read.

        Returns:
            Analyzer initialized with tokens read from the config.
        """
        analyzer = Analyzer(workflow, None)
        analyzer._read_tokens_from_parser_params()
        return analyzer

    @staticmethod
    def from_client(client, workflow, instance):
        """Construct analyzer from tokens of a workflow in the master.

        Args:
            client: The client connected to the master.
            workflow: The workflow whose tokens should be read.
            instance: The instance whose tokens should be read.

        Returns:
            Analyzer initialized with tokens read from the client.
        """
        analyzer = Analyzer(workflow, instance)
        analyzer._read_tokens_from_client(client)
        return analyzer

    def _filter_job_tokens(self, tokens):
        """Filter out all tokens which are not job tokens.

        Args:
            tokens: The tokens to filter.
        """
        for token in tokens:
            name = Name.from_job_token_name(token.name)
            if not self._instance and name.instance:
                self._instance = name.instance
            if name.job:
                job = pickle.loads(token.data)
                self._jobs[job.name] = job
                self._job_priorities[job.name] = token.priority

    def _filter_event_tokens(self, tokens):
        """Filter out all tokens which are not event tokens.

        Args:
            tokens: The tokens to filter.
        """
        for token in tokens:
            name = Name.from_event_token_name(token.name)
            if not self._instance and name.instance:
                self._instance = name.instance
            if name.event:
                event = pickle.loads(token.data)
                self._existing_events[token.name] = event

    def _read_tokens_from_store(self, store):
        """Read archived job tokens from the store.

        Args:
            store: The store to read tokens from.
        """
        name = Name(workflow=self._workflow, instance=self._instance)
        tokens = store.read_archived_tokens(
            name_prefix=name.get_instance_prefix())
        self._filter_job_tokens(tokens)

    def _read_tokens_from_parser_params(self):
        """Read archived job tokens from the PinballConfig.PARSER_PARAMS.
        """
        config_parser = load_parser_with_caller(PinballConfig.PARSER,
                                                PinballConfig.PARSER_PARAMS,
                                                ParserCaller.ANALYZER)
        tokens = config_parser.get_workflow_tokens(self._workflow)
        self._filter_job_tokens(tokens)

    def _read_tokens_from_client(self, client):
        """Read archived job tokens from the client.

        Args:
            client: The client to read tokens from.
        """
        name = Name(workflow=self._workflow, instance=self._instance)
        query = Query(namePrefix=name.get_workflow_prefix())
        request = QueryRequest(queries=[query])
        response = client.query(request)
        assert len(response.tokens) == 1
        tokens = response.tokens[0]
        self._filter_job_tokens(tokens)
        self._filter_event_tokens(tokens)

    def get_tokens(self):
        """Export all internally stored tokens.

        Returns:
            The list of tokens after all transformations performed by the
            analyzer.
        """
        result = []
        for job in self._jobs.values():
            name = Name(workflow=self._workflow, instance=self._instance,
                        job_state=Name.WAITING_STATE, job=job.name)
            data = pickle.dumps(job)
            token = Token(name=name.get_job_token_name(),
                          priority=self._job_priorities[job.name],
                          data=data)
            result.append(token)
        result.extend(self.get_new_event_tokens())
        return result

    def get_new_event_tokens(self):
        """Export new event tokens.

        Returns:
            The list of new event tokens after all transformations performed by
            the analyzer.
        """
        result = []
        for event_name, event in self._new_events.items():
            data = pickle.dumps(event)
            token = Token(name=event_name, data=data)
            result.append(token)
        return result

    def _find_descendants(self, job_name):
        """Find direct and indirect descendants of a job.

        Args:
            job_name: The name of the job whose descendants should be computed.

        Returns:
            The set of job descendants.
        """
        def _dfs(current, visited):
            if not current in visited:
                visited.add(current)
                for child in current.outputs:
                    _dfs(self._jobs[child], visited)
        visited = set()
        _dfs(self._jobs[job_name], visited)
        result = set()
        for job in visited:
            result.add(job.name)
        return result

    def _generate_missing_events(self, job_names):
        """Generate external events required to run all jobs in a set.

        For a set of jobs (a subset of all jobs in the workflow), produce
        events satisfying upstream dependencies external to that set.  E.g.,
        for job dependency structure like this:

        A1  A2
         | /
        B1  B2
         |
        C1  C2
         | /
        D1

        and job_names = (C1, D1) we would generate events satisfying the
        following deps: B1->C1, C2->D1.

        Args:
            job_names: The set of job names whose external deps are to be
                satisfied by the generated events.
        """
        input_prefixes = set()
        for job_name in job_names:
            job = self._jobs[job_name]
            for job_input in job.inputs:
                if job_input not in job_names:
                    name = Name(workflow=self._workflow,
                                instance=self._instance,
                                job=job_name,
                                input_name=job_input,
                                event='poison_%d' % len(input_prefixes))
                    input_prefix = name.get_input_prefix()
                    if input_prefix not in input_prefixes:
                        input_prefixes.add(input_prefix)
                        event_token_name = name.get_event_token_name()
                        if not event_token_name in self._existing_events:
                            self._new_events[
                                name.get_event_token_name()] = Event(
                                    'analyzer')

    def poison(self, roots):
        """Poison the workflow instance at specified root jobs.

        Poisoning is a process of choosing a subset of jobs to run based on
        the dependencies.  Only jobs that depend directly or indirectly on any
        job in the root set will be executed.  Poisoning will generate events
        for dependencies which are not satisfied by the roots or their
        dependents.

        Args:
            roots: The list of job names which are roots of the poisoning.
        """
        descendants = set()
        for root in roots:
            jobs = self._find_descendants(root)
            descendants = descendants.union(jobs)
        self._generate_missing_events(descendants)

    def change_instance(self, instance):
        """Move all tokens to a specific instance."""
        self._instance = instance
        new_events = {}
        for event_name, event in self._new_events.items():
            name = Name.from_event_token_name(event_name)
            name.instance = instance
            new_events[name.get_event_token_name()] = event
        self._new_events = new_events

    def clear_job_histories(self):
        """Remove histories from all job tokens."""
        for job in self._jobs.values():
            job.history = []
