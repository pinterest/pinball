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

"""Validation tests for workflow parser."""
from datetime import datetime
from datetime import timedelta
import pickle
import unittest

import mock
from pinball.scheduler.schedule import OverrunPolicy
from pinball.scheduler.schedule import WorkflowSchedule
from pinball_ext.workflow.config import JobConfig
from pinball_ext.workflow.config import ScheduleConfig
from pinball_ext.workflow.config import WorkflowConfig
from pinball_ext.job_templates import CommandJobTemplate
from pinball_ext.workflow.parser import _is_name_qualified
from pinball_ext.workflow.parser import _get_qualified_name
from pinball_ext.workflow.parser import JobDef
from pinball_ext.workflow.parser import WorkflowDef


__author__ = 'Pawel Garbacki, Mao Ye, Jooseong Kim'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class ToolsTestCase(unittest.TestCase):
    def test_is_name_qualified(self):
        self.assertFalse(_is_name_qualified('workflow_job'))
        self.assertTrue(_is_name_qualified('workflow.job'))

    def test_get_qualified_name(self):
        self.assertEqual('workflow.job', _get_qualified_name('workflow',
                                                             'job'))


class JobDefTestCase(unittest.TestCase):
    def setUp(self):
        self.workflow = WorkflowDef('some_workflow', 'some_schedule',
                                    'some_emails')
        template = CommandJobTemplate('some_template', 'some_command')
        output_template = CommandJobTemplate('output_template', 'output_command', priority=30.0)
        self.job = JobDef('some_job', template, self.workflow)
        self.output_job = JobDef('output_job', output_template, self.workflow)
        self.job.outputs = [self.output_job]

    def test_to_string(self):
        """Test conversion of job to string."""

        self.assertEqual('job:some_workflow.some_job, deps:[]', str(self.job))

    def test_get_qualified_name(self):
        """Test construction of qualified job names."""

        self.assertEqual('some_workflow.some_job',
                         self.job.get_qualified_name())

    def test_get_canonical_name(self):
        """Test construction of canonical job names."""

        self.assertEqual('some_other_workflow_some_workflow_some_job',
                         self.job.get_canonical_name('some_other_workflow'))

    def test_compute_score(self):
        """Test the compute_score method of jobs"""
        self.assertEqual(30.0, self.output_job.compute_score())
        self.assertEqual(31.0, self.job.compute_score())


class WorkflowDefTestCase(unittest.TestCase):
    def setUp(self):
        # 1370354400 = 06/04/2012 2:00p UTC
        self.next_run_time = 1370354400
        # 1370289240 = 06/03/2013 7:53p UTC
        self.previous_run_time = 1370289240
        self.workflow_schedule = WorkflowSchedule(next_run_time=self.next_run_time,
                                                  recurrence_seconds=24 * 60 * 60,
                                                  workflow='some_workflow',
                                                  emails=['some_email'])
        self.workflow = WorkflowDef('some_workflow',
                                    self.workflow_schedule,
                                    'some_emails')
        self.job1 = JobDef('some_job_1', CommandJobTemplate('some_template1', 'some_command1'),
                           self.workflow)
        self.workflow.add_job(self.job1)
        self.job2 = JobDef('some_job_2', CommandJobTemplate('some_template2', 'some_command2'),
                           self.workflow)
        self.workflow.add_job(self.job2)
        self.job2.add_dep(self.job1)
        self.job3 = JobDef('some_job_3', CommandJobTemplate('some_template3', 'some_command3'),
                           self.workflow)
        self.workflow.add_job(self.job3)
        self.job4 = JobDef('some_job_4', CommandJobTemplate('some_template4', 'some_command4'),
                           self.workflow)
        self.workflow.add_job(self.job4)
        self.job4.add_dep(self.job3)

    def _add_final_job(self):
        template = CommandJobTemplate('some_template', 'some_command')
        final_job = JobDef('final_job', template, self.workflow)
        self.workflow.add_job(final_job)
        final_job.add_dep(self.job2)
        final_job.add_dep(self.job4)

    def _add_external_deps(self):
        other_workflow = WorkflowDef('some_other_workflow', 'some_schedule',
                                     'some_emails')
        template = CommandJobTemplate('some_template', 'some_command')
        parent_external_job = JobDef('parent_external_job', template,
                                     other_workflow)
        other_workflow.add_job(parent_external_job)
        external_job = JobDef('external_job', template, other_workflow)
        external_job.add_dep(parent_external_job)
        other_workflow.add_job(external_job)
        self.job1.add_dep(external_job)

    def test_to_string(self):
        """Test conversion of job to string."""

        self.assertEqual(
            "workflow:some_workflow, schedule:WorkflowSchedule(next_run_time=2013-06-04 "
            "14:00:00 UTC, recurrence=1 day, 0:00:00, overrun_policy=SKIP, "
            "parser_params={'caller': 'schedule'}, "
            "workflow=some_workflow, email=['some_email'], max_running_instances=3), "
            "notify_emails:some_emails, jobs:{'some_job_3': "
            "job:some_workflow.some_job_3, deps:[], 'some_job_2': "
            "job:some_workflow.some_job_2, deps:[some_workflow.some_job_1], "
            "'some_job_1': job:some_workflow.some_job_1, deps:[], "
            "'some_job_4': job:some_workflow.some_job_4, "
            "deps:[some_workflow.some_job_3]}", str(self.workflow))

    def test_get_leaf_jobs(self):
        """Test retrieval of leaf jobs."""

        leaf_jobs = self.workflow.get_leaf_jobs()
        leaf_jobs.sort()
        expected_jobs = [self.job2, self.job4]
        expected_jobs.sort()
        self.assertEqual(expected_jobs, leaf_jobs)

    def test_verify(self):
        """Test workflow verification."""

        # TODO(pawel): figure out why putting this at the top level does not
        # work.
        from pinball_ext.workflow.parser import WorkflowVerificationException

        # Raise an exception if the workflow does not have a final job.
        self.assertRaises(WorkflowVerificationException, self.workflow.verify)

        # Verify that the workflow has a final job and is acyclic.
        self._add_final_job()
        self.workflow.verify()

        # Raise an exception if the workflow has a cycle.
        self.job1.add_dep(self.job2)
        self.assertRaises(Exception, self.workflow.verify)

    @mock.patch('time.time')
    def test_get_schedule_token(self, time_mock):
        # the value must be a float
        time_mock.return_value = 1.0 * self.previous_run_time
        self._add_final_job()
        token = self.workflow.get_schedule_token()
        self.assertEqual('/schedule/workflow/some_workflow', token.name)
        schedule = pickle.loads(token.data)
        self.assertEqual(self.next_run_time, schedule.next_run_time)
        self.assertEqual(24 * 60 * 60, schedule.recurrence_seconds)
        self.assertEqual(OverrunPolicy.SKIP, schedule.overrun_policy)
        self.assertEqual('some_workflow', schedule.workflow)

    def test_get_workflow_tokens(self):
        self._add_final_job()
        self._add_external_deps()
        tokens = self.workflow.get_workflow_tokens()
        # There should be 5 (4 local and 1 external) workflow job tokens, 1
        # final job token, and 2 top-level job event tokens.
        self.assertEqual(5 + 1 + 2, len(tokens))


class PyWorkflowParserTestCase(unittest.TestCase):
    def test_get_schedule_token(self):
        # Due to a wired interaction between tests caused by dynamic module
        # imports, the following import statement cannot be placed at the top
        # level.  E.g., see
        # http://stackoverflow.com/questions/9722343/python-super-behavior-not-dependable
        from pinball_ext.workflow.parser import PyWorkflowParser
        params =\
            {'workflows_config':
                 'tests.pinball_ext.workflow.parser_test.WORKFLOWS'}
        py_workflow_parser = PyWorkflowParser(params)
        schedule_token = py_workflow_parser.get_schedule_token('some_workflow')
        self.assertEqual('/schedule/workflow/some_workflow',
                         schedule_token.name)

    def test_get_workflow_tokens(self):
        from pinball_ext.workflow.parser import PyWorkflowParser
        params =\
            {'workflows_config':
                 'tests.pinball_ext.workflow.parser_test.WORKFLOWS'}
        py_workflow_parser = PyWorkflowParser(params)
        workflow_tokens = py_workflow_parser.get_workflow_tokens(
            'some_workflow')
        # Two job tokens and one event token.
        self.assertEqual(3, len(workflow_tokens))

    def test_parse_workflows(self):
        """Test parsing WORKFLOWS."""
        from pinball_ext.workflow.parser import PyWorkflowParser
        params =\
            {'workflows_config':
                 'tests.pinball_ext.workflow.parser_test.WORKFLOWS'}
        py_workflow_parser = PyWorkflowParser(params)
        py_workflow_parser.parse_workflows()
        self.assertEquals(3, len(py_workflow_parser.workflows))
        self.assertEquals(2, len(
            py_workflow_parser.workflows['some_workflow'].jobs))
        self.assertEquals(3, len(
            py_workflow_parser.workflows['some_other_workflow'].jobs))
        self.assertEquals(3, len(
            py_workflow_parser.workflows['another_workflow'].jobs))

        self.assertEquals(24 * 60 * 60,
                          py_workflow_parser.workflows['some_workflow'].
                          schedule.recurrence_seconds)
        self.assertEquals(60 * 60,
                          py_workflow_parser.workflows['some_other_workflow'].
                          schedule.recurrence_seconds)
        self.assertEquals(60 * 60,
                          py_workflow_parser.workflows['another_workflow'].
                          schedule.recurrence_seconds)
        self.assertEquals(py_workflow_parser.workflows_config_str,
                          'tests.pinball_ext.workflow.parser_test.WORKFLOWS')

FINAL_JOB_CONFIG = JobConfig(CommandJobTemplate('final', 'true'))
WORKFLOWS = {
    'some_workflow': WorkflowConfig(
        jobs={
            'some_job': JobConfig(
                CommandJobTemplate('some_job', 'true'), [])
        },
        final_job_config=FINAL_JOB_CONFIG,
        schedule=ScheduleConfig(recurrence=timedelta(days=1),
                                reference_timestamp=datetime(
                                    year=2015, month=2, day=1, second=1)),
        notify_emails='foo@pinterest.com'),
    'some_other_workflow': WorkflowConfig(
        jobs={
            'some_other_job': JobConfig(
                CommandJobTemplate('some_other_job', 'true'),
                []),

            'yet_another_job': JobConfig(
                CommandJobTemplate('yet_another_job', 'true'),
                ['some_other_job'])
        },
        final_job_config=FINAL_JOB_CONFIG,
        schedule=ScheduleConfig(recurrence=timedelta(hours=1),
                                reference_timestamp=datetime(
                                    year=2015, month=2, day=1, second=2)),
        notify_emails='foo@pinterest.com'),
    'another_workflow': WorkflowConfig(
        jobs={
            'another_job': JobConfig(
                CommandJobTemplate('another_job', 'true'),
                []),

            'yet_some_other_job': JobConfig(
                CommandJobTemplate('yet_some_other_job', 'true'),
                ['another_job'])
        },
        final_job_config=FINAL_JOB_CONFIG,
        schedule=ScheduleConfig(recurrence=timedelta(hours=1),
                                reference_timestamp=datetime(
                                    year=2015, month=2, day=1, second=2)),
        notify_emails='bar@pinterest.com'),
}
