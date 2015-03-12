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

"""Validation tests for data builder."""
import mock
import unittest

from pinball.config.utils import PinballException
from pinball.ui.data import Status
from pinball.ui.data_builder import DataBuilder
from pinball.workflow.signaller import Signal
from tests.pinball.persistence.ephemeral_store import EphemeralStore
from tests.pinball.persistence.data_generator import \
    generate_workflows


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class DataBuilderTestCase(unittest.TestCase):
    def setUp(self):
        self._store = EphemeralStore()
        self._data_builder = DataBuilder(self._store)

    @mock.patch('os.makedirs')
    @mock.patch('__builtin__.open')
    def _add_tokens(self, _, __):
        generate_workflows(2, 2, 2, 2, 2, self._store)

    def test_get_workflows_empty(self):
        self.assertEqual([], self._data_builder.get_workflows())

    def _get_workflows(self):
        self._add_tokens()
        workflows = self._data_builder.get_workflows()
        self.assertEqual(4, len(workflows))
        workflow_status = {'workflow_0': Status.RUNNING,
                           'workflow_1': Status.RUNNING,
                           'workflow_2': Status.SUCCESS,
                           'workflow_3': Status.FAILURE}
        for workflow in workflows:
            self.assertEqual(workflow_status[workflow.workflow],
                             workflow.status)
            self.assertEqual('instance_1', workflow.last_instance)
            del workflow_status[workflow.workflow]
        self.assertEqual({}, workflow_status)

    def test_get_workflows(self):
        self._get_workflows()

    def test_get_workflows_using_cache(self):
        self._data_builder.use_cache = True
        self._get_workflows()
        # Only finished (archived) workflow instances should have been cached.
        expected_cached_names = ['/workflow/workflow_2/instance_0/',
                                 '/workflow/workflow_2/instance_1/',
                                 '/workflow/workflow_3/instance_0/',
                                 '/workflow/workflow_3/instance_1/']
        cached_names = sorted(self._store.read_cached_data_names())
        self.assertEqual(expected_cached_names, cached_names)

    def test_get_workflow_empty(self):
        self.assertIsNone(self._data_builder.get_workflow('does_not_exist'))

    def _get_workflow(self):
        self._add_tokens()
        workflow = self._data_builder.get_workflow('workflow_0')
        self.assertEqual('workflow_0', workflow.workflow)
        self.assertEqual(Status.RUNNING, workflow.status)
        self.assertEqual('instance_1', workflow.last_instance)

    def test_get_workflow(self):
        self._get_workflow()

    def test_get_workflow_using_cache(self):
        self._data_builder.use_cache = True
        self._get_workflow()
        # Instances of a running workflow should not have been cached.
        self.assertEqual([], self._store.read_cached_data_names())

    def test_get_instances_empty(self):
        self.assertEqual([],
                         self._data_builder.get_instances('does_not_exist'))

    def _get_instances(self):
        self._add_tokens()
        instances = self._data_builder.get_instances('workflow_2')
        self.assertEqual(2, len(instances))
        instance_status = [Status.SUCCESS, Status.FAILURE]
        for instance in instances:
            self.assertEqual('workflow_2', instance.workflow)
            instance_status.remove(instance.status)
        self.assertEqual([], instance_status)

    def test_get_instances(self):
        self._get_instances()

    def test_get_instances_using_cache(self):
        self._data_builder.use_cache = True
        self._get_instances()
        expected_cached_names = ['/workflow/workflow_2/instance_0/',
                                 '/workflow/workflow_2/instance_1/']
        cached_names = sorted(self._store.read_cached_data_names())
        self.assertEqual(expected_cached_names, cached_names)

    def test_get_instance_empty(self):
        self.assertIsNone(None,
                          self._data_builder.get_instance('does_not_exist',
                                                          'instance_0'))

    def _get_instance(self):
        self._add_tokens()
        instance = self._data_builder.get_instance('workflow_0', 'instance_0')
        self.assertEqual('workflow_0', instance.workflow)
        self.assertEqual('instance_0', instance.instance)

    def test_get_instance(self):
        self._get_instance()

    def test_get_instance_using_cache(self):
        self._data_builder.use_cache = True
        self._get_instance()
        # Running instance should not have been cached.
        self.assertEqual([], self._store.read_cached_data_names())

    def test_get_jobs_empty(self):
        self.assertEqual([],
                         self._data_builder.get_jobs('does_not_exist',
                                                     'does_not_exist'))

    def test_get_jobs(self):
        self._add_tokens()
        jobs = self._data_builder.get_jobs('workflow_0', 'instance_0')
        self.assertEqual(2, len(jobs))
        for job in jobs:
            self.assertEqual('workflow_0', job.workflow)
            self.assertEqual('instance_0', job.instance)
            self.assertEqual('ShellJob', job.job_type)
            self.assertTrue(job.info.startswith('command=some command'))
            self.assertEqual(Status.FAILURE, job.status)
        self.assertEqual([(0, ''), (1, 'SUCCESS'), (9, 'FAILURE')],
                         jobs[0].progress)
        self.assertEqual([(89, ''), (1, 'SUCCESS'), (9, 'FAILURE')],
                         jobs[1].progress)

    def test_get_executions_empty(self):
        self.assertEqual([],
                         self._data_builder.get_executions('does_not_exist',
                                                           'does_not_exist',
                                                           'does_not_exist'))

    def test_get_executions(self):
        self._add_tokens()
        executions = self._data_builder.get_executions('workflow_0',
                                                       'instance_0',
                                                       'job_0')
        self.assertEqual(2, len(executions))
        exit_codes = [0, 1]
        for execution in executions:
            self.assertEqual('workflow_0', execution.workflow)
            self.assertEqual('instance_0', execution.instance)
            self.assertEqual('job_0', execution.job)
            self.assertTrue(execution.info.startswith('some_command'))
            exit_codes.remove(execution.exit_code)
            self.assertEqual(2, len(execution.logs))

    def test_get_executions_across_instances_empty(self):
        self.assertEqual([],
                         self._data_builder.get_executions_across_instances(
                             'does_not_exist',
                             'does_not_exist'))

    def test_get_executions_across_instances(self):
        self._add_tokens()
        executions = self._data_builder.get_executions_across_instances(
            'workflow_0', 'job_0')
        self.assertEqual(2 * 2, len(executions))
        exit_codes = [0, 0, 1, 1]
        for execution in executions:
            self.assertEqual('workflow_0', execution.workflow)
            self.assertEqual('job_0', execution.job)
            self.assertTrue(execution.info.startswith('some_command'))
            exit_codes.remove(execution.exit_code)
            self.assertEqual(2, len(execution.logs))

    def test_get_execution_empty(self):
        self.assertIsNone(self._data_builder.get_execution('does_not_exist',
                                                           'does_not_exist',
                                                           'does_not_exist',
                                                           0))

    def test_get_execution(self):
        self._add_tokens()
        execution = self._data_builder.get_execution('workflow_0',
                                                     'instance_0',
                                                     'job_0',
                                                     1)
        self.assertEqual('workflow_0', execution.workflow)
        self.assertEqual('instance_0', execution.instance)
        self.assertEqual('job_0', execution.job)
        self.assertEqual(1, execution.execution)
        self.assertEqual('some_command 1 some_args 1', execution.info)
        self.assertEqual(1, execution.exit_code)
        self.assertEqual(2, execution.start_time)
        self.assertEqual(13, execution.end_time)
        self.assertEqual(2, len(execution.logs))

    @mock.patch('__builtin__.open')
    def test_get_file_content_no_file(self, _):
        self.assertEqual('',
                         self._data_builder.get_file_content('does_not_exist',
                                                             'does_not_exist',
                                                             'does_not_exist',
                                                             'does_not_exist',
                                                             'does_not_exist'))

    @mock.patch('os.makedirs')
    @mock.patch('__builtin__.open')
    def test_get_file_content(self, open_mock, _):
        generate_workflows(2, 2, 2, 2, 2, self._store)

        file_mock = mock.MagicMock()
        open_mock.return_value = file_mock
        file_mock.__enter__.return_value = file_mock
        file_mock.read.return_value = 'some_content'

        content = self._data_builder.get_file_content('workflow_0',
                                                      'instance_0',
                                                      'job_0',
                                                      0,
                                                      'info')
        self.assertEqual('some_content', content)

    def test_get_token_paths_empty(self):
        self.assertRaises(PinballException,
                          self._data_builder.get_token_paths,
                          '')

    def test_get_token_paths(self):
        self._add_tokens()
        token_paths = self._data_builder.get_token_paths(
            '/workflow/workflow_0/instance_0/job/waiting/')
        self.assertEqual(2, len(token_paths))
        paths = ['/workflow/workflow_0/instance_0/job/waiting/job_0',
                 '/workflow/workflow_0/instance_0/job/waiting/job_1']
        for token_path in token_paths:
            self.assertEqual(1, token_path.count)
            paths.remove(token_path.path)
        self.assertEqual([], paths)

    def test_get_token_empty(self):
        self.assertRaises(PinballException,
                          self._data_builder.get_token,
                          '/does_not_exist')

    def test_get_token(self):
        self._add_tokens()
        token = self._data_builder.get_token(
            '/workflow/workflow_0/instance_0/job/waiting/job_0')
        self.assertEqual('/workflow/workflow_0/instance_0/job/waiting/job_0',
                         token.name)
        self.assertIsNone(token.owner)
        self.assertIsNone(token.expiration_time)
        self.assertEqual(0, token.priority)
        self.assertIsNotNone(token.data)

    def test_signal_not_set(self):
        self.assertFalse(self._data_builder.is_signal_set('does_not_exist', 0,
                                                          Signal.DRAIN))

    def test_signal_set(self):
        self._add_tokens()
        self.assertTrue(self._data_builder.is_signal_set('workflow_0', 0,
                                                         Signal.DRAIN))
