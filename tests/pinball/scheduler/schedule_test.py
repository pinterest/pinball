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

"""Validation tests for schedules."""
import mock
import unittest

from pinball.master.thrift_lib.ttypes import Token
from pinball.parser.config_parser import PARSER_CALLER_KEY
from pinball.scheduler.overrun_policy import OverrunPolicy
from pinball.scheduler.schedule import Schedule
from pinball.scheduler.schedule import WorkflowSchedule
from pinball.ui.data import Status
from pinball.ui.data import WorkflowData
from pinball.ui.data import WorkflowInstanceData
from pinball.workflow.emailer import Emailer
from pinball.workflow.name import Name
from pinball.workflow.signaller import Signal
from tests.pinball.persistence.ephemeral_store import EphemeralStore


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class ScheduleTestCase(unittest.TestCase):
    class FakeSchedule(Schedule):
        def run(self, emailer, store):
            return None

        def is_running(self):
            return False

        def is_failed(self):
            return False

        def abort_running(self, client, store):
            return

    @mock.patch('time.time')
    def test_advance_next_run_time(self, time_mock):
        time_mock.return_value = 100.  # the value must be a float
        schedule = ScheduleTestCase.FakeSchedule(next_run_time=95,
                                                 recurrence_seconds=10)
        schedule.advance_next_run_time()
        self.assertTrue(105, schedule.next_run_time)

    def test_corresponds_to(self):
        some_schedule = ScheduleTestCase.FakeSchedule(next_run_time=10,
                                                      recurrence_seconds=10)
        corresponding_schedule = ScheduleTestCase.FakeSchedule(
            next_run_time=20, recurrence_seconds=10)
        self.assertTrue(some_schedule.corresponds_to(corresponding_schedule))
        non_corresponding_schedule = ScheduleTestCase.FakeSchedule(
            next_run_time=25, recurrence_seconds=10)
        self.assertFalse(some_schedule.corresponds_to(
                         non_corresponding_schedule))
        other_non_corresponding_schedule = ScheduleTestCase.FakeSchedule(
            next_run_time=10, recurrence_seconds=10,
            overrun_policy=OverrunPolicy.DELAY)
        self.assertFalse(some_schedule.corresponds_to(
                         other_non_corresponding_schedule))


class WorkflowScheduleTestCase(unittest.TestCase):
    def test_corresponds_to(self):
        some_schedule = WorkflowSchedule(next_run_time=20,
                                         recurrence_seconds=10,
                                         workflow='some_workflow')
        corresponding_schedule = WorkflowSchedule(next_run_time=10,
                                                  recurrence_seconds=10,
                                                  workflow='some_workflow')
        self.assertTrue(some_schedule.corresponds_to(corresponding_schedule))
        non_corresponding_schedule = WorkflowSchedule(
            next_run_time=20, recurrence_seconds=10,
            workflow='some_other_workflow')
        self.assertFalse(some_schedule.corresponds_to(
                         non_corresponding_schedule))

    @mock.patch('pinball.scheduler.schedule.load_parser_with_caller')
    def test_run(self, load_path_mock):
        config_parser = mock.Mock()
        load_path_mock.return_value = config_parser
        name = Name(workflow='some_workflow',
                    instance='123',
                    job_state=Name.WAITING_STATE,
                    job='some_job')
        config_parser.get_workflow_tokens.return_value = [
            Token(name=name.get_job_token_name())]

        schedule = WorkflowSchedule(workflow='some_workflow')
        store = EphemeralStore()
        emailer = Emailer('some_host', '8080')
        request = schedule.run(emailer, store)
        self.assertEqual(
            load_path_mock.call_args_list,
            [
                mock.call('pinball_ext.workflow.parser.PyWorkflowParser',
                          {},
                          'schedule')
            ]
        )

        self.assertEqual(1, len(request.updates))

    @mock.patch('pinball.scheduler.schedule.DataBuilder')
    def test_is_running(self, data_builder_mock):
        store = mock.Mock()
        data_builder = mock.Mock()
        data_builder_mock.return_value = data_builder

        schedule = WorkflowSchedule(workflow='some_workflow')

        data_builder.get_workflow.return_value = None
        self.assertFalse(schedule.is_running(store))

        workflow_data = WorkflowData('some_workflow', status=Status.RUNNING)
        data_builder.get_workflow.return_value = workflow_data
        self.assertTrue(schedule.is_running(store))

    @mock.patch('pinball.scheduler.schedule.DataBuilder')
    def test_is_failed(self, data_builder_mock):
        store = mock.Mock()
        data_builder = mock.Mock()
        data_builder_mock.return_value = data_builder

        schedule = WorkflowSchedule(workflow='some_workflow')
        data_builder.get_workflow.return_value = None
        self.assertFalse(schedule.is_failed(store))

        workflow_data = WorkflowData('some_workflow', status=Status.FAILURE)
        data_builder.get_workflow.return_value = workflow_data
        self.assertTrue(schedule.is_failed(store))

    @mock.patch('pinball.scheduler.schedule.Signaller')
    @mock.patch('pinball.scheduler.schedule.DataBuilder')
    def test_abort_running(self, data_builder_mock, signaller_mock):
        client = mock.Mock()
        store = mock.Mock()

        data_builder = mock.Mock()
        data_builder_mock.return_value = data_builder

        schedule = WorkflowSchedule(workflow='some_workflow')
        failed_instance = WorkflowInstanceData('some_workflow',
                                               '123',
                                               status=Status.FAILURE)
        running_instance = WorkflowInstanceData('some_workflow',
                                                '12345',
                                                status=Status.RUNNING)

        data_builder.get_instances.return_value = [failed_instance,
                                                   running_instance]

        signaller = mock.Mock()
        signaller_mock.return_value = signaller

        schedule.abort_running(client, store)

        signaller_mock.assert_called_once_with(client,
                                               workflow='some_workflow',
                                               instance='12345')
        signaller.set_action.assert_called_once_with(Signal.ABORT)
