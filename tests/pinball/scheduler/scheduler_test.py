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
import pickle
import time
import unittest

from pinball.master.factory import Factory
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import Token
from pinball.scheduler.overrun_policy import OverrunPolicy
from pinball.scheduler.schedule import WorkflowSchedule
from pinball.scheduler.scheduler import Scheduler
from pinball.workflow.emailer import Emailer
from pinball.workflow.name import Name
from tests.pinball.persistence.ephemeral_store import EphemeralStore


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


# mock.Mock cannot be pickled so we have to define our own mock class.
class MockWorkflowSchedule(WorkflowSchedule):
    def __init__(self, is_running, is_failed):
        super(MockWorkflowSchedule, self).__init__(
            next_run_time=int(time.time() - 10),
            recurrence_seconds=10,
            workflow='some_workflow')
        self._is_running = is_running
        self._is_failed = is_failed
        self.abort_called = False

    def is_running(self, store):
        return self._is_running

    def is_failed(self, store):
        return self._is_failed

    def abort_running(self, client, store):
        self.abort_called = True
        return True

    def run(self, emailer, store):
        return ModifyRequest(updates=[Token()])


class SchedulerTestCase(unittest.TestCase):
    def setUp(self):
        self._factory = Factory()
        store = EphemeralStore()
        self._factory.create_master(store)
        emailer = Emailer('some_host', '8080')
        self._scheduler = Scheduler(self._factory.get_client(), store, emailer)
        self._client = self._factory.get_client()
        self._post_schedule_token()

    @staticmethod
    def _get_schedule_token():
        name = Name(workflow='workflow_0')
        now = int(time.time())
        token = Token(name=name.get_workflow_schedule_token_name(),
                      owner='some_owner',
                      expirationTime=now - 10)
        schedule = WorkflowSchedule(next_run_time=now - 10,
                                    recurrence_seconds=10,
                                    workflow='workflow_0')
        token.data = pickle.dumps(schedule)
        return token

    def _post_schedule_token(self):
        """Add schedule token to the master."""
        request = ModifyRequest()
        request.updates = [SchedulerTestCase._get_schedule_token()]
        self._client.modify(request)

    def test_own_schedule_token(self):
        self._scheduler._own_schedule_token()
        self.assertIsNotNone(self._scheduler._owned_schedule_token)

    def test_advance_schedule(self):
        self._scheduler._own_schedule_token()
        token = self._scheduler._owned_schedule_token
        owned_schedule = pickle.loads(token.data)
        self._scheduler._advance_schedule(owned_schedule)
        now = int(time.time())
        self.assertGreater(token.expirationTime, now - 10)
        schedule = pickle.loads(token.data)
        self.assertEqual(token.expirationTime, schedule.next_run_time)

    def test_run_or_reschedule_incorrect_expiration_time(self):
        self._scheduler._own_schedule_token()
        token = self._scheduler._owned_schedule_token
        schedule = pickle.loads(token.data)
        schedule.next_run_time = int(time.time() + 1000)
        token.data = pickle.dumps(schedule)
        self.assertRaises(AssertionError, self._scheduler._run_or_reschedule)

    def _run_or_reschedule(self, overrun_policy, is_running=True,
                           is_failed=True, is_abort_called=False):
        self._scheduler._own_schedule_token()
        token = self._scheduler._owned_schedule_token
        schedule = MockWorkflowSchedule(is_running, is_failed)
        schedule.overrun_policy = overrun_policy
        token.data = pickle.dumps(schedule)
        token.expirationTime = schedule.next_run_time
        old_expiration_time = token.expirationTime

        self._scheduler._run_or_reschedule()

        token = self._scheduler._owned_schedule_token
        new_expiration_time = token.expirationTime
        self.assertGreater(new_expiration_time, old_expiration_time)
        schedule = pickle.loads(token.data)
        self.assertEqual(is_abort_called, schedule.abort_called)

    def test_run_START_NEW(self):
        self._run_or_reschedule(OverrunPolicy.START_NEW)
        self.assertIsNotNone(self._scheduler._request)

    def test_reschedule_SKIP(self):
        self._run_or_reschedule(OverrunPolicy.SKIP)
        self.assertIsNone(self._scheduler._request)

    def test_run_ABORT_RUNNING(self):
        self._run_or_reschedule(OverrunPolicy.ABORT_RUNNING,
                                is_abort_called=True)
        self.assertIsNotNone(self._scheduler._request)

    def test_reschedule_DELAY(self):
        self._run_or_reschedule(OverrunPolicy.DELAY)
        token = self._scheduler._owned_schedule_token
        schedule = pickle.loads(token.data)
        self.assertLess(schedule.next_run_time, token.expirationTime)
        self.assertIsNone(self._scheduler._request)

    def test_run_DELAY(self):
        self._run_or_reschedule(OverrunPolicy.DELAY, is_running=False)
        self.assertIsNotNone(self._scheduler._request)

    def test_reschedule_DELAY_UNTIL_SUCCESS(self):
        self._run_or_reschedule(OverrunPolicy.DELAY_UNTIL_SUCCESS,
                                is_running=False, is_failed=True)
        token = self._scheduler._owned_schedule_token
        schedule = pickle.loads(token.data)
        self.assertLess(schedule.next_run_time, token.expirationTime)
        self.assertIsNone(self._scheduler._request)

    def test_run_DELAY_UNTIL_SUCCESS(self):
        self._run_or_reschedule(OverrunPolicy.DELAY_UNTIL_SUCCESS,
                                is_running=False, is_failed=False)
        self.assertIsNotNone(self._scheduler._request)
