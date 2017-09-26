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

"""Run tasks at predefined time intervals."""
import pickle
import time

from pinball.config.utils import PinballException
from pinball.config.utils import get_log
from pinball.config.utils import get_unique_name
from pinball.config.utils import token_to_str

from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import Query
from pinball.master.thrift_lib.ttypes import QueryAndOwnRequest
from pinball.master.thrift_lib.ttypes import TokenMasterException
from pinball.scheduler.overrun_policy import OverrunPolicy
from pinball.workflow.name import Name


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.scheduler.scheduler')


class Scheduler(object):
    # How long to own the schedule token while manipulating it.
    _LEASE_TIME_SEC = 5 * 60  # 5 minutes
    # How long to sleep before sending another own token request.
    # As we are doing gang scheduling, we can increase this value to avoid
    # hammering master process too much but without loose the schedule agility.
    _OWN_TOKEN_SLEEP_TIME_SEC = 60
    # How long to delay the schedule if it's already running and appropriate
    # policy is in place.
    _DELAY_TIME_SEC = 5 * 60  # 5 minutes
    # The size of the workflow gang to be scheduled.
    _SCHEDULE_GANG_SIZE = 60

    def __init__(self, client, store, emailer):
        self._client = client
        self._store = store
        self._emailer = emailer
        self._owned_schedule_token = None
        self._owned_schedule_token_list = []
        self._request = None
        self._name = get_unique_name()
        self._test_only_end_if_no_unowned = False

    def _own_schedule_token_list(self):
        """Attempt to own some schedule tokens.

        Only unowned tokens will be considered. Unowned schedules are ready to
        run.  The ownership of the qualifying job token lasts for a limited
        time so it has to be periodically renewed if the schedule takes longer
        than that to run.
        """
        assert not self._owned_schedule_token
        query = Query()
        query.namePrefix = Name.SCHEDULE_PREFIX
        query.maxTokens = self._SCHEDULE_GANG_SIZE
        request = QueryAndOwnRequest()
        request.query = query
        request.expirationTime = int(time.time()) + Scheduler._LEASE_TIME_SEC
        request.owner = self._name
        try:
            response = self._client.query_and_own(request)
            if response.tokens:
                assert len(response.tokens) <= self._SCHEDULE_GANG_SIZE
                self._owned_schedule_token_list = [token for token in response.tokens if token]
                LOG.info(
                    "got %d schedule token(s) from master.",
                    len(self._owned_schedule_token_list)
                )
        except TokenMasterException:
            LOG.exception('')

    def _advance_schedule(self, schedule):
        schedule.advance_next_run_time()
        self._owned_schedule_token.expirationTime = schedule.next_run_time
        self._owned_schedule_token.data = pickle.dumps(schedule)

    def _abort_workflow(self, schedule):
        return schedule.abort_running(self._client, self._store)

    def _run_or_reschedule(self):
        """Run the schedule represented by the owned schedule token.

        If the time is right and the overrun policy permits it, run the owned
        schedule token.  Otherwise, reschedule it until a later time.
        """
        assert self._owned_schedule_token
        schedule = pickle.loads(self._owned_schedule_token.data)
        if schedule.next_run_time > time.time():
            LOG.info("not the time to run token: %s", self._owned_schedule_token.name)

            # It's not time to run it yet.  Although we should claim only
            # tokens which are ready to run, clock skew between different
            # machines may result in claiming a token too soon.
            assert self._owned_schedule_token.expirationTime >= schedule.next_run_time, \
                ('%d < %d in token %s' % (self._owned_schedule_token.expirationTime,
                                          schedule.next_run_time,
                                          token_to_str(self._owned_schedule_token)))
        elif (schedule.overrun_policy == OverrunPolicy.START_NEW or
              schedule.overrun_policy == OverrunPolicy.ABORT_RUNNING or
              # Ordering of the checks in the "and" condition below is
              # important to avoid a race condition when a workflow gets
              # retried and changes the state from failed to running.
              ((schedule.overrun_policy != OverrunPolicy.DELAY_UNTIL_SUCCESS or
                not schedule.is_failed(self._store)) and
               not schedule.is_running(self._store))):
            LOG.info("run token: %s", self._owned_schedule_token.name)

            if schedule.overrun_policy == OverrunPolicy.ABORT_RUNNING:
                if not self._abort_workflow(schedule):
                    return
            self._request = schedule.run(self._emailer, self._store)
            if self._request:
                self._advance_schedule(schedule)
        elif schedule.overrun_policy == OverrunPolicy.SKIP:
            LOG.info("skip schedule due to overrun policy for token: %s",
                     self._owned_schedule_token.name)

            self._advance_schedule(schedule)
        elif (schedule.overrun_policy == OverrunPolicy.DELAY or
              schedule.overrun_policy == OverrunPolicy.DELAY_UNTIL_SUCCESS):
            LOG.info("delay schedule due to overrun policy for token: %s",
                     self._owned_schedule_token.name)

            self._owned_schedule_token.expirationTime = int(
                time.time() + Scheduler._DELAY_TIME_SEC)
        else:
            raise PinballException('unknown schedule policy %d in token %s' % (
                schedule.overrun_policy, self._owned_schedule_token))

    def _update_tokens(self):
        """Update tokens modified during schedule execution in the master.
        """
        assert self._owned_schedule_token
        if not self._request:
            self._request = ModifyRequest()
        if not self._request.updates:
            self._request.updates = []
        self._request.updates.append(self._owned_schedule_token)
        schedule = pickle.loads(self._owned_schedule_token.data)
        if schedule.workflow == 'experiments':
            LOG.info('updating tokens for workflow experiments %s',
                     self._request)
        try:
            self._client.modify(self._request)
        except TokenMasterException:
            LOG.exception('')
        finally:
            self._owned_schedule_token = None
            self._request = None

    def run(self):
        """Run the scheduler."""
        LOG.info('Running scheduler ' + self._name)
        while True:
            start_time = time.time()
            self._own_schedule_token_list()
            if self._owned_schedule_token_list:
                for s_token in self._owned_schedule_token_list:
                    self._owned_schedule_token = s_token
                    self._run_or_reschedule()
                    self._update_tokens()
                elapsed_time = time.time() - start_time
                LOG.info("processed %d schedule token(s) in %d second(s), "
                         "retry after %d seconds ...",
                         len(self._owned_schedule_token_list),
                         elapsed_time,
                         Scheduler._OWN_TOKEN_SLEEP_TIME_SEC)
                time.sleep(Scheduler._OWN_TOKEN_SLEEP_TIME_SEC)
            elif self._test_only_end_if_no_unowned:
                return
            else:
                LOG.info("can't own any schedule token, retry after %d second(s) ...",
                         Scheduler._OWN_TOKEN_SLEEP_TIME_SEC)
                time.sleep(Scheduler._OWN_TOKEN_SLEEP_TIME_SEC)
