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

"""Definition of schedule metadata included in schedule tokens.

A Schedule defines when something should run.  (The thing is abstract; a
WorkflowSchedule runs workflows.)  It has a starting run time, and is
(optionally) repeated periodically.  A schedule also has an OverrunPolicy that
defines how it should behave if a previous run didn't finish by the time the
thing is run again through this schedule.
"""
import abc
import datetime
import math
import time

from pinball.config.pinball_config import PinballConfig
from pinball.config.utils import get_log
from pinball.config.utils import timestamp_to_str
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.parser.config_parser import ParserCaller
from pinball.parser.utils import load_parser_with_caller
from pinball.persistence.token_data import TokenData
from pinball.scheduler.overrun_policy import OverrunPolicy
from pinball.ui.data import Status
from pinball.ui.data_builder import DataBuilder
from pinball.workflow.name import Name
from pinball.workflow.signaller import Signal
from pinball.workflow.signaller import Signaller
from pinball.workflow.utils import load_path


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = ['Pawel Garbacki', 'Mao Ye']
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.scheduler.schedule')


class Schedule(TokenData):
    """Parent class for specialized schedule types."""
    __metaclass__ = abc.ABCMeta

    def __init__(self, next_run_time=None, recurrence_seconds=None,
                 overrun_policy=OverrunPolicy.SKIP):
        self.next_run_time = next_run_time
        self.recurrence_seconds = recurrence_seconds
        self.overrun_policy = overrun_policy

    def advance_next_run_time(self):
        """Advance the scheduled run time beyond the current time."""
        now = time.time()
        if self.next_run_time <= now:
            # Set next run time to the lowest timestamp based off
            # recurrence that is greater than the current time.
            delta_runs = math.ceil((now - self.next_run_time) /
                                   self.recurrence_seconds)
            LOG.info('advancing the next run time now %f '
                     'next_run_time %d recurrence_seconds %d delta_runs %f',
                     now, self.next_run_time,
                     self.recurrence_seconds, delta_runs)
            self.next_run_time += int(delta_runs * self.recurrence_seconds)
            if now == self.next_run_time:
                self.next_run_time += self.recurrence_seconds
        assert self.next_run_time > now

    def corresponds_to(self, schedule):
        """Assess correspondence to another schedule.

        Schedules correspond to each other if their next run times are shifted
        by a multiplication of recurrence seconds, and all other fields are
        the same.

        Args:
            schedule: The schedule to compare with.
        Returns:
            True iff the schedules correspond to each other.
        """
        if (self.overrun_policy != schedule.overrun_policy or
                self.recurrence_seconds != schedule.recurrence_seconds):
            return False
        delta = self.next_run_time - schedule.next_run_time
        delta_multiplicator = 1. * delta / self.recurrence_seconds
        return delta_multiplicator == int(delta_multiplicator)

    @abc.abstractmethod
    def run(self, emailer, store):
        """Run the routine pointed to by this schedule."""
        return None

    @abc.abstractmethod
    def is_running(self, store):
        """Checks if the previous run is still active.

        Args:
            store: The store to query for status.
        Returns:
            True iff the run is running.
        """
        return False

    @abc.abstractmethod
    def is_failed(self, store):
        """Checks if the most recent run has failed.

        Args:
            store: The store to query for status.
        Returns:
            True iff the run has failed.
        """
        return False

    @abc.abstractmethod
    def abort_running(self, client, store):
        """Abort all active runs.

        Args:
            client: The client to communicate with the master.
            store: The store to retrieve runs status.
        Returns:
            True iff the workflow has been aborted.
        """
        return False


class WorkflowSchedule(Schedule):
    """Schedule for a workflow."""
    def __init__(
            self,
            next_run_time=None,
            recurrence_seconds=None,
            overrun_policy=OverrunPolicy.SKIP,
            parser_params=PinballConfig.PARSER_PARAMS,
            workflow=None,
            emails=None,
            max_running_instances=None):
        Schedule.__init__(self, next_run_time, recurrence_seconds,
                          overrun_policy)
        self.parser_params = parser_params
        self.workflow = workflow
        self.emails = emails if emails is not None else []
        self.max_running_instances = max_running_instances if max_running_instances \
            else PinballConfig.DEFAULT_MAX_WORKFLOW_RUNNING_INSTANCES

    def __str__(self):
        if self.next_run_time:
            next_run_time = timestamp_to_str(self.next_run_time)
        else:
            next_run_time = str(self.next_run_time)
        if self.recurrence_seconds:
            delta = datetime.timedelta(seconds=self.recurrence_seconds)
            recurrence = str(delta)
        else:
            recurrence = str(self.recurrence_seconds)
        if self.overrun_policy is not None:
            overrun_policy = OverrunPolicy.to_string(self.overrun_policy)
        else:
            overrun_policy = str(self.overrun_policy)
        return ('WorkflowSchedule(next_run_time=%s, recurrence=%s, '
                'overrun_policy=%s, parser_params=%s, workflow=%s, '
                'email=%s, max_running_instances=%s)' % (next_run_time,
                                                         recurrence,
                                                         overrun_policy,
                                                         self.parser_params,
                                                         self.workflow,
                                                         self.emails,
                                                         str(self.max_running_instances)))

    def __repr__(self):
        return self.__str__()

    def advance_next_run_time(self):
        # TODO(pawel): remove after debugging.
        LOG.info('advancing the next run time for workflow %s', self.workflow)
        super(WorkflowSchedule, self).advance_next_run_time()

    def corresponds_to(self, schedule):
        if (self.parser_params != schedule.parser_params or
                self.workflow != schedule.workflow or
                self.emails != schedule.emails or
                self.max_running_instances != schedule.max_running_instances):
            return False
        return super(WorkflowSchedule, self).corresponds_to(schedule)

    def run(self, emailer, store):
        if not self._check_workflow_instances(emailer, self.workflow, store):
            LOG.warn('too many instances running for workflow %s', self.workflow)
            return None

        config_parser = load_parser_with_caller(
            PinballConfig.PARSER,
            self.parser_params,
            ParserCaller.SCHEDULE
        )
        workflow_tokens = config_parser.get_workflow_tokens(self.workflow)
        if not workflow_tokens:
            LOG.error('workflow %s not found', self.workflow)
            return None
        result = ModifyRequest()
        result.updates = workflow_tokens
        assert result.updates
        token = result.updates[0]
        name = Name.from_job_token_name(token.name)
        if not name.instance:
            name = Name.from_event_token_name(token.name)
        LOG.info('exporting workflow %s instance %s.  Its tokens are under %s',
                 name.workflow, name.instance, name.get_instance_prefix())
        return result

    def is_running(self, store):
        data_builder = DataBuilder(store, use_cache=True)
        workflow_data = data_builder.get_workflow(self.workflow)
        if not workflow_data:
            return False
        return workflow_data.status == Status.RUNNING

    def is_failed(self, store):
        data_builder = DataBuilder(store, use_cache=True)
        workflow_data = data_builder.get_workflow(self.workflow)
        if not workflow_data:
            return False
        return (workflow_data.status != Status.RUNNING and
                workflow_data.status != Status.SUCCESS)

    def _get_running_instances(self, store):
        """Find running instances of the workflow.

        Args:
            store: The store to query for wokflow instance status.
        Returns:
            List of running workflow instance names.
        """
        data_builder = DataBuilder(store, use_cache=True)
        instances = data_builder.get_instances(self.workflow)
        result = []
        for instance in instances:
            if instance.status == Status.RUNNING:
                result.append(instance.instance)
        return result

    def abort_running(self, client, store):
        running_instances = self._get_running_instances(store)
        for instance in running_instances:
            signaller = Signaller(client,
                                  workflow=self.workflow,
                                  instance=instance)
            signaller.set_action(Signal.ABORT)
            if not signaller.is_action_set(Signal.ABORT):
                return False
        return True

    def _check_workflow_instances(self, emailer, workflow_name, store):
        """Check the number of running instances of the workflow.

        Besides of the return, also send out warning email if too many
        instances running for the given workflow.

        Args:
            emailer: The email sender.
            workflow_name: Name of the workflow.
            store: The store to retrieve runs status.

        Returns:
            False if running instance number exceeds the max_running_instances setting;
            Otherwise, True.
        """
        running_instances = self._get_running_instances(store)
        if self.max_running_instances and len(running_instances) >= self.max_running_instances:
            LOG.warn('Too many (%s) instances running for workflow %s !',
                     len(running_instances), workflow_name)
            if emailer:
                emailer.send_too_many_running_instances_warning_message(self.emails,
                                                                        workflow_name,
                                                                        len(running_instances),
                                                                        self.max_running_instances)
            else:
                LOG.warn('Emailer is not set! Failed to send too many instances running warning '
                         'email for workflow %s', workflow_name)
            return False
        return True
