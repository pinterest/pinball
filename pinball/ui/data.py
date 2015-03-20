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

"""Data structures defining data formats send over to Web clients."""
# TODO(pawel): move this file under workflow/
import copy
import time

from pinball.config.utils import PinballException
from pinball.config.utils import timestamp_to_str
from pinball.config.utils import token_data_to_str
from pinball.scheduler.overrun_policy import OverrunPolicy


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class Status(object):
    UNKNOWN, NEVER_RUN, RUNNING, FAILURE, SUCCESS, DISABLED, ABORTED, CONDITION_PENDING = range(8)

    _STATUS_NAMES = {UNKNOWN: 'UNKNOWN',
                     NEVER_RUN: 'NEVER_RUN',
                     RUNNING: 'RUNNING',
                     SUCCESS: 'SUCCESS',
                     FAILURE: 'FAILURE',
                     DISABLED: 'DISABLED',
                     ABORTED: 'ABORTED',
                     CONDITION_PENDING: 'CONDITION_PENDING'}

    COLORS = {UNKNOWN: '#000000',
              NEVER_RUN: '#dddddd',
              RUNNING: '#66ccff',
              SUCCESS: '#89e894',
              FAILURE: '#ff3333',
              DISABLED: '#ffffff',
              ABORTED: '#ff3333',
              CONDITION_PENDING: '#ffcc00'}

    @staticmethod
    def to_string(status):
        return Status._STATUS_NAMES[status]

    @staticmethod
    def from_string(status_name):
        for status, name in Status._STATUS_NAMES.items():
            if name == status_name:
                return status
        raise PinballException('Unknown status %s' % status_name)


def _get_run_time(start_time, end_time):
    if not start_time:
        return None
    if not end_time:
        end_time = time.time()
    return int(end_time - start_time)


class WorkflowData(object):
    def __init__(self, workflow, status=None, last_start_time=None,
                 last_end_time=None, last_instance=None,
                 running_instance_number=None):
        self.workflow = workflow
        self.status = status
        self.last_start_time = last_start_time
        self.last_end_time = last_end_time
        self.last_instance = last_instance
        self.running_instance_number = running_instance_number

    def format(self):
        result = copy.copy(self.__dict__)
        result['status'] = Status.to_string(self.status)
        if self.last_start_time:
            result['last_start_time'] = timestamp_to_str(self.last_start_time)
        if self.last_end_time:
            result['last_end_time'] = timestamp_to_str(self.last_end_time)
        # TODO(pawel): run_time can be inferred from start and end time.  To
        # make it easier to parse in the UI, we should send start and end time
        # in seconds.
        result['run_time'] = _get_run_time(self.last_start_time,
                                           self.last_end_time)
        result['running_instance_number'] = str(self.running_instance_number)
        return result


class WorkflowInstanceData(object):
    def __init__(self, workflow, instance, status=None, start_time=None,
                 end_time=None):
        self.workflow = workflow
        self.instance = instance
        self.status = status
        self.start_time = start_time
        self.end_time = end_time

    def format(self):
        result = copy.copy(self.__dict__)
        result['status'] = Status.to_string(self.status)
        if self.start_time:
            result['start_time'] = timestamp_to_str(self.start_time)
        if self.end_time:
            result['end_time'] = timestamp_to_str(self.end_time)
        result['run_time'] = _get_run_time(self.start_time, self.end_time)
        return result


class JobData(object):
    def __init__(self, workflow, instance, job, job_type, is_condition, info,
                 inputs, outputs, emails=None, max_attempts=1,
                 retry_delay_sec=0, warn_timeout_sec=None,
                 abort_timeout_sec=None, priority=None, status=None,
                 last_start_time=None, last_end_time=None, progress=None):
        self.workflow = workflow
        self.instance = instance
        self.job = job
        self.job_type = job_type
        self.is_condition = is_condition
        self.info = info
        self.inputs = inputs
        self.outputs = outputs
        self.emails = emails if emails is not None else []
        self.max_attempts = max_attempts
        assert self.max_attempts > 0
        self.retry_delay_sec = retry_delay_sec
        self.warn_timeout_sec = warn_timeout_sec
        self.abort_timeout_sec = abort_timeout_sec
        self.priority = priority
        self.status = status
        self.last_start_time = last_start_time
        self.last_end_time = last_end_time
        self.progress = progress if progress else []

    def format(self):
        result = copy.copy(self.__dict__)
        result['status'] = Status.to_string(self.status)
        if self.last_start_time:
            result['last_start_time'] = timestamp_to_str(self.last_start_time)
        if self.last_end_time:
            result['last_end_time'] = timestamp_to_str(self.last_end_time)
        result['run_time'] = _get_run_time(self.last_start_time,
                                           self.last_end_time)
        return result


class JobExecutionData(object):
    def __init__(self, workflow, instance, job, execution, info,
                 exit_code=None, cleanup_exit_code=None, start_time=None,
                 end_time=None, properties=None, logs=None):
        self.workflow = workflow
        self.instance = instance
        self.job = job
        self.execution = execution
        self.info = info
        self.exit_code = exit_code
        self.cleanup_exit_code = cleanup_exit_code
        self.start_time = start_time
        self.end_time = end_time
        self.properties = properties if properties is not None else {}
        self.logs = logs if logs is not None else []

    def format(self):
        result = copy.copy(self.__dict__)
        if self.start_time:
            result['start_time'] = timestamp_to_str(self.start_time)
        if self.end_time:
            result['end_time'] = timestamp_to_str(self.end_time)
        result['run_time'] = _get_run_time(self.start_time, self.end_time)
        return result


class WorkflowScheduleData(object):
    def __init__(self, next_run_time, recurrence_seconds, overrun_policy,
                 overrun_policy_help, workflow, parser_params, emails,
                 max_running_instances):
        self.next_run_time = next_run_time
        self.recurrence_seconds = recurrence_seconds
        self.overrun_policy = overrun_policy
        self.overrun_policy_help = overrun_policy_help
        self.workflow = workflow
        self.parser_params = parser_params
        self.emails = emails
        self.max_running_instances = max_running_instances

    def format(self):
        result = copy.copy(self.__dict__)
        result['next_run_time'] = timestamp_to_str(self.next_run_time)
        result['overrun_policy'] = OverrunPolicy.to_string(self.overrun_policy)
        result['max_running_instances'] = str(self.max_running_instances)
        result['parser_params'] = str(self.parser_params)
        return result


class TokenPathData(object):
    def __init__(self, path, count):
        self.path = path
        self.count = count

    def format(self):
        return copy.copy(self.__dict__)


class TokenData(object):
    def __init__(self, version, name, owner, expiration_time, priority, data):
        self.version = version
        self.name = name
        self.owner = owner
        self.expiration_time = expiration_time
        self.priority = priority
        self.data = data

    def format(self):
        result = copy.copy(self.__dict__)
        if not self.owner:
            result['owner'] = ''
        result['expiration_time'] = timestamp_to_str(self.expiration_time)
        result['data'] = token_data_to_str(self.data)
        return result
