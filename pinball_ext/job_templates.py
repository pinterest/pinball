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

"""Job template translates a user defined job into Pinball job that will be
executed by Pinball workers directly.

Condition is a special type of job.
"""

import abc
from datetime import datetime, timedelta

from pinball.workflow.job import ShellConditionJob
from pinball.workflow.job import ShellJob


__author__ = 'Pawel Garbacki, Changshu Liu, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class JobTemplateBase(object):
    """An interface for job templates."""
    __metaclass__ = abc.ABCMeta

    def __init__(self, name, write_lock=None, max_attempts=1, emails=None,
                 priority=None, warn_timeout_sec=None, abort_timeout_sec=None):
        self.name = name
        self.write_lock = write_lock
        self._max_attempts = max_attempts
        self._emails = emails if emails is not None else []
        self.priority = priority
        self._warn_timeout_sec = warn_timeout_sec
        self._abort_timeout_sec = abort_timeout_sec

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return ('name=%s' % self.name).__hash__()

    @abc.abstractmethod
    def get_pinball_job(self, inputs, outputs, params):
        """Get a Pinball job from user defined job.

        Args:
            inputs: The list of job inputs.
            outputs: The list of job outputs.
            params: The dictionary with extra parameters.
        Returns:
            The pinball job specializing the template.
        """
        return


class JobTemplate(JobTemplateBase):
    """A template that supports running user defined jobs.

    All user defined jobs are supposed to be derived from:
        pinball_ext.job.basic_jobs.JobBase

    Constructor args:
        name: Python class name of the user defined job.
        executor: name of the executor. Available executors can be found from
            pinball_ext.executor.common.Platform
        executor_config: dict config for a specific executor.
    """
    command_template = (
        'cd %(job_repo_dir)s && python -m pinball_ext.job.job_runner '
        '--job_import_dirs_config=%(job_import_dirs_config)s '
        '--job_class_name=%(job_class_name)s '
        '--job_params="%(job_params)s" '
        '%(executor_params)s'
        '%(executor_config)s'
        '%(runner_extra_params)s')

    def __init__(self, name, executor=None, executor_config=None, write_lock=None,
                 max_attempts=1, emails=None, priority=None,
                 warn_timeout_sec=None, abort_timeout_sec=None):
        assert name, 'name should be set.'

        super(JobTemplate, self).__init__(
            name=name,
            write_lock=write_lock,
            max_attempts=max_attempts,
            emails=emails,
            priority=priority,
            warn_timeout_sec=warn_timeout_sec,
            abort_timeout_sec=abort_timeout_sec)

        self._job_class_name = name
        self._executor = executor
        self._executor_config = executor_config

    def get_pinball_job(self, inputs, outputs, params):
        assert 'job_repo_dir' in params and 'job_import_dirs_config' in params

        # Config job_params
        job_params = params.get('job_params', {})
        # TODO(csliu): end_date should always be passed from params
        if 'end_date' not in params:
            yesterday = str(datetime.utcnow().date() - timedelta(days=1))
            job_params['end_date'] = yesterday
        else:
            job_params['end_date'] = params['end_date']
        assert job_params, 'job_params should not be empty!'
        job_params_text = ','.join(
            ['%s=%s' % (k, v) for k, v in job_params.iteritems()])

        # Config executor for job runner. (Only ClusterJob needs executor)
        executor_params = ''
        if self._executor:
            executor_params = '--executor=%s ' % self._executor

        # Executor config.
        executor_config = ''
        if self._executor_config:
            executor_config_text = ','.join(
                ['%s=%s' % (k, v) for k, v in self._executor_config.iteritems()])
            executor_config = '--executor_config="%s" ' % executor_config_text

        runner_extra_params = ''
        if self.write_lock:
            runner_extra_params += '--write_lock=%s ' % self.write_lock

        # Construct job runner command line.
        job_runner_command = self.command_template % {
            'job_repo_dir': params['job_repo_dir'],
            'job_import_dirs_config': params['job_import_dirs_config'],
            'job_class_name': self._job_class_name,
            'job_params': job_params_text,
            'executor_params': executor_params,
            'executor_config': executor_config,
            'runner_extra_params': runner_extra_params
        }

        return ShellJob(name=self.name, inputs=inputs, outputs=outputs,
                        command=job_runner_command,
                        emails=self._emails, max_attempts=self._max_attempts,
                        warn_timeout_sec=self._warn_timeout_sec,
                        abort_timeout_sec=self._abort_timeout_sec)


class CommandJobTemplate(JobTemplateBase):
    """The template to invoke a command-line job."""
    def __init__(self, name, command, write_lock=None,
                 max_attempts=None, emails=None, priority=None,
                 warn_timeout_sec=None, abort_timeout_sec=None):
        super(CommandJobTemplate, self).__init__(name,
                                                 write_lock,
                                                 max_attempts,
                                                 emails,
                                                 priority,
                                                 warn_timeout_sec,
                                                 abort_timeout_sec)
        self._command = command

    def get_pinball_job(self, inputs, outputs, params=None):
        params = params if params else {}
        command = self._command % params
        max_attempts = (self._max_attempts if self._max_attempts is not None
                        else 1)
        return ShellJob(name=self.name, inputs=inputs, outputs=outputs,
                        emails=self._emails, max_attempts=max_attempts,
                        warn_timeout_sec=self._warn_timeout_sec,
                        abort_timeout_sec=self._abort_timeout_sec,
                        command=command)


class ConditionTemplateBase(object):
    """An interface for condition templates."""
    __metaclass__ = abc.ABCMeta

    def __init__(self, name,
                 max_attempts=10,
                 retry_delay_sec=5 * 60,
                 emails=None,
                 priority=None):
        self.name = name
        self.max_attempts = max_attempts
        self.retry_delay_sec = retry_delay_sec
        self.emails = emails if emails is not None else []
        self.priority = priority

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return ('name=%s' % self.name).__hash__()

    @abc.abstractmethod
    def get_pinball_condition(self, outputs):
        """Create a pinball condition specification.

        Args:
            outputs: The list of condition outputs.
        Returns:
            The pinball condition specializing the template.
        """
        return


class CommandConditionTemplate(ConditionTemplateBase):
    """The template to run a custom command."""
    # 72 * 20 * 60 = seconds in a day.
    def __init__(self, name,
                 max_attempts=72,
                 retry_delay_sec=20 * 60,
                 emails=None,
                 priority=None):
        super(CommandConditionTemplate, self).__init__(
            name,
            max_attempts=max_attempts,
            retry_delay_sec=retry_delay_sec,
            emails=emails,
            priority=priority)

    def get_pinball_condition(self, outputs, params=None):
        command = params['command']
        cleanup_template = params.get('cleanup_template', None)
        return ShellConditionJob(name=self.name, outputs=outputs,
                                 emails=self.emails,
                                 max_attempts=self.max_attempts,
                                 retry_delay_sec=self.retry_delay_sec,
                                 command=command,
                                 cleanup_template=cleanup_template)
