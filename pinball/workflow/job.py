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

"""Definition of job metadata included in job tokens.

Job object describes job inputs, outputs, and all information required to
execute a job (e.g., a command line of a shell job or class name of a data
job)."""
import abc

from pinball.config.utils import get_log
from pinball.persistence.token_data import TokenData
from pinball.workflow.name import Name


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.workflow.worker')


class Job(TokenData):
    """Parent class for specialized job types."""
    __metaclass__ = abc.ABCMeta

    IS_CONDITION = False

    def __init__(self, name=None, inputs=None, outputs=None, emails=None,
                 max_attempts=1, retry_delay_sec=0, warn_timeout_sec=None,
                 abort_timeout_sec=None):
        self.name = name
        self.inputs = inputs if inputs is not None else []
        self.outputs = outputs if outputs is not None else []
        self.emails = emails if emails is not None else []
        self.max_attempts = max_attempts
        self.retry_delay_sec = retry_delay_sec
        self.warn_timeout_sec = warn_timeout_sec
        self.abort_timeout_sec = abort_timeout_sec
        assert self.max_attempts > 0
        self.disabled = False
        self.history = []
        self.events = []

    @property
    def _COMPATIBILITY_ATTRIBUTES(self):
        return {
            'emails': [],
            'disabled': False,
            'max_attempts': 1,
            'events': [],
            'warn_timeout_sec': None,
            'abort_timeout_sec': None,
            'retry_delay_sec': 0,
        }

    @abc.abstractmethod
    def info(self):
        return

    def retry(self):
        """Decide if the job should be retried.

        Returns:
            True if the job should be retried, otherwise False.
        """
        if not self.history:
            return False
        last_record = self.history[-1]
        current_instance = last_record.instance
        assert last_record.exit_code != 0
        failed_runs = 0
        for record in reversed(self.history):
            if record.instance != current_instance:
                break
            if record.exit_code != 0:
                # There may have been successful runs in the past if we are
                # re-doing an execution.
                failed_runs += 1
            if failed_runs >= self.max_attempts:
                return False
        return True

    def truncate_history(self):
        if self.IS_CONDITION and len(self.history) > self.max_attempts:
            self.history = self.history[-self.max_attempts:]

    def reload(self, new_job):
        """Reload job config from a new config.

        Configuration elements defining the workflow topology (inputs and
        outputs), execution history, or run-time values (events) are not
        modified.

        Args:
            new_job: The new job configuration to update from.
        """
        assert self.__class__ == new_job.__class__
        self.emails = new_job.emails
        self.max_attempts = new_job.max_attempts


class ShellJob(Job):
    """Shell job runs a command when executed."""
    def __init__(self, name=None, inputs=None, outputs=None, emails=None,
                 max_attempts=1, retry_delay_sec=0, warn_timeout_sec=None,
                 abort_timeout_sec=None, command=None, cleanup_template=None):
        super(ShellJob, self).__init__(name, inputs, outputs, emails,
                                       max_attempts, retry_delay_sec,
                                       warn_timeout_sec, abort_timeout_sec)
        self.command = command
        self.cleanup_template = cleanup_template

    @property
    def _COMPATIBILITY_ATTRIBUTES(self):
        result = super(ShellJob, self)._COMPATIBILITY_ATTRIBUTES
        result['cleanup_template'] = None
        return result

    def __str__(self):
        return ('ShellJob(name=%s, inputs=%s, outputs=%s, emails=%s, '
                'max_attempts=%d, retry_delay_sec=%d, warn_timeout_sec=%s, '
                'abort_timeout_sec=%s, disabled=%s, command=%s, '
                'cleanup_template=%s, events=%s, history=%s)' % (
                    self.name,
                    self.inputs,
                    self.outputs,
                    self.emails,
                    self.max_attempts,
                    self.retry_delay_sec,
                    self.warn_timeout_sec,
                    self.abort_timeout_sec,
                    self.disabled,
                    self.command,
                    self.cleanup_template,
                    self.events,
                    self.history))

    def __repr__(self):
        return self.__str__()

    def info(self):
        return 'command=%s' % self.command

    def reload(self, new_job):
        super(ShellJob, self).reload(new_job)
        self.command = new_job.command
        self.cleanup_template = new_job.cleanup_template

    @staticmethod
    def _get_command_attributes(template):
        """Extract attributes from a command string template.

        E.g., for template 'ls %(dir1)s %(dir2)s' the result is
        ['dir1', 'dir2'].

        Args:
            template: The template to extract attributes from.
        Returns:
            The list of named attributes extracted from the template.
        """
        class Extractor:
            """Helper class extracting attributes from a string template.
            """
            def __init__(self):
                self.attributes = set()

            def __getitem__(self, attribute):
                self.attributes.add(attribute)
                return 0

        extractor = Extractor()
        try:
            template % extractor
        except ValueError:
            LOG.exception('failed to customize template %s', template)
        return list(extractor.attributes)

    def _consolidate_event_attributes(self):
        """Consolidate attributes in triggering events.

        Iterate over events in the most recent execution record and combine
        them into one dictionary mapping attribute names to their values.  If
        multiple events contain the same attribute, the return value will be a
        comma separated string of values from all those events.

        Returns:
            Dictionary of consolidated event attribute key-values.
        """
        assert self.history
        last_execution_record = self.history[-1]
        result = {}
        for event in last_execution_record.events:
            for key, value in event.attributes.items():
                new_value = result.get(key)
                if new_value:
                    new_value += ',%s' % value
                else:
                    new_value = value
                result[key] = new_value
        return result

    def customize_command(self):
        """Specialize the command with attribute values extracted from events.

        Returns:
            Job command with parameter values replaced by attributes extracted
            from the triggering events.  If a parameter is not present in the
            event attribute set, it is replaced with an empty string.
        """
        attributes = {}
        command_attributes = ShellJob._get_command_attributes(self.command)
        for attribute in command_attributes:
            attributes[attribute] = ''
        event_attributes = self._consolidate_event_attributes()
        attributes.update(event_attributes)
        try:
            return self.command % attributes
        except ValueError:
            LOG.exception('failed to customize command %s', self.command)
            return self.command


class ShellConditionJob(ShellJob):
    IS_CONDITION = True

    def __init__(self, name=None, outputs=None, emails=None, max_attempts=10,
                 retry_delay_sec=5 * 60, warn_timeout_sec=None,
                 abort_timeout_sec=None, command=None, cleanup_template=None):
        super(ShellConditionJob, self).__init__(
            name=name,
            inputs=[Name.WORKFLOW_START_INPUT],
            outputs=outputs,
            emails=emails,
            max_attempts=max_attempts,
            retry_delay_sec=retry_delay_sec,
            warn_timeout_sec=warn_timeout_sec,
            abort_timeout_sec=abort_timeout_sec,
            command=command,
            cleanup_template=cleanup_template)
