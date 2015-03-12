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

"""Utilities to construct and manipulate workflow token names.

Token names are hierarchical.  We use '/' as the level separator.

Job token is named
/workflow/<workflow_name>/<workflow_instance>/job/[waiting|runnable]/<job>

Event token is named
/workflow/<workflow_name>/<workflow_instance>/input/<job>/<input>/<event>
In a basic job dependency model, <input> is the name of an upstream job that
<job> depends on.  A special input indicating workflow start is defined for
jobs with no dependencies.

A signal token is named as follows (depending on the level of applicability):
 - top level: /workflow/__SIGNAL__/<action>
 - workflow level: /workflow/<workflow_name>/__SIGNAL__/<action>
 - workflow instance level:
      /workflow/<workflow_name>/<workflow_instance>/__SIGNAL__/<action>
"""
import re


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class Name(object):
    def __init__(self, workflow=None, instance=None, job_state=None, job=None,
                 input_name=None, event=None, signal=None):
        """Create a name. """
        self.workflow = workflow
        self.instance = instance
        self.job_state = job_state
        self.job = job
        self.input = input_name
        self.event = event
        self.signal = signal

    DELIMITER = '/'

    WORKFLOW_PREFIX = '/workflow/'
    RUNNABLE_STATE = 'runnable'
    WAITING_STATE = 'waiting'
    # Special input defined for jobs with no dependencies.
    # TODO(pawel): we should think about this a bit more.  In particular, it
    # should be possible to have jobs with no dependencies which would re-run
    # as soon as they finish.
    # TODO(pawel): rather than having a special input name for top-level jobs,
    # we could simply make them runnable when instantiating the workflow.
    WORKFLOW_START_INPUT = '__WORKFLOW_START__'

    SCHEDULE_PREFIX = '/schedule/'
    WORKFLOW_SCHEDULE_PREFIX = '/schedule/workflow/'

    @staticmethod
    def from_workflow_prefix(prefix):
        result = Name()
        # TODO(pawel): for efficiency, regexes should be pre-compiled.
        TOKENS_REGEX = r'^/workflow/(?P<workflow>\w+)'
        m = re.match(TOKENS_REGEX, prefix)
        if m:
            result.workflow = m.group('workflow')
        return result

    @staticmethod
    def from_instance_prefix(prefix):
        result = Name()
        TOKENS_REGEX = r'^/workflow/(?P<workflow>\w+)/(?P<instance>\w+)'
        m = re.match(TOKENS_REGEX, prefix)
        if m:
            result.workflow = m.group('workflow')
            result.instance = m.group('instance')
        return result

    @staticmethod
    def from_job_prefix(prefix):
        result = Name()
        TOKENS_REGEX = (r'^/workflow/(?P<workflow>\w+)/(?P<instance>\w+)/job')
        m = re.match(TOKENS_REGEX, prefix)
        if m:
            result.workflow = m.group('workflow')
            result.instance = m.group('instance')
        return result

    @staticmethod
    def from_job_state_prefix(prefix):
        result = Name()
        TOKENS_REGEX = (r'^/workflow/(?P<workflow>\w+)/(?P<instance>\w+)/job/'
                        r'(?P<job_state>waiting|runnable)')
        m = re.match(TOKENS_REGEX, prefix)
        if m:
            result.workflow = m.group('workflow')
            result.instance = m.group('instance')
            result.job_state = m.group('job_state')
        return result

    @staticmethod
    def from_job_token_name(name):
        result = Name()
        TOKENS_REGEX = (r'^/workflow/(?P<workflow>\w+)/(?P<instance>\w+)/job/'
                        r'(?P<job_state>waiting|runnable)/(?P<job>\w+)$')
        m = re.match(TOKENS_REGEX, name)
        if m:
            result.workflow = m.group('workflow')
            result.instance = m.group('instance')
            result.job_state = m.group('job_state')
            result.job = m.group('job')
        return result

    @staticmethod
    def from_input_prefix(prefix):
        result = Name()
        TOKENS_REGEX = (r'^/workflow/(?P<workflow>\w+)/'
                        r'(?P<instance>\w+)/input/'
                        r'(?P<job>\w+)/(?P<input>\w+)')
        m = re.match(TOKENS_REGEX, prefix)
        if m:
            result.workflow = m.group('workflow')
            result.instance = m.group('instance')
            result.job = m.group('job')
            result.input = m.group('input')
        return result

    @staticmethod
    def from_event_token_name(name):
        result = Name()
        TOKENS_REGEX = (r'^/workflow/(?P<workflow>\w+)/'
                        r'(?P<instance>\w+)/input/'
                        r'(?P<job>\w+)/(?P<input>\w+)/(?P<event>\w+)$')
        m = re.match(TOKENS_REGEX, name)
        if m:
            result.workflow = m.group('workflow')
            result.instance = m.group('instance')
            result.job = m.group('job')
            result.input = m.group('input')
            result.event = m.group('event')
        return result

    @staticmethod
    def from_workflow_schedule_token_name(name):
        result = Name()
        TOKENS_REGEX = (r'^/schedule/workflow/(?P<workflow>\w+)$')
        m = re.match(TOKENS_REGEX, name)
        if m:
            result.workflow = m.group('workflow')
        return result

    @staticmethod
    def from_signal_token_name(name):
        result = Name()

        TOP_SIGNAL_REGEX = r'^/workflow/__SIGNAL__/(?P<signal>\w+)$'
        m = re.match(TOP_SIGNAL_REGEX, name)
        if m:
            result.signal = m.group('signal')
            return result

        WORKFLOW_SIGNAL_REGEX = (r'^/workflow/(?P<workflow>\w+)/__SIGNAL__/'
                                 r'(?P<signal>\w+)$')
        m = re.match(WORKFLOW_SIGNAL_REGEX, name)
        if m:
            result.workflow = m.group('workflow')
            result.signal = m.group('signal')
            return result

        INSTANCE_SIGNAL_REGEX = (r'^/workflow/(?P<workflow>\w+)/'
                                 r'(?P<instance>\w+)/__SIGNAL__/'
                                 r'(?P<signal>\w+)$')
        m = re.match(INSTANCE_SIGNAL_REGEX, name)
        if m:
            result.workflow = m.group('workflow')
            result.instance = m.group('instance')
            result.signal = m.group('signal')

        return result

    def get_workflow_prefix(self):
        if self.workflow:
            return '/workflow/%(workflow)s/' % {'workflow': self.workflow}
        return ''

    def get_instance_prefix(self):
        if self.workflow and self.instance:
            return ('/workflow/%(workflow)s/%(instance)s/' %
                    {'workflow': self.workflow, 'instance': self.instance})
        return ''

    def get_job_prefix(self):
        if self.workflow and self.instance:
            return ('/workflow/%(workflow)s/%(instance)s/job/' %
                    {'workflow': self.workflow, 'instance': self.instance})
        return ''

    def get_job_state_prefix(self):
        if self.workflow and self.instance and self.job_state:
            return ('/workflow/%(workflow)s/%(instance)s/job/%(job_state)s/' %
                    {'workflow': self.workflow,
                     'instance': self.instance,
                     'job_state': self.job_state})
        return ''

    def get_job_token_name(self):
        if (self.workflow and self.instance and self.job_state and
                self.job):
            return ('/workflow/%(workflow)s/%(instance)s/job/%(job_state)s/'
                    '%(job)s' % {'workflow': self.workflow,
                                 'instance': self.instance,
                                 'job_state': self.job_state,
                                 'job': self.job})
        return ''

    def get_input_prefix(self):
        if self.workflow and self.instance and self.job:
            return ('/workflow/%(workflow)s/%(instance)s/input/%(job)s/'
                    '%(input)s/' % {'workflow': self.workflow,
                                    'instance': self.instance,
                                    'job': self.job,
                                    'input': self.input})
        return ''

    def get_event_token_name(self):
        if (self.workflow and self.instance and self.job and
                self.input and self.event):
            return ('/workflow/%(workflow)s/%(instance)s/input/%(job)s/'
                    '%(input)s/%(event)s' % {'workflow': self.workflow,
                                             'instance': self.instance,
                                             'job': self.job,
                                             'input': self.input,
                                             'event': self.event})
        return ''

    def get_workflow_schedule_token_name(self):
        if self.workflow:
            return '/schedule/workflow/%(workflow)s' % {'workflow':
                                                        self.workflow}

    def get_signal_prefix(self):
        if not self.workflow:
            return '/workflow/__SIGNAL__/'
        if not self.instance:
            return '/workflow/%(workflow)s/__SIGNAL__/' % {
                'workflow': self.workflow}
        return ('/workflow/%(workflow)s/%(instance)s/__SIGNAL__/' % {
                'workflow': self.workflow,
                'instance': self.instance})

    def get_signal_token_name(self):
        if self.signal:
            if not self.workflow:
                return '/workflow/__SIGNAL__/%(signal)s' % {
                    'signal': self.signal}
            if not self.instance:
                return '/workflow/%(workflow)s/__SIGNAL__/%(signal)s' % {
                    'workflow': self.workflow,
                    'signal': self.signal}
            return ('/workflow/%(workflow)s/%(instance)s/__SIGNAL__/'
                    '%(signal)s' % {'workflow': self.workflow,
                                    'instance': self.instance,
                                    'signal': self.signal})
        return ''
