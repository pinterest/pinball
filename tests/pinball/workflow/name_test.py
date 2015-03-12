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

"""Validation tests for operations on token names."""
import unittest

from pinball.workflow.name import Name


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class NameTestCase(unittest.TestCase):
    def test_workflow_prefix(self):
        PREFIX = '/workflow/some_workflow/'
        name = Name.from_workflow_prefix(PREFIX)
        self.assertEqual('some_workflow', name.workflow)
        self.assertEqual(PREFIX, name.get_workflow_prefix())

    def test_instance_prefix(self):
        PREFIX = '/workflow/some_workflow/some_instance/'
        name = Name.from_instance_prefix(PREFIX)
        self.assertEqual('some_workflow', name.workflow)
        self.assertEqual('some_instance', name.instance)
        self.assertEqual(PREFIX, name.get_instance_prefix())

    def test_job_state_prefix(self):
        PREFIX = '/workflow/some_workflow/some_instance/job/waiting/'
        name = Name.from_job_state_prefix(PREFIX)
        self.assertEqual('some_workflow', name.workflow)
        self.assertEqual('some_instance', name.instance)
        self.assertEqual('waiting', name.job_state)
        self.assertEqual(PREFIX, name.get_job_state_prefix())

    def test_job_prefix(self):
        PREFIX = '/workflow/some_workflow/some_instance/job/'
        name = Name.from_job_prefix(PREFIX)
        self.assertEqual('some_workflow', name.workflow)
        self.assertEqual('some_instance', name.instance)
        self.assertEqual(PREFIX, name.get_job_prefix())

    def test_job_token_name(self):
        NAME = '/workflow/some_workflow/some_instance/job/waiting/some_job'
        name = Name.from_job_token_name(NAME)
        self.assertEqual('some_workflow', name.workflow)
        self.assertEqual('some_instance', name.instance)
        self.assertEqual('waiting', name.job_state)
        self.assertEqual('some_job', name.job)
        self.assertEqual(NAME, name.get_job_token_name())

    def test_input_prefix(self):
        PREFIX = ('/workflow/some_workflow/some_instance/input/some_job/'
                  'some_input/')
        name = Name.from_input_prefix(PREFIX)
        self.assertEqual('some_workflow', name.workflow)
        self.assertEqual('some_instance', name.instance)
        self.assertEqual('some_job', name.job)
        self.assertEqual('some_input', name.input)
        self.assertEqual(PREFIX, name.get_input_prefix())

    def test_event_token_name(self):
        NAME = ('/workflow/some_workflow/some_instance/input/some_job/'
                'some_input/some_event')
        name = Name.from_event_token_name(NAME)
        self.assertEqual('some_workflow', name.workflow)
        self.assertEqual('some_instance', name.instance)
        self.assertEqual('some_job', name.job)
        self.assertEqual('some_input', name.input)
        self.assertEqual('some_event', name.event)
        self.assertEqual(NAME, name.get_event_token_name())

    def test_workflow_schedule_token_name(self):
        NAME = '/schedule/workflow/some_workflow'
        name = Name.from_workflow_schedule_token_name(NAME)
        self.assertEqual('some_workflow', name.workflow)
        self.assertEqual(NAME, name.get_workflow_schedule_token_name())

    def test_signal_prefix(self):
        TOP_SIGNAL_PREFIX = '/workflow/__SIGNAL__/'
        name = Name.from_signal_token_name(TOP_SIGNAL_PREFIX)
        self.assertEqual(TOP_SIGNAL_PREFIX, name.get_signal_prefix())

        WORKFLOW_SIGNAL_PREFIX = '/workflow/some_workflow/__SIGNAL__/'
        name.workflow = 'some_workflow'
        self.assertEqual(WORKFLOW_SIGNAL_PREFIX, name.get_signal_prefix())

        INSTANCE_SIGNAL_PREFIX = ('/workflow/some_workflow/some_instance/'
                                  '__SIGNAL__/')
        name.instance = 'some_instance'
        self.assertEqual(INSTANCE_SIGNAL_PREFIX, name.get_signal_prefix())

    def test_signal_token_name(self):
        TOP_SIGNAL_NAME = '/workflow/__SIGNAL__/some_signal'
        name = Name.from_signal_token_name(TOP_SIGNAL_NAME)
        self.assertEqual('some_signal', name.signal)
        self.assertIsNone(name.workflow)
        self.assertEqual(TOP_SIGNAL_NAME, name.get_signal_token_name())

        WORKFLOW_SIGNAL_NAME = ('/workflow/some_workflow/__SIGNAL__/'
                                'some_signal')
        name = Name.from_signal_token_name(WORKFLOW_SIGNAL_NAME)
        self.assertEqual('some_signal', name.signal)
        self.assertEqual('some_workflow', name.workflow)
        self.assertIsNone(name.instance)
        self.assertEqual(WORKFLOW_SIGNAL_NAME, name.get_signal_token_name())

        INSTANCE_SIGNAL_NAME = ('/workflow/some_workflow/some_instance/'
                                '__SIGNAL__/some_signal')
        name = Name.from_signal_token_name(INSTANCE_SIGNAL_NAME)
        self.assertEqual('some_signal', name.signal)
        self.assertEqual('some_workflow', name.workflow)
        self.assertEqual('some_instance', name.instance)
        self.assertEqual(INSTANCE_SIGNAL_NAME, name.get_signal_token_name())
