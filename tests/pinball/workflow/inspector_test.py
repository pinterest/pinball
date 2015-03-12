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

"""Validation tests for the token hierarchy inspector."""
import unittest

from pinball.master.factory import Factory
from pinball.workflow.inspector import Inspector
from pinball.workflow.name import Name
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import Token
from tests.pinball.persistence.ephemeral_store import EphemeralStore


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class InspectorTestCase(unittest.TestCase):
    def setUp(self):
        self._factory = Factory()
        self._factory.create_master(EphemeralStore())
        self._inspector = Inspector(self._factory.get_client())

    def _post_job_tokens(self):
        """Add some job tokens to the master."""
        request = ModifyRequest(updates=[])
        name = Name(workflow='some_workflow', instance='12345')
        for job_id in range(0, 2):
            if job_id % 2 == 0:
                name.job_state = Name.WAITING_STATE
            else:
                name.job_state = Name.RUNNABLE_STATE
            name.job = 'some_job_%d' % job_id
            job_token = Token(name=name.get_job_token_name())
            request.updates.append(job_token)
        client = self._factory.get_client()
        client.modify(request)

    def _post_event_tokens(self):
        """Add some event tokens to the master."""
        request = ModifyRequest(updates=[])
        name = Name(workflow='some_workflow', instance='12345')
        for job_id in range(0, 2):
            for input_id in range(0, 2):
                for event_id in range(0, 2):
                    name.job = 'some_job_%d' % job_id
                    name.input = 'some_input_%d' % input_id
                    name.event = 'some_event_%d' % event_id
                    event_token = Token(name=name.get_event_token_name())
                    request.updates.append(event_token)
        client = self._factory.get_client()
        client.modify(request)

    def test_inspect_empty_tree(self):
        self.assertEqual([], self._inspector.get_workflow_names())
        self.assertEqual([], self._inspector.get_workflow_instances(
            'some_workflow'))
        self.assertEqual([], self._inspector.get_waiting_job_names(
            'some_workflow', '12345'))
        self.assertEqual([], self._inspector.get_runnable_job_names(
            'some_workflow', '12345'))
        self.assertEqual([], self._inspector.get_event_names('some_workflow',
                                                             '12345',
                                                             'some_job_0',
                                                             'some_input_0'))

    def test_inspect_non_empty_tree(self):
        self._post_job_tokens()
        self._post_event_tokens()
        self.assertEqual(['some_workflow'],
                         self._inspector.get_workflow_names())
        self.assertEqual(['12345'], self._inspector.get_workflow_instances(
            'some_workflow'))
        self.assertEqual(['some_job_0'], self._inspector.get_waiting_job_names(
            'some_workflow', '12345'))
        self.assertEqual(['some_job_1'],
                         self._inspector.get_runnable_job_names(
                             'some_workflow', '12345'))
        event_names = self._inspector.get_event_names('some_workflow',
                                                      '12345',
                                                      'some_job_0',
                                                      'some_input_0')
        event_names = sorted(event_names)
        self.assertEqual(['some_event_0', 'some_event_1'], event_names)
