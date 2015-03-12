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

"""Validation tests for the signaller."""
import mock
import pickle
import unittest

from pinball.config.pinball_config import PinballConfig
from pinball.master.factory import Factory
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import Token
from pinball.workflow.name import Name
from pinball.workflow.signaller import Signal
from pinball.workflow.signaller import Signaller
from tests.pinball.persistence.ephemeral_store import EphemeralStore


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class SignallerTestCase(unittest.TestCase):
    def setUp(self):
        self._factory = Factory()
        self._factory.create_master(EphemeralStore())

    def _post_signal_tokens(self):
        """Add some signal tokens to the master."""
        request = ModifyRequest(updates=[])

        signal = Signal(action=Signal.EXIT)
        name = Name(signal='exit')
        signal_token = Token(name=name.get_signal_token_name())
        signal_token.data = pickle.dumps(signal)
        request.updates.append(signal_token)

        signal = Signal(action=Signal.DRAIN)
        name.signal = 'drain'
        name.workflow = 'some_workflow'
        signal_token = Token(name=name.get_signal_token_name())
        signal_token.data = pickle.dumps(signal)
        request.updates.append(signal_token)

        name.instance = '123'
        signal_token = Token(name=name.get_signal_token_name())
        signal_token.data = pickle.dumps(signal)
        request.updates.append(signal_token)

        signal = Signal(action=Signal.ABORT)
        name.signal = 'abort'
        signal_token = Token(name=name.get_signal_token_name())
        signal_token.data = pickle.dumps(signal)
        request.updates.append(signal_token)

        client = self._factory.get_client()
        client.modify(request)

    def test_is_action_set(self):
        client = self._factory.get_client()
        signaller = Signaller(client)
        self.assertFalse(signaller.is_action_set(Signal.EXIT))
        self.assertFalse(signaller.is_action_set(Signal.DRAIN))
        self.assertFalse(signaller.is_action_set(Signal.ABORT))

        self._post_signal_tokens()

        signaller = Signaller(client)
        self.assertTrue(signaller.is_action_set(Signal.EXIT))
        self.assertFalse(signaller.is_action_set(Signal.DRAIN))
        self.assertFalse(signaller.is_action_set(Signal.ABORT))

        signaller = Signaller(client, workflow='some_workflow')
        self.assertTrue(signaller.is_action_set(Signal.EXIT))
        self.assertTrue(signaller.is_action_set(Signal.DRAIN))
        self.assertFalse(signaller.is_action_set(Signal.ABORT))

        signaller = Signaller(client, workflow='some_workflow', instance='123')
        self.assertTrue(signaller.is_action_set(Signal.EXIT))
        self.assertTrue(signaller.is_action_set(Signal.DRAIN))
        self.assertTrue(signaller.is_action_set(Signal.ABORT))

    def test_set_action(self):
        client = self._factory.get_client()

        writing_signaller = Signaller(client)
        writing_signaller.set_action(Signal.EXIT)
        reading_signaller = Signaller(client)
        # New generation.
        self.assertFalse(reading_signaller.is_action_set(Signal.EXIT))
        # Old generation.
        with mock.patch('pinball.workflow.signaller.PinballConfig.GENERATION', 0):
            self.assertTrue(reading_signaller.is_action_set(Signal.EXIT))
        self.assertFalse(reading_signaller.is_action_set(Signal.DRAIN))
        self.assertFalse(reading_signaller.is_action_set(Signal.ABORT))

        writing_signaller = Signaller(client, workflow='some_workflow')
        writing_signaller.set_action(Signal.DRAIN)
        reading_signaller = Signaller(client, workflow='some_workflow')
        # Old generation.
        with mock.patch('pinball.workflow.signaller.PinballConfig.GENERATION', 0):
            self.assertTrue(reading_signaller.is_action_set(Signal.EXIT))
        self.assertTrue(reading_signaller.is_action_set(Signal.DRAIN))
        self.assertFalse(reading_signaller.is_action_set(Signal.ABORT))

        writing_signaller = Signaller(client, workflow='some_workflow',
                                      instance='123')
        writing_signaller.set_action(Signal.ABORT)
        reading_signaller = Signaller(client, workflow='some_workflow',
                                      instance='123')
        # Old generation.
        with mock.patch('pinball.workflow.signaller.PinballConfig.GENERATION', 0):
            self.assertTrue(reading_signaller.is_action_set(Signal.EXIT))
        self.assertTrue(reading_signaller.is_action_set(Signal.DRAIN))
        self.assertTrue(reading_signaller.is_action_set(Signal.ABORT))

    def test_remove_sction(self):
        client = self._factory.get_client()

        writing_signaller = Signaller(client)
        writing_signaller.set_action(Signal.EXIT)
        writing_signaller.remove_action(Signal.EXIT)
        self.assertFalse(writing_signaller.is_action_set(Signal.EXIT))
        reading_signaller = Signaller(client)
        self.assertFalse(reading_signaller.is_action_set(Signal.EXIT))

        writing_signaller = Signaller(client, workflow='some_workflow')
        writing_signaller.set_action(Signal.DRAIN)
        reading_signaller = Signaller(client, workflow='some_workflow')
        self.assertTrue(reading_signaller.is_action_set(Signal.DRAIN))
        writing_signaller.remove_action(Signal.DRAIN)
        self.assertFalse(writing_signaller.is_action_set(Signal.DRAIN))
        reading_signaller = Signaller(client)
        self.assertFalse(reading_signaller.is_action_set(Signal.DRAIN))

        writing_signaller = Signaller(client, workflow='some_workflow',
                                      instance='123')
        writing_signaller.set_action(Signal.ABORT)
        reading_signaller = Signaller(client, workflow='some_workflow',
                                      instance='123')
        self.assertTrue(reading_signaller.is_action_set(Signal.ABORT))
        writing_signaller.remove_action(Signal.ABORT)
        self.assertFalse(writing_signaller.is_action_set(Signal.ABORT))
        reading_signaller = Signaller(client)
        self.assertFalse(reading_signaller.is_action_set(Signal.ABORT))

    def test_get_attribute(self):
        client = self._factory.get_client()

        writing_signaller = Signaller(client)
        writing_signaller.set_action(Signal.EXIT)
        self.assertEqual(PinballConfig.GENERATION,
                         writing_signaller.get_attribute(
                             Signal.EXIT,
                             Signal.GENERATION_ATTR))
        reading_signaller = Signaller(client)
        self.assertEqual(PinballConfig.GENERATION,
                         reading_signaller.get_attribute(
                             Signal.EXIT,
                             Signal.GENERATION_ATTR))

    def test_set_attribute_if_missing(self):
        client = self._factory.get_client()

        writing_signaller = Signaller(client, workflow='some_workflow',
                                      instance='123')

        self.assertFalse(writing_signaller.set_attribute_if_missing(
                         Signal.ARCHIVE, Signal.TIMESTAMP_ATTR, 12345))

        writing_signaller.set_action(Signal.ARCHIVE)
        self.assertTrue(writing_signaller.set_attribute_if_missing(
                        Signal.ARCHIVE, Signal.TIMESTAMP_ATTR, 12345))
        self.assertEqual(12345,
                         writing_signaller.get_attribute(
                             Signal.ARCHIVE,
                             Signal.TIMESTAMP_ATTR))

        self.assertFalse(writing_signaller.set_attribute_if_missing(
                         Signal.ARCHIVE, Signal.TIMESTAMP_ATTR, 123456))

        reading_signaller = Signaller(client, workflow='some_workflow',
                                      instance='123')
        self.assertEqual(12345,
                         reading_signaller.get_attribute(
                             Signal.ARCHIVE,
                             Signal.TIMESTAMP_ATTR))
