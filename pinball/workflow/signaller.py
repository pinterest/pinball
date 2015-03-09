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

"""Signal the worker.

Signal structures stored in tokens provide a way to communicate with workflow
workers.  Workers periodically query for signal tokens and execute actions
stored in those tokens.

Signal tokens may be placed at different levels of the workflow hierarchy,
i.e., the top level, the workflow level, and the instance level.  Location of
the signal token defines its scope.  E.g., a DRAIN token posted to
/workflow/__SIGNAL__/ drains all workflows in the system, a token located in
/workflow/<workflow>/__SIGNAL__/ drains instances of workflow <workflow> only,
and a token in /workflow/<workflow>/<instance>/__SIGNAL__/ will drain only
instance <instance> of workflow <workflow>.

TODO(pawel): certain types of signals should be restricted to specific levels.
E.g., EXIT signal should be posted only at the top level, and ABORT token at an
instance level.
"""
import pickle
import time

from pinball.config.pinball_config import PinballConfig
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import Query
from pinball.master.thrift_lib.ttypes import QueryRequest
from pinball.master.thrift_lib.ttypes import Token
from pinball.persistence.token_data import TokenData
from pinball.workflow.name import Name


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class Signal(TokenData):
    """A signal communicates actions to workflow workers."""
    # Action types.
    DRAIN, ABORT, ARCHIVE, EXIT = range(4)

    _ACTION_NAMES = {
        # Finish currently running jobs but do not start new ones.
        DRAIN: 'DRAIN',
        # Abort all running jobs, do not start new ones.
        ABORT: 'ABORT',
        # Archive the workflow if there are no runnable jobs.
        ARCHIVE: 'ARCHIVE',
        # Shut down the worker.  It only makes sense to define this action at
        # the top level.
        EXIT: 'EXIT'
    }

    GENERATION_ATTR, TIMESTAMP_ATTR = range(2)

    _ATTRIBUTE_NAMES = {
        GENERATION_ATTR: 'GENERATION',
        TIMESTAMP_ATTR: 'TIMESTAMP'
    }

    def __init__(self, action, attributes=None):
        self.action = action
        self.attributes = {} if attributes is None else attributes

    @property
    def _COMPATIBILITY_ATTRIBUTES(self):
        return {
            'attributes': {}
        }

    @staticmethod
    def action_to_string(action):
        return Signal._ACTION_NAMES[action]

    @staticmethod
    def attribute_to_string(attribute):
        return Signal._ATTRIBUTE_NAMES[attribute]

    def __str__(self):
        action_str = Signal.action_to_string(self.action)
        return 'Signal(action=%s, attributes=%s)' % (action_str,
                                                     self.attributes)

    def __repr__(self):
        return self.__str__()


class Signaller(object):
    """Signaller delivers and retrieves signals."""
    def __init__(self, client, workflow=None, instance=None):
        assert workflow or not instance
        self._client = client
        self._workflow = workflow
        self._instance = instance
        self._signals = {}  # mapping from action to signal
        self._refresh_actions()

    def _dedup_actions(self, signal_tokens):
        """Remove action duplicates from a list of signal tokens.

        Duplicate actions are possible because the same action may be signalled
        at different levels of the hierarchy.  If it happens, we arbitrarily
        choose one of the duplicates.

        This method refreshes signals stored internally from the list of input
        tokens.

        Args:
            signal_tokens: The signal tokens to dedup.
        """
        self._signals = {}
        for signal_token in signal_tokens:
            signal = pickle.loads(signal_token.data)
            self._signals[signal.action] = signal

    def _refresh_actions(self):
        """Reload actions from the master."""
        request = QueryRequest(queries=[])
        name = Name()

        top_query = Query()
        top_query.namePrefix = name.get_signal_prefix()
        request.queries.append(top_query)

        if self._workflow:
            workflow_query = Query()
            name.workflow = self._workflow
            workflow_query.namePrefix = name.get_signal_prefix()
            request.queries.append(workflow_query)

        if self._instance:
            instance_query = Query()
            name.instance = self._instance
            instance_query.namePrefix = name.get_signal_prefix()
            request.queries.append(instance_query)

        response = self._client.query(request)
        signal_tokens = []
        for tokens in response.tokens:
            signal_tokens.extend(tokens)

        self._dedup_actions(signal_tokens)

    def _get_signal_token(self, action):
        """Retrieve signal for a specific action from the master.

        Args:
            action: The action to get signal for.
        Returns:
            The signal token if found, otherwise None.
        """
        request = QueryRequest(queries=[])
        name = Name(workflow=self._workflow,
                    instance=self._instance,
                    signal=Signal.action_to_string(action))

        query = Query()
        query.namePrefix = name.get_signal_token_name()
        request.queries.append(query)

        response = self._client.query(request)
        assert len(response.tokens) == 1
        tokens = response.tokens[0]
        if not tokens:
            return None
        assert len(tokens) == 1
        return tokens[0]

    def is_signal_present(self, action):
        """Check if a signal is set.

        Consult local signal storage only.  No calls to the master are made.

        Args:
            action: The action we are interested in.
        Returns:
            True iff a signal for the specific action exists.
        """
        return action in self._signals

    def is_action_set(self, action):
        """Check if a signal for a specific action exists.

        Consult local signal storage only.  No calls to the master are made.

        This method is different from is_signal_present as it interprets signal
        attribute set to determine if the signal is relevant in the context of
        the current program.  An example is the EXIT token whose applicability
        depends on the generation.

        Args:
            action: The action we are interested in.
        Returns:
            True iff a signal for the action exists and the action is relevant
            in the local context.
        """
        signal = self._signals.get(action)
        if not signal:
            return False
        if action == Signal.EXIT:
            generation = signal.attributes.get(Signal.GENERATION_ATTR)
            if generation and generation <= PinballConfig.GENERATION:
                return False
        return signal is not None

    def _send_request(self, request):
        """Send modify request to the master.

        If the request fails, locally stored signals get refreshed from the
        master.

        Args:
            request: The modify request to send.
        Returns:
            True iff the request has succeeded.
        """
        try:
            self._client.modify(request)
        except:
            # This can happen if someone concurrently posts the same signal
            # token.
            self._refresh_actions()
            return False
        else:
            return True

    def set_action(self, action):
        """Send a signal with a specific action to the master.

        Local signal store gets updated with the new action if it is
        successfully submitted to the master.  If the communication with the
        master fails, locally stored signals get refreshed.

        Args:
            action: The action to set.
        """
        attributes = {}
        if action == Signal.ABORT:
            attributes[Signal.TIMESTAMP_ATTR] = time.time()
        elif action == Signal.EXIT:
            attributes[Signal.GENERATION_ATTR] = PinballConfig.GENERATION
        signal = self._signals.get(action)
        if signal and signal.attributes == attributes:
            return
        # A signal with the same action but different data may already exist
        # in the master.
        signal_token = self._get_signal_token(action)
        if not signal_token:
            name = Name(workflow=self._workflow, instance=self._instance,
                        signal=Signal.action_to_string(action))
            signal_token = Token(name=name.get_signal_token_name())
        signal = Signal(action, attributes)
        signal_token.data = pickle.dumps(signal)
        request = ModifyRequest(updates=[signal_token])
        if self._send_request(request):
            self._signals[action] = signal

    def remove_action(self, action):
        """Remove signal with a given action from the master.

        Args:
            action: The action to remove.
        """
        if not self.is_signal_present(action):
            return
        signal_token = self._get_signal_token(action)
        if signal_token:
            request = ModifyRequest(deletes=[signal_token])
            self._client.modify(request)
        del self._signals[action]

    def get_attribute(self, action, attribute):
        """Get the value of a specific attribute.

        Args:
            action: The action whose attribute we are interested in.
            attribute: The attribute we are interested in.
        Returns:
            The value of the attribute or None if that attribute was not found.
        """
        signal = self._signals.get(action)
        if not signal:
            return None
        return signal.attributes.get(attribute)

    def set_attribute_if_missing(self, action, attribute, value):
        """Set an attribute value unless that attribute is already set.

        Args:
            action: The action whose attribute should be set.
            attribute: The attribute to set.
            value: The attribute value to set.
        Returns:
            True iff the attribute value was set.  Return False if the signal
            is not set.
        """
        if self.get_attribute(action, attribute) is not None:
            return False
        signal_token = self._get_signal_token(action)
        if not signal_token:
            return False
        signal = pickle.loads(signal_token.data)
        self._signals[action] = signal
        if self.get_attribute(action, attribute) is not None:
            return False
        signal.attributes[attribute] = value
        signal_token.data = pickle.dumps(signal)
        request = ModifyRequest(updates=[signal_token])
        if self._send_request(request):
            self._signals[action] = signal
            return True
        return False
