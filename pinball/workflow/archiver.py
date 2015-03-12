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

"""Archiver takes care of archiving workflow instance tokens."""
import time

from pinball.config.utils import get_log
from pinball.master.thrift_lib.ttypes import ArchiveRequest
from pinball.master.thrift_lib.ttypes import Query
from pinball.master.thrift_lib.ttypes import QueryRequest
from pinball.master.thrift_lib.ttypes import TokenMasterException

from pinball.workflow.name import Name
from pinball.workflow.signaller import Signal


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.workflow.archiver')


class Archiver(object):
    def __init__(self, client, workflow, instance):
        self._client = client
        self._workflow = workflow
        self._instance = instance

    def _get_instance_tokens(self):
        """Retrieve all workflow instance tokens.

        Returns:
            List of tokens in the workflow instance.
        """
        prefix = Name(workflow=self._workflow, instance=self._instance)
        query = Query(namePrefix=prefix.get_instance_prefix())
        query_request = QueryRequest(queries=[query])
        try:
            query_response = self._client.query(query_request)
        except TokenMasterException:
            LOG.exception('error sending request %s', query_request)
            return None
        if not query_response.tokens:
            return None
        assert len(query_response.tokens) == 1
        return query_response.tokens[0]

    def _archive_tokens(self, tokens):
        """Archive tokens.

        Args:
            tokens: The list of tokens to archive.
        Returns:
            True iff tokens have been successfully archived.
        """
        archive_request = ArchiveRequest(tokens=tokens)
        try:
            self._client.archive(archive_request)
        except TokenMasterException:
            # It's no big deal if the request failed this time - the same or a
            # different worker will try it again some other time.
            LOG.exception('error sending request %s', archive_request)
            return False
        return True

    def archive_if_expired(self, expiration_timestamp):
        """Archive workflow instance tokens if we passed expiration time.

        Args:
            expiration_timestamp: Timestamp that has to be reached before
                archiving tokens.
        Returns:
            True iff the workflow was successfully archived.
        """
        if expiration_timestamp > time.time():
            return False
        workflow_tokens = self._get_instance_tokens()
        if not workflow_tokens:
            return False
        return self._archive_tokens(workflow_tokens)

    def _has_abort_token(self, tokens):
        """Check if a list of tokens contains an abort token.

        Args:
            tokens: The list of tokens to check.
        Returns:
            True iff the list contains an abort token.
        """
        abort_signal = Signal.action_to_string(Signal.ABORT)
        abort_name = Name(workflow=self._workflow,
                          instance=self._instance,
                          signal=abort_signal)
        abort_token_name = abort_name.get_signal_token_name()
        for token in tokens:
            if token.name == abort_token_name:
                return True
        return False

    @staticmethod
    def _is_owned(token):
        """Check if a given token is owned.

        Returns:
            True iff the given token is owned. Due to a possible clock skew
            between machines it is hard to tell with a high confidence if a
            token is owned at a given time.  We prefer to err on the false
            positive side - i.e., if the result is False, we are highly
            confident that the token is not owned, but not the opposite.
        """
        # Clocks can be off by this much on different machines.
        CLOCK_SKEW_THRESHOLD_SEC = 10
        timestamp = time.time()
        if not token.expirationTime:
            return False
        return timestamp - token.expirationTime < CLOCK_SKEW_THRESHOLD_SEC

    @staticmethod
    def _has_owned_tokens(tokens):
        """Check if any token in a given list is owned.

        Args:
            tokens: The tokens whose ownership should be assessed.
        Returns:
            True iff the list contains an owned token.
        """
        for token in tokens:
            if Archiver._is_owned(token):
                return True
        return False

    def archive_if_aborted(self):
        """Archives workflow instance if it has been aborted.

        Returns:
            True iff the workflow has been aborted.
        """
        workflow_tokens = self._get_instance_tokens()
        if (not workflow_tokens or
                not self._has_abort_token(workflow_tokens) or
                Archiver._has_owned_tokens(workflow_tokens)):
            return False
        return self._archive_tokens(workflow_tokens)
