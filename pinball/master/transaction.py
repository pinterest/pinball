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

"""Transactions handling different types of requests."""

import abc
import collections
import copy
import sys
import time

from pinball.config.utils import get_log
from pinball.master.thrift_lib.ttypes import ArchiveRequest
from pinball.master.thrift_lib.ttypes import ErrorCode
from pinball.master.thrift_lib.ttypes import GroupRequest
from pinball.master.thrift_lib.ttypes import GroupResponse
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import ModifyResponse
from pinball.master.thrift_lib.ttypes import QueryAndOwnRequest
from pinball.master.thrift_lib.ttypes import QueryAndOwnResponse
from pinball.master.thrift_lib.ttypes import QueryRequest
from pinball.master.thrift_lib.ttypes import QueryResponse
from pinball.master.thrift_lib.ttypes import TokenMasterException


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.master.transaction')


class Transaction(object):
    """Interface defining a transaction on a token trie."""
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self._updates = []
        self._deletes = []
        self._committed = False
        self._blessed_version = None
        self._store = None
        self._trie = None

    @abc.abstractmethod
    def prepare(self, request):
        """Prepare to handle a provided request."""
        return

    @abc.abstractmethod
    def commit(self, trie, blessed_version, store):
        """Execute the transaction on the provided token trie."""
        return

    @staticmethod
    def _verify_have_version(tokens):
        """Raise an exception if any token does not have a version set."""
        for token in tokens:
            if not token.version:
                raise TokenMasterException(
                    ErrorCode.INPUT_ERROR,
                    'token %s does not have version set' % token.name)

    def _set_trie(self, trie, blessed_version, store):
        self._trie = trie
        self._blessed_version = copy.copy(blessed_version)
        self._store = store

    def _add_update(self, token):
        """Add a token update resetting the token version."""
        token = copy.copy(token)
        token.version = self._blessed_version.advance_version()
        self._updates.append(token)

    def _add_delete(self, token):
        self._deletes.append(token)

    def _commit(self):
        """Merge token updates into the trie."""
        assert not self._committed
        try:
            self._blessed_version.advance_version()
            self._trie[self._blessed_version.name] = self._blessed_version
            self._store.commit_tokens(self._updates + [self._blessed_version],
                                      self._deletes)
            for token in self._updates:
                self._trie[token.name] = token
            for token in self._deletes:
                del self._trie[token.name]
        except:
            # This should never happen but if it does happen, our state will
            # get out of sync so we better crash.
            LOG.exception('')
            sys.exit(1)
        self._committed = True


class ArchiveTransaction(Transaction):
    """Transaction handling archive requests."""
    def __init__(self):
        super(ArchiveTransaction, self).__init__()
        self._request = None

    def prepare(self, request):
        self._request = request
        if request.tokens:
            Transaction._verify_have_version(request.tokens)

    def _verify_archive_tokens(self):
        for token in self._request.tokens:
            existing_token = self._trie.get(token.name)
            if not existing_token:
                raise TokenMasterException(ErrorCode.NOT_FOUND,
                                           'token %s not found' % token.name)
            if token.version != existing_token.version:
                raise TokenMasterException(
                    ErrorCode.VERSION_CONFLICT,
                    'token %s with different version %d found' %
                    (existing_token.name, existing_token.version))

    def commit(self, trie, blessed_version, store):
        self._set_trie(trie, blessed_version, store)
        if self._request.tokens:
            self._verify_archive_tokens()
            try:
                store.archive_tokens(self._request.tokens)
                for token in self._request.tokens:
                    del self._trie[token.name]
            except:
                # This should never happen but if it does happen, our state
                # will get out of sync so we better crash.
                LOG.exception('')
                sys.exit(1)


class GroupTransaction(Transaction):
    """Transaction handling group requests."""
    def __init__(self):
        super(GroupTransaction, self).__init__()
        self._request = None

    def prepare(self, request):
        self._request = request

    def _get_group(self, name):
        assert name.startswith(self._request.namePrefix)
        start = len(self._request.namePrefix)
        if self._request.groupSuffix:
            end = name.find(self._request.groupSuffix, start)
        else:
            end = len(name)
        if end == -1:
            end = len(name)
        return name[:end]

    def commit(self, trie, blessed_version, store):
        response = GroupResponse()
        if self._request.namePrefix:
            response.counts = collections.defaultdict(int)
            names = trie.keys(self._request.namePrefix)
            for name in names:
                group = self._get_group(name)
                response.counts[group] += 1
        return response


class ModifyTransaction(Transaction):
    """Transaction handling update requests."""
    def __init__(self):
        super(ModifyTransaction, self).__init__()
        self._request = None

    def prepare(self, request):
        self._request = request
        if self._request.deletes:
            Transaction._verify_have_version(self._request.deletes)
        # TODO(pawel): add more checks of the request

    def _verify_modify_tokens(self, tokens):
        """Check if tokens on the provided list can be modified."""
        for token in tokens:
            if token.name in self._trie:
                existing_token = self._trie[token.name]
                assert token.name == existing_token.name
                assert existing_token.version
                if existing_token.version != token.version:
                    raise TokenMasterException(
                        ErrorCode.VERSION_CONFLICT,
                        'token %s with different version %d found' %
                        (existing_token.name, existing_token.version))
            elif token.version:
                raise TokenMasterException(ErrorCode.NOT_FOUND,
                                           'token %s not found' % token.name)

    def commit(self, trie, blessed_version, store):
        self._set_trie(trie, blessed_version, store)
        if self._request.updates:
            self._verify_modify_tokens(self._request.updates)
        if self._request.deletes:
            self._verify_modify_tokens(self._request.deletes)
        if self._request.updates:
            for token in self._request.updates:
                self._add_update(token)
        if self._request.deletes:
            for token in self._request.deletes:
                self._add_delete(token)
        self._commit()
        response = ModifyResponse()
        if self._updates:
            response.updates = []
        for token in self._updates:
            response.updates.append(token)
        return response


class QueryTransaction(Transaction):
    """Transaction handling query requests."""
    def __init__(self):
        super(QueryTransaction, self).__init__()
        self._request = None

    def prepare(self, request):
        self._request = request
        # TODO(pawel): add checks of the request

    @staticmethod
    def _sort_on_priority(tokens):
        def _priority(token):
            if not token.priority:
                return 0
            return token.priority
        return sorted(tokens, key=_priority, reverse=True)

    def _get_tokens(self, query):
        """Retrieve tokens matching a given query."""
        matching_tokens = self._trie.values(query.namePrefix)
        if query.maxTokens is None:
            return matching_tokens
        sorted_tokens = QueryTransaction._sort_on_priority(matching_tokens)
        return sorted_tokens[:query.maxTokens]

    def commit(self, trie, blessed_version, store):
        self._set_trie(trie, blessed_version, store)
        response = QueryResponse()
        if self._request.queries:
            response.tokens = []
            for query in self._request.queries:
                response.tokens.append(self._get_tokens(query))
        return response


class QueryAndOwnTransaction(QueryTransaction):
    """Transaction handling query and own requests."""
    @staticmethod
    def _get_timestamp_secs():
        """Return time in seconds since the epoch."""
        return int(time.time())

    @ staticmethod
    def _is_owned(token):
        """Check if a given token is currently owned."""
        return (token.owner and
                token.expirationTime and
                (token.expirationTime >
                 QueryAndOwnTransaction._get_timestamp_secs()))

    def commit(self, trie, blessed_version, store):
        self._set_trie(trie, blessed_version, store)
        response = QueryAndOwnResponse()
        response.tokens = []
        if self._request.query:
            matching_tokens = self._trie.values(self._request.query.namePrefix)
            sorted_tokens = QueryTransaction._sort_on_priority(matching_tokens)
            for token in sorted_tokens:
                if (self._request.query.maxTokens is not None and
                        len(self._updates) >= self._request.query.maxTokens):
                    break
                if not QueryAndOwnTransaction._is_owned(token):
                    token = copy.copy(token)
                    token.owner = self._request.owner
                    token.expirationTime = self._request.expirationTime
                    self._add_update(token)
            self._commit()
            response.tokens = self._updates
        return response


# Mapping from request class to the transaction class that handles requests of
# this type.
REQUEST_TO_TRANSACTION = {ArchiveRequest: ArchiveTransaction,
                          GroupRequest: GroupTransaction,
                          ModifyRequest: ModifyTransaction,
                          QueryAndOwnRequest: QueryAndOwnTransaction,
                          QueryRequest: QueryTransaction}
