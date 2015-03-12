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

"""Validation tests for transactions."""
import copy
import pickle
import pytrie
import sys
import unittest

from pinball.master.blessed_version import BlessedVersion
from pinball.master.master_handler import MasterHandler
from pinball.master.thrift_lib.ttypes import ArchiveRequest
from pinball.master.thrift_lib.ttypes import GroupRequest
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import Query
from pinball.master.thrift_lib.ttypes import QueryAndOwnRequest
from pinball.master.thrift_lib.ttypes import QueryRequest
from pinball.master.thrift_lib.ttypes import Token
from pinball.master.thrift_lib.ttypes import TokenMasterException
from pinball.master.transaction import ArchiveTransaction
from pinball.master.transaction import GroupTransaction
from pinball.master.transaction import ModifyTransaction
from pinball.master.transaction import QueryAndOwnTransaction
from pinball.master.transaction import QueryTransaction
from tests.pinball.persistence.ephemeral_store import EphemeralStore


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class TransactionTestCase(unittest.TestCase):
    def setUp(self):
        """Set up self._trie with 111 tokens, one of them a blessed version."""
        self._trie = pytrie.StringTrie()
        self._store = EphemeralStore()
        blessed_version = BlessedVersion(MasterHandler._BLESSED_VERSION,
                                         MasterHandler._MASTER_OWNER)
        for i in range(0, 10):
            some_token = Token(blessed_version.advance_version(),
                               '/some_dir/some_token_%d' % i,
                               priority=i,
                               data='some_data_%d' % i)
            self._trie[some_token.name] = some_token
            self._store.commit_tokens(updates=[some_token])
            for j in range(0, 10):
                some_other_token = Token(
                    blessed_version.advance_version(),
                    '/some_dir/some_token_%d/some_other_token_%d' % (i, j),
                    priority=j,
                    data='some_data_%d_%d' % (i, j))
                self._trie[some_other_token.name] = some_other_token
                self._store.commit_tokens(updates=[some_other_token])
        blessed_version.advance_version()
        self._trie[MasterHandler._BLESSED_VERSION] = blessed_version
        self._store.commit_tokens(updates=[blessed_version])
        self._check_version_uniqueness()

    def _check_version_uniqueness(self):
        """Check self._trie.values() have distinct version values."""
        versions = set()
        for token in self._trie.values():
            versions.add(token.version)
        self.assertEqual(len(self._trie), len(versions))

    def _get_blessed_version(self):
        return self._trie[MasterHandler._BLESSED_VERSION]

    # Archive tests.
    def test_archive_empty(self):
        request = ArchiveRequest()
        transaction = ArchiveTransaction()
        # Make sure that prepare and commit do not throw an exception.
        transaction.prepare(request)
        transaction.commit(self._trie,
                           self._get_blessed_version(),
                           self._store)

    def test_archive(self):
        request = ArchiveRequest(tokens=[])
        n_tokens_before = len(self._trie)
        some_token = copy.copy(self._trie['/some_dir/some_token_0'])
        request.tokens.append(some_token)
        some_other_token = copy.copy(
            self._trie['/some_dir/some_token_0/some_other_token_0'])
        request.tokens.append(some_other_token)
        transaction = ArchiveTransaction()
        transaction.prepare(request)
        transaction.commit(self._trie,
                           self._get_blessed_version(),
                           self._store)
        n_tokens_after = len(self._trie)
        # We deleted two things from self._trie.
        self.assertEqual(n_tokens_before - 2, n_tokens_after)
        n_active_tokens = len(self._store.read_active_tokens())
        self.assertEqual(n_tokens_after, n_active_tokens)
        n_all_tokens = len(self._store.read_tokens())
        self.assertEqual(n_tokens_before, n_all_tokens)

    # Group tests.
    def test_group_empty(self):
        request = GroupRequest()
        transaction = GroupTransaction()
        # Make sure that prepare and commit do not throw an exception.
        transaction.prepare(request)
        transaction.commit(self._trie,
                           self._get_blessed_version(),
                           self._store)

    def test_group(self):
        request = GroupRequest()
        request.namePrefix = '/some_dir/'
        request.groupSuffix = '/'
        transaction = GroupTransaction()
        transaction.prepare(request)
        response = transaction.commit(self._trie,
                                      self._get_blessed_version(),
                                      self._store)

        expected_groups = set()
        for i in range(0, 10):
            expected_groups.add('/some_dir/some_token_%d' % i)
        groups = set()
        for group, count in response.counts.iteritems():
            groups.add(group)
            self.assertEqual(11, count)
        self.assertEqual(expected_groups, groups)

    # Modify tests.
    def test_modity_empty(self):
        request = ModifyRequest()
        transaction = ModifyTransaction()
        # Make sure that prepare and commit do not throw an exception.
        transaction.prepare(request)
        transaction.commit(self._trie,
                           self._get_blessed_version(),
                           self._store)

    def test_modify_deletes(self):
        request = ModifyRequest(deletes=[])
        n_tokens_before = len(self._trie)
        some_token = copy.copy(self._trie['/some_dir/some_token_0'])
        request.deletes.append(some_token)
        some_other_token = copy.copy(
            self._trie['/some_dir/some_token_0/some_other_token_0'])
        request.deletes.append(some_other_token)
        transaction = ModifyTransaction()
        transaction.prepare(request)
        transaction.commit(self._trie,
                           self._get_blessed_version(),
                           self._store)
        n_tokens_after = len(self._trie)
        # We deleted two things from self._trie.
        self.assertEqual(n_tokens_before - 2, n_tokens_after)
        self._check_version_uniqueness()

    def test_modify_updates(self):
        request = ModifyRequest(updates=[])
        n_tokens_before = len(self._trie)
        token = copy.copy(self._trie['/some_dir/some_token_0'])
        token.data = 'some other data'
        request.updates.append(token)
        new_token = Token(name='/some_other_dir/some_token', data='some data')
        request.updates.append(new_token)
        transaction = ModifyTransaction()
        transaction.prepare(request)
        response = transaction.commit(self._trie,
                                      self._get_blessed_version(),
                                      self._store)

        self.assertEqual(2, len(response.updates))
        self.assertNotEqual(token.version, response.updates[0].version)
        self.assertEqual(token.name, response.updates[0].name)
        self.assertEqual(token.data, response.updates[0].data)
        self.assertLess(0, response.updates[1].version)
        self.assertEqual(new_token.name, response.updates[1].name)
        self.assertEqual(new_token.data, response.updates[1].data)
        n_tokens_after = len(self._trie)
        self.assertEqual(n_tokens_before + 1, n_tokens_after)
        self._check_version_uniqueness()

    def test_modify_no_name_change(self):
        """Do not allow changing token names."""
        request = ModifyRequest(updates=[])
        # pickle gets maximum recursion depth exceeded when traversing
        # the trie, probably a bug in pickle. Setting the recursion limit
        # to a high number fixes it.
        sys.setrecursionlimit(10000)
        trie_before = pickle.dumps(self._trie)
        token = copy.copy(self._trie['/some_dir/some_token_0'])
        token.name = '/some_other_dir/some_token_0'
        request.updates.append(token)
        transaction = ModifyTransaction()
        transaction.prepare(request)
        self.assertRaises(TokenMasterException, transaction.commit,
                          self._trie, self._get_blessed_version(), self._store)
        trie_after = pickle.dumps(self._trie)
        self.assertEqual(trie_before, trie_after)

    def test_modify_deletes_and_updates(self):
        """Updates and deletes in a single request."""
        request = ModifyRequest(updates=[], deletes=[])
        n_tokens_before = len(self._trie)
        delete_token = copy.copy(self._trie['/some_dir/some_token_0'])
        request.deletes.append(delete_token)
        update_token = copy.copy(self._trie['/some_dir/some_token_1'])
        update_token.data = 'some other data'
        request.updates.append(update_token)
        transaction = ModifyTransaction()
        transaction.prepare(request)
        response = transaction.commit(self._trie,
                                      self._get_blessed_version(),
                                      self._store)

        self.assertEqual(1, len(response.updates))
        n_tokens_after = len(self._trie)
        self.assertEqual(n_tokens_before - 1, n_tokens_after)
        self._check_version_uniqueness()

    # Query tests.
    def test_query_empty(self):
        request = QueryRequest()
        transaction = QueryTransaction()
        # Make sure that prepare and commit do not throw an exception.
        transaction.prepare(request)
        transaction.commit(self._trie,
                           self._get_blessed_version(),
                           self._store)

    def test_query(self):
        some_query = Query()
        some_query.namePrefix = '/some_dir'
        some_query.maxTokens = 10
        some_other_query = Query()
        some_other_query.namePrefix = '/some_dir/some_token_0'
        some_other_query.maxTokens = 100
        request = QueryRequest()
        request.queries = [some_query, some_other_query]
        transaction = QueryTransaction()
        transaction.prepare(request)
        response = transaction.commit(self._trie,
                                      self._get_blessed_version(),
                                      self._store)
        self.assertEqual(2, len(response.tokens))
        self.assertEqual(10, len(response.tokens[0]))
        for token in response.tokens[0]:
            self.assertTrue(token.name.startswith('/some_dir'))
            self.assertEqual(9, token.priority)
        self.assertEqual(11, len(response.tokens[1]))
        for token in response.tokens[1]:
            self.assertTrue(token.name.startswith('/some_dir/some_token_0'))

    # Query and own tests.
    def test_query_and_own_empty(self):
        request = QueryAndOwnRequest()
        transaction = QueryAndOwnTransaction()
        # Make sure that prepare and commit do not throw an exception.
        transaction.prepare(request)
        transaction.commit(self._trie,
                           self._get_blessed_version(),
                           self._store)

    def test_query_and_own(self):
        some_token = self._trie['/some_dir/some_token_0']
        some_token.owner = 'some_owner'
        some_token.expirationTime = 10  # in the past
        some_token = self._trie['/some_dir/some_token_1']
        some_token.owner = 'some_owner'
        some_token.expirationTime = sys.maxint  # in the future
        some_query = Query()
        some_query.namePrefix = ''
        some_query.maxTokens = 200
        request = QueryAndOwnRequest()
        request.owner = 'some_other_owner'
        request.expirationTime = sys.maxint
        request.query = some_query
        transaction = QueryAndOwnTransaction()
        transaction.prepare(request)
        response = transaction.commit(self._trie,
                                      self._get_blessed_version(),
                                      self._store)

        # Should have owned all tokens but two: the blessed version and the one
        # token that is already owned.
        self.assertEqual(len(self._trie) - 2, len(response.tokens))
        for token in response.tokens:
            self.assertEquals('some_other_owner', token.owner)
            self.assertEquals(sys.maxint, token.expirationTime)
