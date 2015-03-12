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

"""Validation tests for master handler."""
import sys
import unittest

from pinball.master.master_handler import MasterHandler
from pinball.master.thrift_lib.ttypes import ArchiveRequest
from pinball.master.thrift_lib.ttypes import GroupRequest
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import Query
from pinball.master.thrift_lib.ttypes import QueryAndOwnRequest
from pinball.master.thrift_lib.ttypes import QueryRequest
from pinball.master.thrift_lib.ttypes import Token
from tests.pinball.persistence.ephemeral_store import EphemeralStore


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class MasterHandlerTestCase(unittest.TestCase):
    def _insert_token(self, handler):
        request = ModifyRequest()
        token = Token(name='/some_other_dir/some_token', data='some data')
        request.updates = [token]
        response = handler.modify(request)
        self.assertEqual(1, len(response.updates))
        return response.updates[0]

    def test_archive(self):
        handler = MasterHandler(EphemeralStore())
        token = self._insert_token(handler)
        request = ArchiveRequest()
        request.tokens = [token]
        handler.archive(request)
        # The logic handling the request is tested thoroughly in
        # transaction tests.  Here we only make sure that the plumbing is in
        # place.

    def test_group(self):
        request = GroupRequest()
        request.namePrefix = '/'
        handler = MasterHandler(EphemeralStore())
        response = handler.group(request)
        self.assertEqual(1, len(response.counts))
        self.assertEqual(1, response.counts.values()[0])

    def test_modify(self):
        handler = MasterHandler(EphemeralStore())
        self._insert_token(handler)

    def test_query(self):
        query = Query()
        query.namePrefix = ''
        query.maxTokens = 10
        request = QueryRequest()
        request.queries = [query]
        handler = MasterHandler(EphemeralStore())
        response = handler.query(request)
        self.assertEqual(1, len(response.tokens))

    def test_query_and_own(self):
        query = Query()
        query.namePrefix = ''
        query.maxTokens = 10
        request = QueryAndOwnRequest()
        request.owner = 'some_owner'
        request.expirationTime = sys.maxint
        request.query = query
        handler = MasterHandler(EphemeralStore())
        response = handler.query_and_own(request)
        self.assertEqual(0, len(response.tokens))
