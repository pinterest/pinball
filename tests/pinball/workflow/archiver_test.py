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

"""Validation tests for the workflow archiver."""
import mock
import time
import unittest

from pinball.workflow.archiver import Archiver
from pinball.master.thrift_lib.ttypes import ArchiveRequest
from pinball.master.thrift_lib.ttypes import Token
from pinball.master.thrift_lib.ttypes import Query
from pinball.master.thrift_lib.ttypes import QueryRequest
from pinball.master.thrift_lib.ttypes import QueryResponse


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class ArchiverTestCase(unittest.TestCase):
    def setUp(self):
        self._client = mock.Mock()
        self._archiver = Archiver(self._client, 'some_workflow', '123')
        self._job_token = Token(
            version=12345,
            name='/workflow/some_workflow/123/waiting/some_job')
        self._abort_token = Token(
            version=123456,
            name='/workflow/some_workflow/123/__SIGNAL__/ABORT')

    def _prepare_get_instance_tokens(self, response_tokens):
        query_response = QueryResponse([response_tokens])
        self._client.query.return_value = query_response

    def _verify_get_instance_tokens(self):
        query = Query(namePrefix='/workflow/some_workflow/123/')
        query_request = QueryRequest(queries=[query])
        self._client.query.assert_called_once_with(query_request)

    def _verify_archive_tokens(self, request_tokens):
        archive_request = ArchiveRequest(tokens=request_tokens)
        self._client.archive.assert_called_once_with(archive_request)

    def test_archive_if_expired_non_existent(self):
        self._prepare_get_instance_tokens([])
        self.assertFalse(self._archiver.archive_if_expired(10))
        self._verify_get_instance_tokens()

    def test_archive_not_expired(self):
        self._prepare_get_instance_tokens([])
        self.assertFalse(self._archiver.archive_if_expired(time.time() + 1000))
        self.assertEqual(0, self._client.query.call_count)

    def test_archive_expired(self):
        self._prepare_get_instance_tokens([self._job_token])
        self.assertTrue(self._archiver.archive_if_expired(10))
        self._verify_get_instance_tokens()
        self._verify_archive_tokens([self._job_token])

    def test_archive_if_aborted_not_aborted(self):
        self._prepare_get_instance_tokens([self._job_token])
        self.assertFalse(self._archiver.archive_if_aborted())
        self._verify_get_instance_tokens()

    def test_archive_if_aborted_owned(self):
        self._job_token.owner = 'some_owner'
        self._job_token.expirationTime = time.time() + 1000
        self._prepare_get_instance_tokens([self._job_token, self._abort_token])
        self.assertFalse(self._archiver.archive_if_aborted())
        self._verify_get_instance_tokens()

    def test_archive_if_aborted(self):
        self._prepare_get_instance_tokens([self._job_token, self._abort_token])
        self.assertTrue(self._archiver.archive_if_aborted())
        self._verify_get_instance_tokens()
        self._verify_archive_tokens([self._job_token, self._abort_token])
