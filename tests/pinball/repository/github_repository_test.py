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

"""Validation tests for Github repository."""
import base64
import httplib
import mock
import unittest

from pinball.repository.github_repository import GithubRepository


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class GithubRepositoryTestCase(unittest.TestCase):
    def setUp(self):
        self._call_count = 0

    @mock.patch('pinball.repository.github_repository.httplib.'
                'HTTPSConnection')
    def test_get_config(self, httpsconnection_mock):
        conn = mock.Mock()
        httpsconnection_mock.return_value = conn
        response = mock.Mock()
        conn.getresponse.return_value = response
        response.status = httplib.OK
        encoded_content = base64.b64encode('some_content')
        response.read.return_value = '{"content": "%s"}' % encoded_content

        repository = GithubRepository()
        content = repository._get_config('/some_path')
        self.assertEqual('some_content', content)

        self.assertEqual('GET', conn.request.call_args[0][0])
        self.assertEqual('/api/v3/repos/data/configs/contents/some_path',
                         conn.request.call_args[0][1])
        self.assertIsNone(conn.request.call_args[0][2])

    @mock.patch('pinball.repository.github_repository.httplib.'
                'HTTPSConnection')
    def test_put_new_config(self, httpsconnection_mock):
        def _getresponse():
            self.assertLessEqual(self._call_count, 1)
            result = mock.Mock()
            if self._call_count == 0:
                result.status = httplib.NOT_FOUND
                result.read.return_value = '{"message":"Not Found"}'
            else:
                result.status = httplib.CREATED
                # result.read.return_value = '{"type":"file"}'
            self._call_count += 1
            return result

        conn = mock.Mock()
        httpsconnection_mock.return_value = conn
        conn.getresponse = _getresponse

        repository = GithubRepository()
        repository._put_config('/some_path', 'some_content')

        self.assertEqual('GET', conn.request.call_args_list[0][0][0])
        self.assertEqual('/api/v3/repos/data/configs/contents/some_path',
                         conn.request.call_args_list[0][0][1])
        self.assertIsNone(conn.request.call_args_list[0][0][2])

        self.assertEqual('PUT', conn.request.call_args_list[1][0][0])
        self.assertEqual('/api/v3/repos/data/configs/contents/some_path',
                         conn.request.call_args_list[1][0][1])
        encoded_content = base64.b64encode('some_content')
        self.assertEqual('{"content": "%s", "committer": {"name": "Pinball", '
                         '"email": "workflows@pinterest.com"}, "message": '
                         '"updating config"}' % encoded_content,
                         conn.request.call_args_list[1][0][2])

    @mock.patch('pinball.repository.github_repository.httplib.'
                'HTTPSConnection')
    def test_put_replace_config(self, httpsconnection_mock):
        def _getresponse():
            self.assertLessEqual(self._call_count, 1)
            result = mock.Mock()
            if self._call_count == 0:
                result.status = httplib.OK
                result.read.return_value = '{"type":"file", "sha":"abc"}'
            else:
                result.status = httplib.CREATED
            self._call_count += 1
            return result

        conn = mock.Mock()
        httpsconnection_mock.return_value = conn
        conn.getresponse = _getresponse

        repository = GithubRepository()
        repository._put_config('/some_path', 'some_content')

        self.assertEqual('GET', conn.request.call_args_list[0][0][0])
        self.assertEqual('/api/v3/repos/data/configs/contents/some_path',
                         conn.request.call_args_list[0][0][1])
        self.assertIsNone(conn.request.call_args_list[0][0][2])

        self.assertEqual('PUT', conn.request.call_args_list[1][0][0])
        self.assertEqual('/api/v3/repos/data/configs/contents/some_path',
                         conn.request.call_args_list[1][0][1])
        encoded_content = base64.b64encode('some_content')
        self.assertEqual('{"content": "%s", "committer": {"name": "Pinball", '
                         '"email": "workflows@pinterest.com"}, "message": '
                         '"updating config", "sha": "abc"}' % encoded_content,
                         conn.request.call_args_list[1][0][2])

    @mock.patch('pinball.repository.github_repository.httplib.'
                'HTTPSConnection')
    def test_delete_config(self, httpsconnection_mock):
        def _getresponse():
            self.assertLessEqual(self._call_count, 1)
            result = mock.Mock()
            result.status = httplib.OK
            if self._call_count == 0:
                result.read.return_value = '{"type":"file", "sha":"abc"}'
            self._call_count += 1
            return result

        conn = mock.Mock()
        httpsconnection_mock.return_value = conn
        conn.getresponse = _getresponse

        repository = GithubRepository()
        repository._delete_config('/some_path')

        self.assertEqual('GET', conn.request.call_args_list[0][0][0])
        self.assertEqual('/api/v3/repos/data/configs/contents/some_path',
                         conn.request.call_args_list[0][0][1])
        self.assertIsNone(conn.request.call_args_list[0][0][2])

        self.assertEqual('DELETE', conn.request.call_args_list[1][0][0])
        self.assertEqual('/api/v3/repos/data/configs/contents/some_path',
                         conn.request.call_args_list[1][0][1])
        self.assertEqual('{"committer": {"name": "Pinball", '
                         '"email": "workflows@pinterest.com"}, "message": '
                         '"updating config", "sha": "abc"}',
                         conn.request.call_args_list[1][0][2])

    @mock.patch('pinball.repository.github_repository.httplib.'
                'HTTPSConnection')
    def test_list_directory(self, httpsconnection_mock):
        conn = mock.Mock()
        httpsconnection_mock.return_value = conn
        response = mock.Mock()
        conn.getresponse.return_value = response
        response.status = httplib.OK
        response.read.return_value = ('[{"type":"dir", "name":"some_dir"},'
                                      ' {"type":"file", "name":"some_file"}]')

        repository = GithubRepository()
        paths = repository._list_directory('/some_path/', False)
        self.assertEqual(['some_dir/', 'some_file'], paths)

        self.assertEqual('GET', conn.request.call_args[0][0])
        self.assertEqual('/api/v3/repos/data/configs/contents/some_path',
                         conn.request.call_args[0][1])
        self.assertIsNone(conn.request.call_args[0][2])
