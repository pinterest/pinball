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

"""Validation tests for pinball_util tool."""
import collections
import mock
import unittest

from pinball.tools.pinball_util import Cat
from pinball.tools.pinball_util import Ls
from pinball.tools.pinball_util import Rm
from pinball.tools.pinball_util import Update
from pinball.master.thrift_lib.ttypes import GroupRequest
from pinball.master.thrift_lib.ttypes import GroupResponse
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import ModifyResponse
from pinball.master.thrift_lib.ttypes import Query
from pinball.master.thrift_lib.ttypes import QueryRequest
from pinball.master.thrift_lib.ttypes import QueryResponse
from pinball.master.thrift_lib.ttypes import Token


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class CatTestCase(unittest.TestCase):
    def test_empty(self):
        Options = collections.namedtuple('args', 'recursive command_args')
        options = Options(recursive=False, command_args=['/some_path'])
        command = Cat()
        command.prepare(options)
        client = mock.Mock()
        response = QueryResponse()
        client.query.return_value = response

        output = command.execute(client, None)
        query = Query(namePrefix='/some_path')
        request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(request)
        self.assertEqual('total 0\n', output)

    def test_recursive(self):
        Options = collections.namedtuple('args', 'recursive command_args')
        options = Options(recursive=True, command_args=['/some_path'])
        command = Cat()
        command.prepare(options)

        client = mock.Mock()
        token = Token(version=10,
                      name='/some_path/some_token',
                      owner='some_owner',
                      expirationTime=10,
                      data='some_data')
        query_response = QueryResponse(tokens=[[token]])
        client.query.return_value = query_response

        output = command.execute(client, None)

        query = Query(namePrefix='/some_path')
        query_request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(query_request)

        self.assertEqual('total 1\nToken(version=10, owner=some_owner, '
                         'expirationTime=1970-01-01 00:00:10 UTC, '
                         'priority=0.000000, name=/some_path/some_token, '
                         'data=some_data)\n',
                         output)


class LsTestCase(unittest.TestCase):
    def test_empty(self):
        Options = collections.namedtuple('args', 'recursive command_args')
        options = Options(recursive=False, command_args=['/some_path'])
        command = Ls()
        command.prepare(options)
        client = mock.Mock()
        response = GroupResponse()
        client.group.return_value = response

        output = command.execute(client, None)

        request = GroupRequest(namePrefix='/some_path', groupSuffix='/')
        client.group.assert_called_once_with(request)
        self.assertEqual('total 0\n', output)

    def test_recursive(self):
        Options = collections.namedtuple('args', 'recursive command_args')
        options = Options(recursive=True, command_args='/')
        command = Ls()
        command.prepare(options)
        client = mock.Mock()
        # Respond 10, and that should come in the output of the executed
        # command.
        response = GroupResponse(counts={'/some_path': 10})
        client.group.return_value = response

        output = command.execute(client, None)

        self.assertEqual('total 1\n/some_path [10 token(s)]\n', output)


class RmTestCase(unittest.TestCase):
    def test_empty(self):
        Options = collections.namedtuple('args',
                                         'recursive force command_args')
        options = Options(recursive=False, force=True,
                          command_args=['/some_path'])
        command = Rm()
        command.prepare(options)
        client = mock.Mock()
        response = QueryResponse()
        client.query.return_value = response

        output = command.execute(client, None)

        query = Query(namePrefix='/some_path')
        request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(request)
        self.assertEqual('no tokens found\nremoved 0 token(s)\n', output)

    def test_recursive(self):
        Options = collections.namedtuple('args',
                                         'recursive force command_args')
        options = Options(recursive=True, force=True,
                          command_args=['/some_path'])
        command = Rm()
        command.prepare(options)

        client = mock.Mock()
        token = Token(version=10,
                      name='/some_path/some_token',
                      owner='some_owner',
                      expirationTime=10,
                      data='some_data')
        query_response = QueryResponse(tokens=[[token]])
        client.query.return_value = query_response

        modify_response = ModifyResponse()
        client.modify.return_value = modify_response

        output = command.execute(client, None)

        query = Query(namePrefix='/some_path')
        query_request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(query_request)

        modify_request = ModifyRequest(deletes=[token])
        client.modify.assert_called_once_with(modify_request)

        self.assertEqual('removed 1 token(s)\n', output)


class UpdateTestCase(unittest.TestCase):
    def test_insert(self):
        Options = collections.namedtuple('args', 'name version owner '
                                         'expiration_time priority data '
                                         'command_args')
        options = Options(name='/some_path/some_token',
                          version=None,
                          owner=None,
                          expiration_time=None,
                          priority=0,
                          data=None,
                          command_args=None)
        command = Update()
        command.prepare(options)
        client = mock.Mock()
        output_token = Token(version=10,
                             name='/some_path/some_token',
                             owner='some_owner',
                             expirationTime=10,
                             data='some_data')
        response = ModifyResponse(updates=[output_token])
        client.modify.return_value = response

        output = command.execute(client, None)

        input_token = Token(name='/some_path/some_token')
        request = ModifyRequest(updates=[input_token])
        client.modify.assert_called_once_with(request)
        self.assertEqual('inserted %s\nupdated 1 token\n' % str(output_token),
                         output)

    def test_update(self):
        Options = collections.namedtuple('args', 'name version owner '
                                         'expiration_time priority data '
                                         'command_args')
        options = Options(name='/some_path/some_token',
                          version=10,
                          owner='some_other_owner',
                          expiration_time=100,
                          priority=10,
                          data='some_other_data',
                          command_args=None)
        command = Update()
        command.prepare(options)
        client = mock.Mock()
        output_token = Token(version=11,
                             name='/some_path/some_token',
                             owner='some_other_owner',
                             expirationTime=100,
                             priority=10,
                             data='some_other_data')
        response = ModifyResponse(updates=[output_token])
        client.modify.return_value = response

        output = command.execute(client, None)

        input_token = Token(version=10,
                            name='/some_path/some_token',
                            owner='some_other_owner',
                            expirationTime=100,
                            priority=10,
                            data='some_other_data')
        request = ModifyRequest(updates=[input_token])
        client.modify.assert_called_once_with(request)
        self.assertEqual('updated %s\nupdated 1 token\n' % str(output_token),
                         output)
