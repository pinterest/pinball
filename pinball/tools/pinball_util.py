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

"""Command line tool to interact with token master.

TODO(pawel): extend commands in this file to interact not only with the master
but also with the store.

TODO(mao): Make sure proper django setting when interact with store.
"""
import argparse
import sys

from pinball.config.pinball_config import PinballConfig
from pinball.config.utils import token_to_str
from pinball.master.factory import Factory
from pinball.master.thrift_lib.ttypes import GroupRequest
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import Token
from pinball.master.thrift_lib.ttypes import Query
from pinball.master.thrift_lib.ttypes import QueryRequest
from pinball.tools.base import Command
from pinball.tools.base import CommandException
from pinball.tools.base import confirm
from pinball.workflow.name import Name


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


def _get_tokens(prefix, recursive, client):
    """Get tokens for a given name prefix.

    Args:
        prefix: The token name prefix to match.
        recursive: If False, only token with name fully matching the prefix
            will be retrieved.  Otherwise, all tokens with names starting with
            the prefix will be retrieved.
        client: The client to use when communicating with the master.
    Returns:
        List of tokens matching a given prefix.
    """
    result = []
    query = Query(namePrefix=prefix)
    request = QueryRequest(queries=[query])
    response = client.query(request)
    if response.tokens:
        assert len(response.tokens) == 1
        for token in response.tokens[0]:
            if recursive or token.name == prefix:
                result.append(token)
                if not recursive:
                    return result
    return result


class Cat(Command):
    """Show token content."""
    def __init__(self):
        self._prefix = None
        self._recursive = None

    def prepare(self, options):
        self._recursive = options.recursive
        if len(options.command_args) != 1:
            raise CommandException('cat command takes a token name prefix '
                                   'argument')
        self._prefix = options.command_args[0]

    def execute(self, client, store):
        output = ''
        tokens = _get_tokens(self._prefix, self._recursive, client)
        if not tokens:
            output += 'total 0\n'
        else:
            output += 'total %d\n' % len(tokens)
            for token in tokens:
                output += '%s\n' % token_to_str(token)
        return output


class Ls(Command):
    """List tokens in the master."""
    def __init__(self):
        self._prefix = None
        self._recursive = None

    def prepare(self, options):
        self._recursive = options.recursive
        if len(options.command_args) != 1:
            raise CommandException('ls command takes a token name prefix '
                                   'argument')
        self._prefix = options.command_args[0]

    def execute(self, client, store):
        output = ''
        if self._recursive:
            suffix = None
        else:
            suffix = Name.DELIMITER
        request = GroupRequest(namePrefix=self._prefix, groupSuffix=suffix)
        response = client.group(request)
        if not response.counts:
            output += 'total 0\n'
        else:
            output += 'total %d\n' % len(response.counts)
            for group in sorted(response.counts.keys()):
                output += '%s [%d token(s)]\n' % (group,
                                                  response.counts[group])
        return output


class Rm(Command):
    """Remove tokens from the master."""
    def __init__(self):
        self._force = None
        self._prefix = None
        self._recursive = None

    def prepare(self, options):
        self._recursive = options.recursive
        if len(options.command_args) != 1:
            raise CommandException('rm command takes a name prefix argument')
        self._prefix = options.command_args[0]
        self._force = options.force

    def execute(self, client, store):
        output = ''
        tokens = _get_tokens(self._prefix, self._recursive, client)
        deleted = 0
        if not tokens:
            output += 'no tokens found\n'
        else:
            print 'removing:'
            for token in tokens:
                print '\t%s' % token.name
            if self._force or confirm('remove %d tokens' % len(tokens)):
                request = ModifyRequest(deletes=tokens)
                client.modify(request)
                deleted = len(tokens)
        output += 'removed %d token(s)\n' % deleted
        return output


class Update(Command):
    """Insert or update tokens in the master."""
    def __init__(self):
        self._data = None
        self._owner = None
        self._expiration_time = None
        self._version = None
        self._name = None

    def prepare(self, options):
        if not options.name:
            raise CommandException('update command requires token name')
        self._name = options.name
        if options.version:
            self._version = options.version
        else:
            self._version = None
        if ((options.owner and not options.expiration_time) or
                (not options.owner and options.expiration_time)):
            raise CommandException('if either of owner and expiration_time is '
                                   'set, then the other must be set as well')
        if options.owner:
            self._owner = options.owner
        else:
            self._owner = None
        if options.expiration_time:
            self._expiration_time = options.expiration_time
        else:
            self._expiration_time = None
        self._priority = options.priority
        if options.data:
            self._data = options.data
        else:
            self._data = None
        if options.command_args:
            raise CommandException('update command does not take positional '
                                   'arguments')

    def execute(self, client, store):
        output = ''
        token = Token(name=self._name,
                      version=self._version,
                      owner=self._owner,
                      expirationTime=self._expiration_time,
                      priority=self._priority,
                      data=self._data)
        request = ModifyRequest(updates=[token])
        response = client.modify(request)
        assert len(response.updates) == 1
        if token.version is None:
            action = 'inserted'
        else:
            action = 'updated'
        output += '%s %s\n' % (action, str(response.updates[0]))
        output += 'updated 1 token\n'
        return output


_COMMANDS = {'cat': Cat, 'ls': Ls, 'rm': Rm, 'update': Update}


def main():
    parser = argparse.ArgumentParser(
        description='Interact with Pinball master server.')
    parser.add_argument('-p',
                        '--port',
                        dest='port',
                        type=int,
                        default=PinballConfig.MASTER_PORT,
                        help='port of the pinball master server')
    parser.add_argument('-s',
                        '--host',
                        dest='host',
                        default='localhost',
                        help='hostname of the pinball master server')
    parser.add_argument('-f',
                        '--force',
                        dest='force',
                        action='store_true',
                        default=False,
                        help='do not ask for confirmation')
    parser.add_argument('-r',
                        '--recursive',
                        dest='recursive',
                        action='store_true',
                        default=False,
                        help='perform the operation recursively')
    parser.add_argument('-n',
                        '--name',
                        dest='name',
                        help='token name')
    parser.add_argument('-v',
                        '--version',
                        dest='version',
                        type=int,
                        help='token version')
    parser.add_argument('-o',
                        '--owner',
                        dest='owner',
                        help='token owner; must be provided if '
                             'expiration_time is set')
    parser.add_argument('-t',
                        '--expiration_time',
                        dest='expiration_time',
                        type=int,
                        help='ownership expiration time in seconds since '
                             'epoch; must be provided if owner is set')
    parser.add_argument('-d',
                        '--data',
                        dest='data',
                        help='token data')
    parser.add_argument('-i',
                        '--priority',
                        dest='priority',
                        type=float,
                        default=0,
                        help='token priority')
    parser.add_argument('command',
                        choices=_COMMANDS.keys(),
                        help='command name')
    parser.add_argument('command_args',
                        nargs='*')
    options = parser.parse_args(sys.argv[1:])

    command = _COMMANDS[options.command]()
    command.prepare(options)
    factory = Factory(master_hostname=options.host, master_port=options.port)
    client = factory.get_client()
    print command.execute(client, None)


if __name__ == '__main__':
    main()
