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

"""Interface and implementation of clients talking to the token master."""

import abc
import random
import socket
import time

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from pinball.config.pinball_config import PinballConfig
from pinball.config.utils import get_log
from pinball.master.thrift_lib.ttypes import ArchiveRequest
from pinball.master.thrift_lib.ttypes import GroupRequest
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import QueryAndOwnRequest
from pinball.master.thrift_lib.ttypes import QueryRequest
from pinball.master.thrift_lib import TokenMasterService


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = ['Pawel Garbacki', 'Mao Ye']
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.master.client')


class Client(object):
    """Interface of a client communicating with token master."""
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        # Mapping from request class to the end point that handles requests of
        # this type.
        self._request_to_end_point = None

    def call(self, request):
        return self._request_to_end_point[request.__class__](request)

    # For description of individual methods, see master.thrift.

    # TODO(pawel): remove these methods after replacing their invocations with
    # calls to call().

    def archive(self, request):
        return self.call(request)

    def group(self, request):
        return self.call(request)

    def modify(self, request):
        return self.call(request)

    def query(self, request):
        return self.call(request)

    def query_and_own(self, request):
        return self.call(request)


class LocalClient(Client):
    """Client communicating with master living in the same address space."""

    def __init__(self, master):
        super(LocalClient, self).__init__()
        self._master = master
        self._request_to_end_point = {
            ArchiveRequest: self._master.archive,
            GroupRequest: self._master.group,
            ModifyRequest: self._master.modify,
            QueryAndOwnRequest: self._master.query_and_own,
            QueryRequest: self._master.query}


class RemoteClient(Client):
    """Thrift client communicating with a remote master."""

    def __init__(self, host, port):
        super(RemoteClient, self).__init__()
        self._host = host
        self._port = port
        self._request_to_end_point = None
        self._connect()

    def __del__(self):
        self._transport.close()

    def _connect(self):
        backoff = PinballConfig.CLIENT_TIMEOUT_SEC
        for i in range(0, PinballConfig.CLIENT_CONNECT_ATTEMPTS):
            try:
                transport = TSocket.TSocket(self._host, self._port)
                transport.setTimeout(1000 * PinballConfig.CLIENT_TIMEOUT_SEC)
                self._transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
                self._client = TokenMasterService.Client(protocol)
                self._request_to_end_point = {
                    ArchiveRequest: self._client.archive,
                    GroupRequest: self._client.group,
                    ModifyRequest: self._client.modify,
                    QueryAndOwnRequest: self._client.query_and_own,
                    QueryRequest: self._client.query}
                self._transport.open()
            except (TTransport.TTransportException, socket.timeout):
                LOG.exception('')
                if i == PinballConfig.CLIENT_CONNECT_ATTEMPTS - 1:
                    raise
                LOG.warning('failed during communication with master.  '
                            'Reconnecting %d / %d' % (
                    i + 1, PinballConfig.CLIENT_CONNECT_ATTEMPTS - 1))
                backoff *= 2
                time.sleep(self._get_backoff_time(backoff))

    def _get_backoff_time(self, backoff):
        """Get the backoff time for the client reconnecting to master."""
        return min(backoff * (1 + random.uniform(-0.5, 0.5)),
                   PinballConfig.MAX_BACKOFF_CLIENT_RECONNECT_SEC)

    def call(self, request):
        try:
            return super(RemoteClient, self).call(request)
        except (TTransport.TTransportException, socket.timeout, socket.error):
            LOG.exception('')
            self._connect()
            return super(RemoteClient, self).call(request)
