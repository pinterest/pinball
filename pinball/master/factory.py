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

"""Factory for creating token master and client objects."""
import socket

from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket
from thrift.transport import TTransport

from pinball.config.utils import get_log
from pinball.master.client import LocalClient, RemoteClient
from pinball.master.master_handler import MasterHandler
from pinball.master.thrift_lib.TokenMasterService import Processor


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.master.factory')


class Factory(object):
    """Factory creating token master and clients."""

    def __init__(self, master_hostname=None, master_port=None):
        """Create a factory.

        Args:
            master_hostname: Hostname of the master server.  Not required if
                master is running locally.  Defaults to the name of the local
                host.
            master_port: Port of the master server.  Not required if master
                is running locally.
        """
        self._master_handler = None
        if master_hostname:
            self._hostname = master_hostname
        else:
            self._hostname = socket.gethostname()
        self._port = master_port

    def create_master(self, store):
        """Create a local master.

        Args:
            store: The store where the master persists tokens.
        """
        self._master_handler = MasterHandler(store)

    def run_master_server(self):
        """Start thrift token master server and block waiting until it's done.
        """
        assert self._master_handler
        processor = Processor(self._master_handler)
        if self._port:
            transport = TSocket.TServerSocket(port=self._port)
        else:
            transport = TSocket.TServerSocket()
            self._port = transport.port
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        server = TServer.TThreadedServer(processor, transport, tfactory,
                                         pfactory)

        LOG.info('Starting server on host:port %s:%d', self._hostname,
                 self._port)
        server.serve()
        LOG.info('server is done')

    def get_client(self):
        """Create local or remote client depending on the master availability.

        Return:
            A local client if this factory was used to create a master.
            Otherwise, return a thrift client connected to a remote master.  In
            the latter case, hostname and port must have been provided in the
            factory constructor.
        """
        if self._master_handler:
            return LocalClient(self._master_handler)
        else:
            if not self._hostname or not self._port:
                raise Exception('hostname and port must be defined')
            return RemoteClient(self._hostname, self._port)
