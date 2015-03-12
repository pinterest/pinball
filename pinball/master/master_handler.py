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

"""Implementation of the token master logic."""
import pytrie
import sys
import threading

from pinball.config.utils import get_log
from pinball.master.blessed_version import BlessedVersion
from pinball.master.transaction import REQUEST_TO_TRANSACTION


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.master.master_handler')


class MasterHandler(object):
    """Handler implementing the token master logic.

    Tokens are stored in a trie where keys are token names while the values are
    the tokens themselves.  Trie structure provides an efficient access to
    operations on token name prefixes such as token querying and counting.

    A special type of singleton token - called the blessed version - is stored
    in the tree with other tokens.  The blessed version is used to generate
    unique version numbers.
    """
    _BLESSED_VERSION = '/__BLESSED_VERSION__'
    _MASTER_OWNER = '__master__'

    def __init__(self, store):
        self._store = store
        self._trie = pytrie.StringTrie()
        self._lock = threading.Lock()
        self._load_tokens()

    def _load_tokens(self):
        try:
            tokens = self._store.read_active_tokens()
            for token in tokens:
                self._trie[token.name] = token
            blessed_version = self._trie.get(MasterHandler._BLESSED_VERSION)
            if blessed_version:
                # Note that blessed_version is an instance of Token class.  We
                # need a BlessedVersion object.
                self._trie[MasterHandler._BLESSED_VERSION] = (
                    BlessedVersion.from_token(blessed_version))
            else:
                assert not self._trie
                self._trie[MasterHandler._BLESSED_VERSION] = BlessedVersion(
                    MasterHandler._BLESSED_VERSION,
                    MasterHandler._MASTER_OWNER)
                self._store.commit_tokens(
                    updates=[self._trie[MasterHandler._BLESSED_VERSION]])
        except:
            LOG.exception('')
            # A failure here may indicate that the token tree was populated
            # partially.  It's dangerous to continue in this state so just
            # exit.
            sys.exit(1)

    def _process_request(self, request):
        transaction_cls = REQUEST_TO_TRANSACTION[request.__class__]
        transaction = transaction_cls()
        transaction.prepare(request)
        with self._lock:
            # TODO(pawel): it would be cleaner to subclass trie implementing
            # auto-persistence in the store when modifying tokens.
            return transaction.commit(self._trie,
                                      self._trie[
                                          MasterHandler._BLESSED_VERSION],
                                      self._store)

    # TODO(pawel): add a meta-operation inferring what to do from the class
    # of the request.

    def archive(self, request):
        return self._process_request(request)

    def group(self, request):
        return self._process_request(request)

    def modify(self, request):
        return self._process_request(request)

    def query(self, request):
        return self._process_request(request)

    def query_and_own(self, request):
        return self._process_request(request)
