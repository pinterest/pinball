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

from pinball.persistence.store import Store


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class EphemeralStore(Store):
    """Memory-only store used for testing."""
    def initialize(self):
        self._active_tokens = {}
        self._archived_tokens = {}
        self._cached_data = {}

    def commit_tokens(self, updates=None, deletes=None):
        updates = updates if updates is not None else []
        deletes = deletes if deletes is not None else []
        for token in updates:
            self._active_tokens[token.name] = token
        for token in deletes:
            del self._active_tokens[token.name]

    def delete_archived_tokens(self, deletes):
        for token in deletes:
            del self._archived_tokens[token.name]

    @staticmethod
    def _matches(name, name_prefix, name_infix, name_suffix):
        if name_prefix and not name.startswith(name_prefix):
            return False
        if name_infix and not name_infix in name:
            return False
        if name_suffix and not name.endswith(name_suffix):
            return False
        return True

    @staticmethod
    def _filter_tokens(tokens, name_prefix, name_infix, name_suffix):
        result = []
        for token in tokens:
            if EphemeralStore._matches(token.name, name_prefix, name_infix,
                                       name_suffix):
                result.append(token)
        return result

    def read_active_tokens(self, name_prefix='', name_infix='',
                           name_suffix=''):
        return EphemeralStore._filter_tokens(self._active_tokens.values(),
                                             name_prefix,
                                             name_infix,
                                             name_suffix)

    def read_archived_tokens(self, name_prefix='', name_infix='',
                             name_suffix=''):
        return EphemeralStore._filter_tokens(self._archived_tokens.values(),
                                             name_prefix,
                                             name_infix,
                                             name_suffix)

    def archive_tokens(self, tokens):
        for token in tokens:
            del self._active_tokens[token.name]
            self._archived_tokens[token.name] = token

    def get_cached_data(self, name):
        return self._cached_data.get(name)

    def set_cached_data(self, name, data):
        self._cached_data[name] = data

    def read_tokens(self, name_prefix='', name_infix='', name_suffix=''):
        return (EphemeralStore._filter_tokens(self._active_tokens.values(),
                                              name_prefix,
                                              name_infix,
                                              name_suffix) +
                EphemeralStore._filter_tokens(self._archived_tokens.values(),
                                              name_prefix,
                                              name_infix,
                                              name_suffix))

    def read_token_names(self, name_prefix='', name_infix='', name_suffix=''):
        tokens = self.read_tokens(name_prefix, name_infix, name_suffix)
        result = []
        for token in tokens:
            result.append(token.name)
        return result

    def read_archived_token_names(self, name_prefix='', name_infix='',
                                  name_suffix=''):
        tokens = self.read_archived_tokens(name_prefix, name_infix,
                                           name_suffix)
        result = []
        for token in tokens:
            result.append(token.name)
        return result

    def read_cached_data_names(self, name_prefix='', name_infix='',
                               name_suffix=''):
        cached_data_names = self._cached_data.keys()
        result = []
        for name in cached_data_names:
            if EphemeralStore._matches(name, name_prefix, name_infix,
                                       name_suffix):
                result.append(name)
        return result
