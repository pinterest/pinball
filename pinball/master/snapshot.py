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

"""Snapshot maintains a collection of tokens matching a given query."""


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class Snapshot(object):
    def __init__(self, client, request):
        self._client = client
        self._request = request
        self._response = None
        self.refresh()

    def refresh(self):
        """Query the master.

        Returns:
            True if the local copy of the tokens has changed.  Otherwise False.
        """
        old_response = self._response
        self._response = self._client.query(self._request)
        return self._response != old_response
