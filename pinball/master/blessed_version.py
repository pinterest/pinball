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

"""Definition of a token used to generate unique version values.

Blessed version is stored in the master as any other token.  Each time a new
version number is needed, it is generated off the value stored in that token.
The value stored in blessed version is a monotonically increasing counter
so it is guaranteed that no single value is issued more than once.
"""

import sys
import time

from pinball.config.utils import timestamp_to_str
from pinball.master.thrift_lib.ttypes import Token


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class BlessedVersion(Token):
    """A singleton token keeping track of token versions.

    Versions of tokens stored in a given master are required to be unique.
    """

    def __init__(self, name=None, owner=None):
        """Create blessed version with a given name and owner.

        Name and owner have to either both be set or none should be set.
        Blessed version in use should always have name and owner set.  The
        version of init with name and owner set to None relies on external
        initialization of those fields.
        Args:
            name: The name of the blessed version token.
            owner: The owner of the blessed version token.
        """
        assert (name and owner) or (not name and not owner)
        if name and owner:
            now = BlessedVersion._get_timestamp_millis()
            data_str = ('blessed version created at %s' %
                        timestamp_to_str(now / 1000))
            Token.__init__(self, now, name, owner, sys.maxint, 0, data_str)
        else:
            Token.__init__(self)

    @staticmethod
    def from_token(token):
        blessed_version = BlessedVersion()
        for key, value in token.__dict__.items():
            blessed_version.__dict__[key] = value
        return blessed_version

    @staticmethod
    def _get_timestamp_millis():
        """Return time in milliseconds since the epoch."""
        return int(time.time() * 1000)

    def advance_version(self):
        """Increase the internal version counter.

        The counter value is based on the current time.  Since those values
        are used as token modification ids, basing them on time has an
        advantage for debugging - looking at the version we can tell when a
        token was modified.

        A BIG WARNING: as an application developer do not assume anything about
        the semantics of version values other than their uniqueness.  The
        implementation details are subject to change.
        """
        self.version = max(self.version + 1,
                           BlessedVersion._get_timestamp_millis())
        return self.version
