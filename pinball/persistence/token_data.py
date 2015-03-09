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

"""Token data represents the entities stored in the token data attribute."""
import abc


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class TokenData(object):
    """Parent class for entities stored in token data field."""
    __metaclass__ = abc.ABCMeta

    @property
    def _COMPATIBILITY_ATTRIBUTES(self):
        """Return attributes that should be set if missing.

        The set of object attributes may change between versions.  We need the
        ability to deserialize (unpickle) older versions of an object into new
        versions of object implementation.  To this end, we offer the ability
        to fill attributes missing in the serialized (old) objects with default
        values.

        This property may be overridden in subclasses.

        Returns:
            Dictionary of attribute names and their default values to be filled
            in if missing during unpickling.
        """
        return {}

    def __setstate__(self, state):
        self.__dict__ = state
        for attribute, default in self._COMPATIBILITY_ATTRIBUTES.items():
            if attribute not in self.__dict__:
                self.__dict__[attribute] = default
