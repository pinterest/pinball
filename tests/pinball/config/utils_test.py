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

"""Validation tests for utils."""
import pickle
import re
import unittest

from pinball.config.utils import get_unique_name
from pinball.config.utils import timestamp_to_str
from pinball.config.utils import token_to_str
from pinball.config.utils import str_to_timestamp
from pinball.master.thrift_lib.ttypes import Token


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class UtilsTestCase(unittest.TestCase):
    def test_get_unique_name(self):
        """Verify that unique name contains only allowed characters."""
        name = get_unique_name()
        self.assertIsNotNone(re.match(r'^\w+$', name))

    def test_token_to_string(self):
        """Test conversion of token to string."""
        token = Token(version=12345, name='/some_name', owner='some_owner',
                      expirationTime=10, priority=1.5,
                      data=pickle.dumps('some_data'))
        self.assertEqual('Token(version=12345, owner=some_owner, '
                         'expirationTime=1970-01-01 00:00:10 UTC, '
                         'priority=1.500000, name=/some_name, data=some_data)',
                         token_to_str(token))

    def test_timestamp_to_from_str(self):
        self.assertEqual('1970-01-01 00:00:10 UTC', timestamp_to_str(10))
        self.assertEqual(10, str_to_timestamp('1970-01-01 00:00:10 UTC'))
