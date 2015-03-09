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
import collections
import datetime
import mock
import pytz
import unittest

from pinball.parser.utils import recurrence_str_to_sec
from pinball.parser.utils import schedule_to_timestamp


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class UtilsTestCase(unittest.TestCase):
    def test_recurrence_str_to_sec(self):
        self.assertEqual(10 * 60, recurrence_str_to_sec('10M'))
        self.assertEqual(10 * 60 * 60, recurrence_str_to_sec('10H'))
        self.assertEqual(10 * 24 * 60 * 60, recurrence_str_to_sec('10d'))
        self.assertEqual(10 * 7 * 24 * 60 * 60, recurrence_str_to_sec('10w'))
        self.assertIsNone(recurrence_str_to_sec('10Y'))

    def test_schedule_to_timestamp_with_start_date(self):
        # 1325376000 = 01 Jan 2012 00:00:00 UTC
        self.assertEqual(1325376000, schedule_to_timestamp('00.00.01.000',
                                                           '2012-01-01'))

    def test_schedule_to_timestamp_without_start_date(self):
        # utcnow cannot be monkey patched so we need to add a work around.
        datetime_now = datetime.datetime(2012, 1, 2, 0, 1, 0, 0, pytz.utc)
        Date = collections.namedtuple('date', 'year,month,day')
        date = Date(year=2012, month=1, day=2)
        with mock.patch('pinball.parser.utils.datetime.datetime'
                        ) as datetime_mock:
            datetime_mock.utcnow.return_value = date
            datetime_mock.return_value = datetime_now

            # 1325462460 = 02 Jan 2012 00:01:00 UTC
            self.assertEqual(1325462460, schedule_to_timestamp('00.00.01.000'))
