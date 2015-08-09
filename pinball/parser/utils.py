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

"""Parser utilities shared across modules."""
import calendar
import datetime
import pytz

from pinball.parser.config_parser import PARSER_CALLER_KEY
from pinball.workflow.utils import load_path


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


def recurrence_str_to_sec(recurrence_str):
    """Convert recurrence string to seconds value.

    Args:
        recurrence_str: The execution recurrence formatted as a numeric value
            and interval unit descriptor, e.b., 1d for a daily recurrence.
    Returns:
        Recurrence in seconds or None if input is misformatted.
    """
    if not recurrence_str or len(recurrence_str) < 2:
        return None
    value = int(recurrence_str[:-1])
    assert value > 0
    unit = recurrence_str[-1]
    if unit == 'w':
        return 7 * 24 * 60 * 60 * value
    elif unit == 'd':
        return 24 * 60 * 60 * value
    elif unit == 'H':
        return 60 * 60 * value
    elif unit == 'M':
        return 60 * value
    else:
        return None


def schedule_to_timestamp(execution_time, start_date=None):
    """Convert schedule specification to timestamp.

    Args:
        run_time: the execution time formatted as %H.%M.%S.%{millisecond}
        start_date: The first execution date formatted as %Y-%m-%d. If not
            present, we use the current date as the start date.
    Returns:
        Datetime object extracted from the schedule.
    """
    if start_date:
        date_arg = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    else:
        date_arg = datetime.datetime.utcnow()
    time_arg = datetime.datetime.strptime(execution_time[:5], '%H.%M')
    parsed_datetime = datetime.datetime(date_arg.year,
                                        date_arg.month,
                                        date_arg.day,
                                        time_arg.hour,
                                        time_arg.minute,
                                        0,  # second
                                        0,  # microsecond
                                        pytz.utc)

    return int(calendar.timegm(parsed_datetime.timetuple()))


def annotate_parser_caller(parser_params, parser_caller):
    if parser_params:
        return dict({PARSER_CALLER_KEY: parser_caller}, **parser_params)
    else:
        return {PARSER_CALLER_KEY: parser_caller}


def load_parser_with_caller(parser_name, parser_params, parser_caller):
    return load_path(parser_name)(annotate_parser_caller(parser_params, parser_caller))
