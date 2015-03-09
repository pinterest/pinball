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

"""Overrun policy definition."""
from pinball.config.utils import PinballException


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class OverrunPolicy(object):
    """Overrun policy defines what to do if the previous run didn't finish.
    """
    SKIP, ABORT_RUNNING, DELAY, DELAY_UNTIL_SUCCESS, START_NEW = range(5)

    # Policy names and help strings.
    _POLICY_NAMES = {SKIP: ('SKIP', 'Skip execution if already running'),
                     ABORT_RUNNING: ('ABORT_RUNNING',
                                     'Abort the running instance before '
                                     'starting a new one'),
                     DELAY: ('DELAY', 'Delay the execution until the previous '
                             'one finishes'),
                     DELAY_UNTIL_SUCCESS: ('DELAY_UNTIL_SUCCESS', 'Delay the '
                                           'execution until the previous one '
                                           'succeeds'),
                     START_NEW: ('START_NEW', 'Start a new instance in '
                                 'parallel to currently running ones')}

    @staticmethod
    def to_string(policy):
        return OverrunPolicy._POLICY_NAMES[policy][0]

    @staticmethod
    def get_help(policy):
        return OverrunPolicy._POLICY_NAMES[policy][1]

    @staticmethod
    def from_string(policy_name):
        for policy, name in OverrunPolicy._POLICY_NAMES.items():
            if name[0] == policy_name:
                return policy
        raise PinballException('Unknown policy %s' % policy_name)
