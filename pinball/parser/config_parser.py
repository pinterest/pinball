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

"""Parser converting configurations to Pinball tokens."""
import abc


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'

PARSER_CALLER_KEY = 'caller'


class ParserCaller(object):
    ANALYZER = 'analyzer'
    SCHEDULE = 'schedule'
    UI = 'ui'
    WORKFLOW_UTIL = 'workflow_util'


class ConfigParser(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, params=None):
        """Create a config parser instance.

        Args:
            params: The free-form parser initialization parameters.
        """
        return

    @abc.abstractmethod
    def get_schedule_token(self, workflow):
        """Create schedule token for a given workflow.

        Args:
            workflow: The name of the workflow whose schedule token should be
                created.
        Returns:
            The workflow schedule token.
        """
        return

    @abc.abstractmethod
    def get_workflow_tokens(self, workflow):
        """Create tokens describing instance of a workflow.

        Args:
            workflow: The workflow whose tokens should be created.
        Returns:
            Job and event tokens defining a ready-to-run workflow instance.
        """
        return

    @abc.abstractmethod
    def get_workflow_names(self):
        """Get names of all workflows.

        Returns:
            Names of all workflows known to the parser.
        """
        return
