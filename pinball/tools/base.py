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

"""Constructs shared by multiple tools."""
import abc

from pinball.config.utils import PinballException


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class CommandException(PinballException):
    pass


class Command(object):
    """Interface for commands interacting with the master."""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def prepare(self, options):
        """Parse command options."""
        return

    @abc.abstractmethod
    def execute(self, client, store):
        """Execute the command using a provided client.

        Args:
            client: The client connected to the master.
            store: The token store.
        Returns:
            Output of the command.  We return a string rather than printing it
            to stdout to make testing easier - mocking 'print' is a pain.
        """
        # TODO(pawel): modify this method to return a tuple: (exit_code,
        # stdout, stderr)
        return


def confirm(prompt='Confirm'):
    """Prompt for yes or no response from the user.

    Args:
        prompt: A message to show when asking for confirmation.
    Returns:
        True iff the user approved the action.
    """
    prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        ans = raw_input(prompt)
        if not ans:
            return False
        if ans not in ['y', 'Y', 'n', 'N']:
            print 'please enter y or n.'
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False
