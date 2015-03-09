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

"""Event signals system state change.

Events are posted to job inputs.  Job input is a bucket for tokens representing
a class of events (e.g., completion of a specific upstream job).  A job becomes
runnable if it has at least one event in each input.  We call a group of
events with one token in each job input 'triggering' events.  Triggering events
get consumed (i.e., removed from the master) when the job becomes runnable.
"""
from pinball.persistence.token_data import TokenData


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class Event(TokenData):
    def __init__(self, creator=None, attributes=None):
        self.creator = creator
        self.attributes = {} if attributes is None else attributes

    @property
    def _COMPATIBILITY_ATTRIBUTES(self):
        return {
            'attributes': {}
        }

    def __str__(self):
        return 'Event(creator=%s, attributes=%s)' % (self.creator,
                                                     self.attributes)

    def __repr__(self):
        return self.__str__()
