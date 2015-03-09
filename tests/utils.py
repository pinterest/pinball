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

"""Utils for test cases."""

import StringIO


__author__ = 'Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


def setup(setup_method):
    """Decorator for declaring setup methods for test cases."""
    setup_method._is_setup_method = True
    return setup_method


def teardown(teardown_method):
    """Decorator for declaring teardown methods for test cases."""
    teardown_method._is_teardown_method = True
    return teardown_method


class StringIOWithContext(StringIO.StringIO):
    """Subclass of StringIO to allow it to be used as a context manager."""
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_t):
        # Don't suppress exceptions!
        return False
