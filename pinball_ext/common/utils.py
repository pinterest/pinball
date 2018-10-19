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

"""Various misc utilities"""

import logging
import random
import string


try:
    random = random.SystemRandom()
except NotImplementedError:
    logging.getLogger(__name__).error('No system level randomness available. PRNG in software is not secure.')


__author__ = 'Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


def get_random_string(size=10, chars=string.ascii_lowercase + string.digits):
    """Get a random string of given size and composed of given characters.

    Args:
        size: Size of random string generated.
        chars: Set of characters which make up the random string.
    """
    return ''.join(random.choice(chars) for x in range(size))


def get_logger(module):
    logging.basicConfig(
        format='[%(asctime)s] - %(name)s - %(levelname)-8s"%(message)s"',
        datefmt='%Y-%m-%d %a %H:%M:%S')
    log = logging.getLogger(module)
    log.setLevel(logging.INFO)
    return log


def parse_arguments(args):
    """Parse argument text into Python dictionary.

    Args:
        args: a list of arguments separated by ','. Each argument could be
            key/value pair in format: key1=value1 or it could just be a
            flag_name.

    Returns:
        Corresponding Python dict that maps:
            key1->value1,
            flag_name->True
    """
    arg_dict = {}
    if args:
        for arg in args.split(','):
            if '=' in arg:
                # The key from foo=bar is foo. For foo=bar=xyz, the key is foo
                # and the value is bar=xyz.
                key, value = arg.split('=', 1)
            else:
                # Just some existence flag. Assume it is True.
                key = arg
                value = True
            arg_dict[key.strip()] = value
    return arg_dict
