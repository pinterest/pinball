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

"""Generic utils shared across pinball modules."""

import base64
import calendar
import datetime
import logging
import os
import pickle
import pytz
import random
import re
import socket
import sys
import threading
import time

from django.conf import settings
from pinball.config.pinball_config import PinballConfig


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class PinballException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)


def get_log(module):
    logging.basicConfig(
        format='[%(asctime)s] - %(name)s - %(levelname)-8s"%(message)s"',
        datefmt='%Y-%m-%d %a %H:%M:%S')
    log = logging.getLogger(module)
    log.setLevel(logging.INFO)
    return log


def sanitize_name(name):
    """Rewrite the name to contain only alphanumerics and underscores.
    This function is used mainly to sanitize token names to simplify their
    parsing.

    Args:
        name: The name to rewrite.
    Returns:
        A rewritten version of the name.  If the original name does not contain
        any forbidden characters, it is returned without change.
    """
    ALLOWED_FORMAT = r'^\w+$'
    if re.match(ALLOWED_FORMAT, name):
        return name
    return base64.b32encode(name).replace('=', '_')


def _check_name_sanitized(name):
    """Check if a name is sanitized.

    Args:
        name: The name to check.
    Raises:
        PinballException: If name is not sanitized.  The message in the
        exception explains why the check failed.
    """
    sanitized_name = sanitize_name(name)
    if sanitized_name != name:
        raise PinballException('name %s contains disallowed characters.  Name '
                               'may contain only alphanumerics and '
                               'underscores' % name)


# The global store for master name.
_MASTER_NAME = sanitize_name(socket.gethostname())


def master_name(new_master_name=None):
    """Get token master name.

    In a typical case, token master name does not change between restarts of a
    given master server.  This name is used by the persistence layer to
    identify the location of the master state.  This way master can recover
    from failures.

    Args:
        new_master_name: The master name to return in subsequent calls of this
            function.
    Returns:
        A master name.  It defaults to the name of the local host.
    Raises:
        PinballException: If new_master_name is not sanitized.
    """
    global _MASTER_NAME
    if new_master_name:
        _check_name_sanitized(new_master_name)
        _MASTER_NAME = new_master_name
    return _MASTER_NAME


def timestamp_to_str(timestamp):
    """Convert epoch timestamp to a time string in UTC.

    Args:
        timestamp: The epoch time, in seconds, to convert.
    Returns:
        String representation of the timestamp in UTC timezone.
    """
    if not timestamp:
        return ''
    if timestamp == sys.maxint:
        # sys.maxint represents infinity.
        return 'inf'
    utc = pytz.timezone('UTC')
    return datetime.datetime.fromtimestamp(timestamp, tz=utc).strftime(
        '%Y-%m-%d %H:%M:%S %Z')


def str_to_timestamp(time_str):
    """Convert a time string to epoch timestamp.

    Args:
        time_str: String representation of the timestamp in UTC timezone.
    Returns:
        The epoch time, in seconds.
    """
    date = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S %Z')
    return calendar.timegm(date.utctimetuple())


def get_unique_name():
    """Generate a globally unique name.

    Returns:
        A name that is unique across threads, processes, machines, and time.
        The name contains only alphanumerics and underscores.
    """
    # TODO(pawel): make sure this is truly unique e.g., by memorizing all names
    # issued during the last millisecond or whatever the resolution of
    # time.time() is.
    hostname = sanitize_name(socket.gethostname())
    thread_name = sanitize_name(threading.current_thread().name)
    return '%s_%d_%d_%s_%d_%d_%d' % (hostname,
                                     PinballConfig.GENERATION,
                                     os.getpid(),
                                     thread_name,
                                     threading.current_thread().ident,
                                     int(time.time() * 1000),
                                     int(random.random() * sys.maxint))


def token_data_to_str(token_data):
    try:
        return str(pickle.loads(token_data))
    except:
        return token_data


def token_to_str(token):
    data_str = token_data_to_str(token.data)
    if token.expirationTime:
        if token.expirationTime == sys.maxint:
            expiration = 'inf (%d)' % token.expirationTime
        else:
            expiration = timestamp_to_str(token.expirationTime)
    else:
        expiration = str(token.expirationTime)
    return ('Token(version=%d, owner=%s, expirationTime=%s, priority=%f, '
            'name=%s, data=%s)' % (token.version, token.owner, expiration,
                                   token.priority, token.name, data_str))


def set_django_environment():
    settings.configure(DEBUG=PinballConfig.DEBUG,
                       ALLOWED_HOSTS=PinballConfig.ALLOWED_HOSTS,
                       SECRET_KEY=PinballConfig.SECRET_KEY,
                       INSTALLED_APPS=PinballConfig.INSTALLED_APPS,
                       MIDDLEWARE_CLASSES=PinballConfig.MIDDLEWARE_CLASSES,
                       ROOT_URLCONF=PinballConfig.ROOT_URLCONF,
                       TEMPLATE_DIRS=PinballConfig.TEMPLATE_DIRS,
                       STATICFILES_DIRS=PinballConfig.STATICFILES_DIRS,
                       STATIC_ROOT=PinballConfig.STATIC_ROOT,
                       MANAGERS=PinballConfig.MANAGERS,
                       STATIC_URL=PinballConfig.STATIC_URL,
                       TEMPLATE_CONTEXT_PROCESSORS=
                       PinballConfig.TEMPLATE_CONTEXT_PROCESSORS,
                       DATABASES=PinballConfig.DATABASES)
