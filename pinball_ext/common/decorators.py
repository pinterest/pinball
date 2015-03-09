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

"""Various decorator classes."""

import sys
import time

from pinball_ext.common.utils import get_logger


__author__ = 'Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_logger('pinball_ext.common.decorators.retry')


def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=LOG,
          sleep_func=time.sleep, max_delay=sys.maxint):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    Args:
        ExceptionToCheck: exception to check. May be a tuple of
            exceptions to check.
        tries: an integer, number of times to try (not retry) before
            giving up.
        delay: an integer, initial delay between retries in seconds.
        backoff: an integer, backoff multiplier e.g. value of 2 will
            double the delay each retry
        logger: logging.Logger instance, logger to use. By default,
            we use ``logging.Logger.log``; if None is explicitly specified by
            the caller, ``print`` is used.
        sleep_func: the sleep function to be used for waiting between
            retries. By default, it is ``time.sleep``,
            but it could also be gevent.sleep if we are using this with
            gevent.
        max_delay: the max number of seconds to wait between retries.

    Returns:
        Decorator function.
    """
    def deco_retry(f):

        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck, e:
                    if logger:
                        # Don't evaluate the message string as logger module
                        # uses lazy evaluation: assembles the string only if it
                        # is going to be logged at the current logging level.
                        logger.warning(
                            "%s, Retrying in %d seconds...", e, mdelay)
                    else:
                        print "%s, Retrying in %d seconds..." % (
                            str(e), mdelay)
                    sleep_func(mdelay)
                    mtries -= 1
                    mdelay *= backoff
                    # Don't wait more than max_delay allowed
                    if mdelay > max_delay:
                        mdelay = max_delay
            return f(*args, **kwargs)

        return f_retry  # True decorator.

    return deco_retry
