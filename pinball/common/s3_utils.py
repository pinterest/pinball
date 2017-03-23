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

"""Utilities for interacting with Amazon s3 service."""

import re

import boto

from pinball.config.pinball_config import PinballConfig
from pinball.config.utils import get_log


__author__ = 'Mao Ye, Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = ['Mao Ye', 'Changshu Liu']
__license__ = 'Apache'
__version__ = '2.0'


if not boto.config.has_section('Boto'):
    boto.config.add_section('Boto')
    boto.config.set('Boto', 'http_socket_timeout', '180')

LOG = get_log('pinball.common.s3_utils')


def parse_s3_location(s3_location):
    """Parse s3_location to get the bucket name and the rest of the file path.

    Args:
        s3_location: A string in the form of:
            's3n://<bucket_name>/<rest_of_the_file_path>'.

    Returns:
        bucket_name, rest_of_the_file_path
    """
    try:
        regex = r'\s*s3n://(.+?)/(.+)'
        return re.match(regex, s3_location).groups()
    except:
        raise Exception('Invalid s3 location: %s' % s3_location)


def get_s3_bucket(bucket_name):
    """Get the Boto s3 bucket reference for the given bucket_name.

    Args:
        bucket_name: name of s3 bucket.

    Returns
        s3 bucket object.
    """
    connection = boto.connect_s3(PinballConfig.AWS_ACCESS_KEY_ID,
                                 PinballConfig.AWS_SECRET_ACCESS_KEY)
    assert connection
    bucket = connection.get_bucket(bucket_name, validate=False)
    return bucket


def delete_s3_directory(s3_directory):
    """Delete the given s3 directory."""
    bucket_name, rest_of_dir_path = parse_s3_location(s3_directory)
    bucket = get_s3_bucket(bucket_name)
    rest_of_dir_path = rest_of_dir_path \
        if rest_of_dir_path[-1] == '/' else rest_of_dir_path + '/'
    bucket.delete_keys(bucket.list(prefix=rest_of_dir_path))
