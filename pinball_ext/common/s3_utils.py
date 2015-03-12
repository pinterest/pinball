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

"""Utilities for interacting with Amazon s3 service.

Please call config_s3_utils() first before calling any other methods that will
interact with Amazon s3 service in this module.

For more information about, please check:
    - s3: http://aws.amazon.com/s3/
    - boto: http://aws.amazon.com/sdk-for-python/

TODO(csliu): change the utils from c-style to OO style to reuse connection etc.
"""

import os
import re

import boto

from pinball_ext.common.utils import get_logger


__author__ = 'Mao Ye, Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
if not boto.config.has_section('Boto'):
    boto.config.add_section('Boto')
    boto.config.set('Boto', 'http_socket_timeout', '180')

LOG = get_logger('pinball_ext.common.s3_utils')


def config_s3_utils(aws_access_key_id, aws_secret_access_key):
    """Config core parameters for s3_utils module.

    Args:
        aws_access_key_id: first parameter passed to boto.connect_s3()
        aws_secrete_access_key: second parameter passed to boto.connect_s3()

    Returns:
        None
    """
    assert aws_access_key_id
    assert aws_secret_access_key

    global AWS_ACCESS_KEY_ID
    global AWS_SECRET_ACCESS_KEY
    AWS_ACCESS_KEY_ID = aws_access_key_id
    AWS_SECRET_ACCESS_KEY = aws_secret_access_key


def parse_s3_path(s3_path):
    """Parse s3 directory into bucket name and remaining path.

    Args:
        s3_path: a s3 path following the s3n scheme.

    Returns:
        bucket_name, path_name
    """
    directory_regex = r's3n?://(?P<bucket_name>[\w-]+)/(?P<path_name>.+)'
    m = re.search(directory_regex, s3_path)
    if m:
        bucket_name = m.group('bucket_name')
        path_name = m.group('path_name')
        if bucket_name and path_name:
            return bucket_name, path_name
    return None, None


def get_s3_bucket(bucket_name):
    """Get the Boto s3 bucket reference for the given bucket_name.

    Args:
        bucket_name: name of s3 bucket.

    Returns
        s3 Boto bucket object.
    """
    assert AWS_ACCESS_KEY_ID
    assert AWS_SECRET_ACCESS_KEY
    connection = boto.connect_s3(AWS_ACCESS_KEY_ID,
                                 AWS_SECRET_ACCESS_KEY)
    assert connection
    bucket = connection.get_bucket(bucket_name, validate=False)
    return bucket


def get_s3_key(s3_path):
    """Get the Boto s3 key for given s3_path.

    Args:
        s3_path: a s3 path following the s3n scheme.

    Returns
        s3 Boto key object.
    """
    bucket_name, path = parse_s3_path(s3_path)
    bucket = get_s3_bucket(bucket_name)
    key = bucket.get_key(path)
    return key


def get_or_create_s3_key(s3_path):
    """Gets or creates a Boto reference to a s3 key.

    Usage:
        with get_or_create_key(s3_path) as key:
            key.set_contents_from_string(data)

    Args:
        s3_path - of the form 's3n://<bucket_name>/<path>

    Returns:
        the Boto key.
    """
    bucket_name, path = parse_s3_path(s3_path)
    bucket = get_s3_bucket(bucket_name)
    key = bucket.get_key(path)
    if not key:
        key = bucket.new_key(path)
    return key


def list_s3_directory(s3_directory):
    """List all keys under given s3_directory.

    Args:
        s3_directory: a s3 directory path following s3n scheme.

    Returns:
        list of names of s3 keys.
    """
    bucket_name, path_name = parse_s3_path(s3_directory)
    path_name = path_name if path_name[-1] == '/' else path_name + '/'
    bucket = get_s3_bucket(bucket_name)
    rs = bucket.list(prefix=path_name)
    s3_names = [key.name for key in rs]
    return s3_names


def s3_put_string(s3_location, data):
    """Uploads a data string to a particular location on S3.

    Args:
        s3_location - of the form 's3n://<bucket_name>/<path>
        data - a string
    """
    key = get_or_create_s3_key(s3_location)
    key.set_contents_from_string(data)


def delete_s3_directory(s3_directory):
    """Delete the given s3 directory."""
    bucket_name, rest_of_dir_path = parse_s3_path(s3_directory)
    bucket = get_s3_bucket(bucket_name)
    rest_of_dir_path = rest_of_dir_path if rest_of_dir_path[-1] == '/' \
        else rest_of_dir_path + '/'
    bucket.delete_keys(bucket.list(prefix=rest_of_dir_path))


def extract_file_name_from_s3_path(s3_path):
    """Extract file name from a full s3 path."""
    # is os.path.split(str(bucket_entry.name))[1] safe?
    # >>> os.path.split('/foo')
    # ('/', 'foo')
    # >>> os.path.split('/')
    # ('/', '')
    # >>> os.path.split('/foo/')
    # ('/foo', '')
    # So for '/' and '/foo/' the array element #2 will be empty.
    # We can assume S3 will not give us such entries since:
    # 1. the bucket is not empty so '/' will not exist;
    # 2. '/foo/' will not appear as there is no 'directory' on S3
    # and everything is a file.
    return os.path.split(str(s3_path))[1]
