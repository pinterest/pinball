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

"""Validation tests for log saver"""

import unittest

from pinball.config.pinball_config import PinballConfig
from pinball.workflow import log_saver


__author__ = 'Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class FileLogSaverTestCase(unittest.TestCase):
    def setUp(self):
        self._local_file_path = '/tmp/local/file/path'
        self._log_saver = log_saver.FileLogSaver.from_path(self._local_file_path)

    def test_from_path(self):
        isinstance(self._log_saver, log_saver.FileLogSaver)


class S3FileLogSaverTestCase(unittest.TestCase):
    def setUp(self):
        PinballConfig.S3_LOGS_DIR_PREFIX = 's3n://bucket_name/'
        self._s3_path = 's3n://bucket_name/pinball_job_logs/rest_file_path'
        self._log_saver = log_saver.FileLogSaver.from_path(self._s3_path)

    def test_from_path(self):
        isinstance(self._log_saver, log_saver.S3FileLogSaver)

    def test_local_file_log_saver(self):
        isinstance(self._log_saver._local_file_log_saver, log_saver.FileLogSaver)
        self.assertEqual(self._log_saver._local_file_log_saver._file_path,
                         PinballConfig.LOCAL_LOGS_DIR+'rest_file_path')

# TODO(mao): Add more comprehensive tests including
# 1) data is read-from and written-to correct paths,
# 2) s3 log saver creates non-conflicting local file paths,
# 3) writing to s3 log saver delays the upload based on time and size.
