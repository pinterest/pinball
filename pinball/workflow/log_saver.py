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

"""Logic handling log read/write."""

import abc
import os
import time

from pinball.common import s3_utils
from pinball.config.pinball_config import PinballConfig
from pinball.config.utils import get_log

LOG = get_log('pinball.workflow.log_saver')


__author__ = 'Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class LogSaver(object):
    """Interface of a component reading and writing job execution logs."""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def write(self, content_str):
        """Write content_str to the file.

        Args:
            content_str: The string to be written to the file.
        """
        return

    @abc.abstractmethod
    def read(self):
        """Return the file content as string.

        Returns:
            content_str: The string of the file content.
        """
        return


class FileLogSaver(LogSaver):
    """FileLogSaver class provides basic methods interacting with a local file.

    A list of methods are open, write, read and close.

    Attributes:
        _file_path: A string of the local file path.
        _file_descriptor: A file descriptor of the file in _file_path.
    """
    def __init__(self, file_path):
        self._file_path = file_path
        self._file_descriptor = None

    def __str__(self):
        return self._file_path

    def open(self, mode='a+'):
        """Open the local file.

        Args:
            mode: The default mode is 'a+', i.e., append/read
        """
        self._file_descriptor = open(self._file_path, mode)

    def write(self, content_str):
        """Write to the local file.

        Args:
            content_str: The string to be written to the local file.
        """
        self._file_descriptor.write(content_str)
        self._file_descriptor.flush()

    def read(self):
        """Read content from the local file.

        Returns:
            String content of the local file.
        """
        return self._file_descriptor.read()

    def close(self):
        """Close the local file"""
        self._file_descriptor.close()
        self._file_descriptor = None

    @staticmethod
    def from_path(file_path):
        """A factory method which returns the right LogSaver class for the given file path.

        A list of supported file path includes:
        1) local file, which is supported by FileLogSaver;
        2) s3 file, which is supported by S3FileLogSaver.

        Args:
            file_path: A string presentation of the file path.

        Returns:
            A LogSaver class instance.
        """
        if file_path.startswith('s3n://'):
            return S3FileLogSaver(file_path)
        else:
            return FileLogSaver(file_path)


class S3FileLogSaver(FileLogSaver):
    """S3FileLogSaver class provides basic methods interacting with remote s3 file.

    A list of methods are open, close, read, write.
    Note that there is no appending operation in s3 file system. In order to implement
    write to s3, we write the content to local file first, then upload the content from
    local file to s3. Considering the write performance, we only upload the content from
    local file to s3, when either of the following two conditions is satisfied.

    Condition-1: it has been a long time since the last upload to s3
    Condition-2: there is a lot new content flushed into the local file, and waiting
    to be uploaded to s3

    Attributes:
        _S3_UPLOAD_INTERVAL_IN_SEC: A class level setting for Condition-1.
        _S3_UPLOAD_BATCH_IN_BYTE:  A class level setting for Condition-2.

        _file_path: The s3 file path to read from or write to.
        _s3_key: A Boto refernce to an s3 key for the _file_path
        _local_file_log_saver: This is a class instance of FileLogSaver,
        which helps to read and write content to a local file.

        _last_remote_upload_time: The last time (in second) when we uploaded the file to s3.
        _pending_bytes: The size of the data that is pending to be written to s3 file.
    """
    _S3_UPLOAD_BATCH_IN_BYTE = 1000
    _S3_UPLOAD_INTERVAL_IN_SEC = 3*60

    def __init__(self, file_path):
        super(S3FileLogSaver, self).__init__(file_path)
        local_file_path = self._file_path.replace(
            PinballConfig.S3_LOGS_DIR_PREFIX,
            PinballConfig.LOCAL_LOGS_DIR_PREFIX)
        self._local_file_log_saver = FileLogSaver(local_file_path)
        self._last_remote_upload_time = time.time()
        self._pending_bytes = 0L
        self._s3_key = None

    def open(self, mode=None):
        """Open S3FileLogSaver to make it ready to read/write.

        More specifically, we need a s3 key for ready/write to remote s3 file,
        and open the LogSaver for local file as well.

        """
        # TODO(Mao): With "a+" mode, we need to warn if local file is missing
        #  while there is a file in s3.
        self._s3_key = self._get_or_create_s3_key(self._file_path)

    def close(self):
        """Close S3FileLogSaver. No further operation on the saver are permitted.

        Note that, we need to make sure all the content which is stored in
        the local file is uploaded to s3.
        """
        self._sync_to_s3()
        self._s3_key = None

        LOG.info("deleting local file: %s as all content is uploaded.",
                 self._local_file_log_saver._file_path)
        if os.path.exists(self._local_file_log_saver._file_path):
            try:
                os.remove(self._local_file_log_saver._file_path)
            except OSError, e:
                LOG.warn('deletion failed due to: %s', e)

    def _check_s3_upload_condition(self):
        """Check whether to upload local log file to remote s3 storage.

        There are two conditions which are related to the class level attributes:
        _S3_UPLOAD_BATCH_IN_BYTE and _S3_UPLOAD_INTERVAL_IN_SEC.

        Returns:
            True: If either of the two conditions is satisfied.
        """
        if self._pending_bytes >= self._S3_UPLOAD_BATCH_IN_BYTE:
            return True
        elif time.time() - self._last_remote_upload_time >= self._S3_UPLOAD_INTERVAL_IN_SEC:
            return True

    def write(self, content_str):
        """Write the content_str to remote s3 storage.

        Since there is no appending operation in s3 storage,
        we write the content_str to local file, and then upload
        the local file to the remote s3 storage.

        Args:
            content_str: The string to be written to s3.
        """

        # First write the content_str to local file
        self._local_file_log_saver.open()
        self._write_to_local_file(content_str)
        self._local_file_log_saver.close()

        # Check if we need to upload the local file to remote s3 storage.
        self._pending_bytes += len(content_str)
        if self._check_s3_upload_condition():
            self._sync_to_s3()
            self._last_remote_upload_time = time.time()
            self._pending_bytes = 0L

    def read(self):
        """Read from a s3 file."""
        return self._s3_key.get_contents_as_string()

    def _sync_to_s3(self):
        """Upload data from local file to remote s3 storage."""
        self._local_file_log_saver.open()
        content = self._local_file_log_saver.read()
        self._local_file_log_saver.close()
        self._s3_key.set_contents_from_string(content)
        LOG.info("%d bytes of data has been uploaded to s3 path %s",
                 len(content),
                 self._file_path)

    def _write_to_local_file(self, content_str):
        """Write content_str to a local file."""
        self._local_file_log_saver.write(content_str)

    @staticmethod
    def _get_or_create_s3_key(s3_location):
        """Get or create a Boto reference to an s3 key of the s3_location."""
        bucket_name, path = s3_utils.parse_s3_location(s3_location)
        bucket = s3_utils.get_s3_bucket(bucket_name)
        key = bucket.get_key(path)
        if not key:
            key = bucket.new_key(path)
        return key

