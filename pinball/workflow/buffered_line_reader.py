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

"""Logic handling reading logs from subprocess pipe"""

import os
from StringIO import StringIO


__author__ = 'Jooseong Kim'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class BufferedLineReader(object):
    """Reads text lines from a file object. The input stream may be buffered to
       form a line.

    This class encapsulates a text input stream that returns text lines.
    Python's file object readline() makes a blocking read() system call until
    it sees a new line delimiter in order to return a properly terminated text
    line. When readline() reads from a file descriptor on a file with limited
    buffer, the blocking system call may cause unexpected behavior. This class
    uses non-blocking read (os.read) calls so that it returns once it reads as
    many bytes that can be read from the buffer.

    This class was introduced to prevent the parent and child process
    communicating on pipes from deadlocking. The specific sequence of events
    that cause the deadlock is as follows:
    1. The child process writes a text string without the new line delimiter
       into the stdout pipe and flushes the buffer. As a result, the other end
       of the pipe becomes readable.
    2. select() returns and the pipe for the child's stdout becomes readable.
       The parent tries to read until a new line delimiter is seen, so it will
       block until the child process writes a new line delimiter intothe pipe.
    3. The child process writes a text string longer than the pipe buffer into
       stderr and flushes.

    As a result, the parent process is blocked on a read() on the stdout pipe
    from the child and waits for the child to write a new line delimiter into
    stdout. The child process is blocked on a write() on stderr and waits for
    the parent to read from the stderr pipe.

    Since the parent and child is waiting for each other on an action, this
    cyclic wait cannot be broken.
    """

    # default amount of bytes to read from the file object
    _DEFAULT_READ_SIZE = 1 << 11
    # default maximum size of the buffer we hold
    _DEFAULT_BUFFER_SIZE = 1 << 14

    def __init__(self, input_file, read_size=_DEFAULT_READ_SIZE,
                 max_buffer_size=_DEFAULT_BUFFER_SIZE):
        self._input_file = input_file
        self._read_size = read_size
        self._max_buffer_size = max_buffer_size

        self._reset_buffer()
        self._file_eof = False

    def _reset_buffer(self, initial_content=''):
        """Resets the buffer with initial_content
        """
        self._strio_buffer = StringIO()
        self._buffer_size = 0

        if initial_content:
            self._strio_buffer.write(initial_content)
            self._buffer_size = len(initial_content)

    def eof(self):
        """Returns whether EOF of the file is reached.

        Returns:
            True, if EOF of the file is reached.
            False, otherwise.
        """
        return self._file_eof

    def readlines(self):
        """Reads lines from a file.

        Reads new line (\n) terminated lines and maintains a buffer of text
        that is not yet terminated by a new line.

        Returns:
            list of one incomplete line, if the buffer is full.
            list of full lines read from the file, if such line exists.
            [], if no line could be formed.
        """

        # return empty list if EOF was already reached
        if self.eof():
            return []

        read_buffer = os.read(self._input_file.fileno(),
                              min(self._read_size,
                                  self._max_buffer_size - self._buffer_size))

        # empty string means we reached EOF
        if not read_buffer:
            self._file_eof = True

            # the buffer may have bytes not terminated by a new line, so return
            # the whole buffer.
            str_buffer = self._strio_buffer.getvalue()

            if str_buffer:
                return [str_buffer]
            else:
                return []

        self._strio_buffer.write(read_buffer)
        self._buffer_size += len(read_buffer)

        # new line is not found. There are two possible outcomes:
        # 1. Return empty list, if we still have space in the buffer. The text
        #    already read-in will be kept in _strio_buffer.
        # 2. Return a list with the content of the buffer if we reached the
        #    buffer size limit.
        if read_buffer.find('\n') < 0:
            if self._buffer_size < self._max_buffer_size:
                return []
            else:
                incomplete_line = self._strio_buffer.getvalue()
                self._reset_buffer()
                return [incomplete_line]

        str_buffer = self._strio_buffer.getvalue()

        # there is at least one full line in the buffer
        lines = []
        offset = 0
        while offset < len(str_buffer):
            newline_index = str_buffer.find('\n', offset)

            # no more new lines, so break out
            if newline_index < 0:
                break

            lines.append(str_buffer[offset:newline_index + 1])

            offset = newline_index + 1

        self._reset_buffer(str_buffer[offset:])

        return lines
