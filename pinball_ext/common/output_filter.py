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

import sys


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class OutputFilter(object):
    """A filter processing a stream and generating magic properties."""
    def __init__(self, log_line_processor, output=sys.stdout):
        self._log_line_processor = log_line_processor
        self._output = output
        self._buffer = None

    def _generate_magic(self, line):
        """Process log line potentially generating Pinball magic properties.

        Args:
            line: The line to process.

        Returns:
            A string with Pinball magic properties or an empty string if no
            properties were generated.
        """
        key_values = self._log_line_processor(line)
        result = ''
        for key, value in key_values.items():
            result += 'PINBALL:%s=%s\n' % (key, value)
        return result

    def _write(self, line):
        """Write a line to the output or to the buffer.

        Args:
            line: The line to write.
        """
        if self._buffer is not None:
            self._buffer += line
        else:
            self._output.write(line)

    def _process_line(self, line):
        """Process a line.

        Args:
            line: The line to process.
        """
        self._write(line)
        magic = self._generate_magic(line)
        if magic:
            if self._buffer is None and not line.endswith('\n'):
                self._write('\n')
            self._output.write(magic)
        self._output.flush()

    def _read_from_input(self, input_stream):
        """Read and process all lines from an input stream.

        Args:
            input_stream: The input stream to read from.
        """
        self._line = ''
        # Iterating directly over input_stream incurs delays in output
        # propagation.
        for line in iter(input_stream.readline, ''):
            self._process_line(line)

    def read_and_output(self, input_stream):
        """Read all lines from an input stream and write to output stream.

        Args:
            input_stream: The input stream to read from.
        """
        self._buffer = None
        self._read_from_input(input_stream)

    def read_and_buffer(self, input_stream):
        """Read all lines from an input stream and write to the buffer.

        Pinball magic properties will be still written to the output rather
        than to the buffer.

        Args:
            input_stream: The input stream to read from.

        Returns:
            The buffer with read lines.
        """
        self._buffer = ''
        self._read_from_input(input_stream)
        return self._buffer

    def process_and_output(self, input_lines):
        """ Reads an iterable of lines and writes to output stream.

        Args:
            input_lines: An iterable of lines
        """
        self._buffer = None
        for line in input_lines:
            self._process_line(line)

    def process_and_buffer(self, input_lines):
        """ Reads an iterable of lines and writes to buffer.

        Pinball magic properties will be still written to the output rather
        than to the buffer.

        Args:
            input_lines: An iterable of lines
        """
        self._buffer = ''
        for line in input_lines:
            self._process_line(line)
        return self._buffer
