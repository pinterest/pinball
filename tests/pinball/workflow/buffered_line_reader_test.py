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

"""Validation tests for buffered line reader"""

import unittest
import mock

from pinball.workflow import buffered_line_reader


__author__ = 'Jooseong Kim'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class BufferedLineReaderTestCase(unittest.TestCase):
    def setUp(self):
        file_mock = mock.Mock()
        self._file_mock = file_mock
        self._buffered_line_reader = \
            buffered_line_reader.BufferedLineReader(file_mock)

    @mock.patch('os.read')
    def test_one_line(self, os_read_mock):
        os_read_mock.return_value = 'one line'
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, [])
        self.assertFalse(self._buffered_line_reader.eof())

        os_read_mock.return_value = ''
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, ['one line'])
        self.assertTrue(self._buffered_line_reader.eof())

    @mock.patch('os.read')
    def test_one_line_continued(self, os_read_mock):
        os_read_mock.return_value = 'one line '
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, [])
        self.assertFalse(self._buffered_line_reader.eof())

        os_read_mock.return_value = 'continued'
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, [])
        self.assertFalse(self._buffered_line_reader.eof())

        os_read_mock.return_value = ''
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, ['one line continued'])
        self.assertTrue(self._buffered_line_reader.eof())

    @mock.patch('os.read')
    def test_one_line_terminated(self, os_read_mock):
        os_read_mock.return_value = 'one line terminated\n'
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, ['one line terminated\n'])
        self.assertFalse(self._buffered_line_reader.eof())

        os_read_mock.return_value = ''
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, [])
        self.assertTrue(self._buffered_line_reader.eof())

    @mock.patch('os.read')
    def test_one_line_continued_terminated(self, os_read_mock):
        os_read_mock.return_value = 'one line '
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, [])
        self.assertFalse(self._buffered_line_reader.eof())

        os_read_mock.return_value = 'continued\n'
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, ['one line continued\n'])
        self.assertFalse(self._buffered_line_reader.eof())

        os_read_mock.return_value = ''
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, [])
        self.assertTrue(self._buffered_line_reader.eof())

    @mock.patch('os.read')
    def test_multi_line(self, os_read_mock):
        os_read_mock.return_value = 'first line\nsecond line\nthird line'
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, ['first line\n', 'second line\n'])
        self.assertFalse(self._buffered_line_reader.eof())

        os_read_mock.return_value = ''
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, ['third line'])
        self.assertTrue(self._buffered_line_reader.eof())

    @mock.patch('os.read')
    def test_multi_line_continued_terminated(self, os_read_mock):
        os_read_mock.return_value = 'first '
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, [])
        self.assertFalse(self._buffered_line_reader.eof())

        os_read_mock.return_value = 'line\nsecond '
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, ['first line\n'])
        self.assertFalse(self._buffered_line_reader.eof())

        os_read_mock.return_value = 'line\nthird line\n'
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, ['second line\n', 'third line\n'])
        self.assertFalse(self._buffered_line_reader.eof())

        os_read_mock.return_value = ''
        lines = self._buffered_line_reader.readlines()
        self.assertEqual(lines, [])
        self.assertTrue(self._buffered_line_reader.eof())

    @mock.patch('os.read')
    def test_long_line(self, os_read_mock):
        """Tests if BufferedLineReader respects max_buffer_size"""
        a1009 = 'a' * 1009

        os_read_mock.return_value = a1009
        for i in range(15):
            lines = self._buffered_line_reader.readlines()
            os_read_mock.assert_called_with(self._file_mock.fileno(), 2048)
            self.assertEqual(lines, [])
            self.assertFalse(self._buffered_line_reader.eof())

        lines = self._buffered_line_reader.readlines()
        os_read_mock.assert_called_with(self._file_mock.fileno(), 1249)
        self.assertEqual(lines, [])
        self.assertFalse(self._buffered_line_reader.eof())

        os_read_mock.return_value = 'a' * 240
        lines = self._buffered_line_reader.readlines()
        os_read_mock.assert_called_with(self._file_mock.fileno(), 240)
        self.assertEqual(lines, ['a' * (1 << 14)])
        self.assertFalse(self._buffered_line_reader.eof())

        os_read_mock.return_value = a1009 + '\n'
        lines = self._buffered_line_reader.readlines()
        os_read_mock.assert_called_with(self._file_mock.fileno(), 2048)
        self.assertEqual(lines, [a1009 + '\n'])
        self.assertFalse(self._buffered_line_reader.eof())
