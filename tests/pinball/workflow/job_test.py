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

"""Validation tests for the job."""
import unittest

from pinball.workflow.event import Event
from pinball.workflow.job import ShellJob
from pinball.workflow.job_executor import ExecutionRecord


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class JobTestCase(unittest.TestCase):
    def test_retry(self):
        job = ShellJob(name='some_job')

        # Empty history.
        self.assertFalse(job.retry())

        # History with a successful execution.
        record = ExecutionRecord(instance=123, exit_code=0)
        job.history.append(record)
        self.assertRaises(AssertionError, job.retry)

        # History with too many failures.
        record = ExecutionRecord(instance=1234, exit_code=1)
        job.history.append(record)
        self.assertFalse(job.retry())

        # History without too many failures.
        job.max_attempts = 2
        self.assertTrue(job.retry())

        # History with too many failures in a different instance.
        job.history.append(record)
        record = ExecutionRecord(instance=12345, exit_code=1)
        job.history.append(record)
        self.assertTrue(job.retry())


class ShellJobTestCase(unittest.TestCase):
    def test_customize_command(self):
        job = ShellJob(name='some_job',
                       inputs=['some_input', 'some_other_input'])
        some_event = Event(attributes={'some_attr': 'some_value'})
        some_other_event = Event(attributes={
            'some_attr': 'some_other_value',
            'yet_another_attr': 'yet_another_value'})
        execution_record = ExecutionRecord(instance=123, start_time=10)
        execution_record.events = [some_event, some_other_event]
        job.history = [execution_record]

        # Empty command.
        job.command = ''
        self.assertEqual('', job.customize_command())

        # Command with no attributes.
        job.command = 'some_command'
        self.assertEqual('some_command', job.customize_command())

        # Command with attributes.
        job.command = ('%(non_existent_attr)s %(some_attr)s '
                       '%(yet_another_attr)s')
        self.assertEqual(' some_value,some_other_value yet_another_value',
                         job.customize_command())

        # Command with percentage marks.
        job.command = ('%% some_command')
        self.assertEqual('% some_command', job.customize_command())
