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

"""Validation tests for the job executor."""
import mock
import os
import subprocess
import unittest

from pinball.config.pinball_config import PinballConfig
from pinball.workflow.event import Event
from pinball.workflow.job import ShellJob
from pinball.workflow.job_executor import ExecutionRecord
from pinball.workflow.job_executor import ShellJobExecutor


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class ShellJobExecutorTestCase(unittest.TestCase):
    def setUp(self):
        self._data_builder = mock.Mock()
        self._emailer = mock.Mock()
        job = ShellJob(name='some_job',
                       command='printf "line1\\nline2\\nline3";'
                               'printf "line1\\nline2" >&2',
                       emails=['some_email@pinterest.com'],
                       warn_timeout_sec=10,
                       abort_timeout_sec=20)
        self._executor = ShellJobExecutor('some_workflow', '123', 'some_job',
                                          job, self._data_builder,
                                          self._emailer)
        # Set PinballConfig to enable s3 log saver
        PinballConfig.S3_LOGS_DIR_PREFIX = 's3n://pinball/tmp/'
        PinballConfig.S3_LOGS_DIR = \
            PinballConfig.S3_LOGS_DIR_PREFIX \
            + PinballConfig.JOB_LOG_PATH_PREFIX


    @mock.patch('pinball.workflow.log_saver.S3FileLogSaver._get_or_create_s3_key')
    @mock.patch('__builtin__.open')
    def test_token_lost(self, open_mock, get_s3_key_mock):
        file_mock = mock.MagicMock()
        open_mock.return_value = file_mock
        file_mock.__enter__.return_value = file_mock

        s3_key_mock = mock.MagicMock()
        get_s3_key_mock.return_value = s3_key_mock
        s3_key_mock.__enter__.return_value = s3_key_mock

        execution_record = ExecutionRecord(start_time=10)
        self._executor.job.history = [execution_record]

        self.assertFalse(self._executor.prepare())
        file_mock.write.assert_called_once_with('executor failed to renew job '
                                                'ownership on time\n')
        get_s3_key_mock.assert_called_once_with('s3n://pinball/tmp/pinball_job_logs/'
                                                'some_workflow/123/some_job.10.pinlog')

    @mock.patch('pinball.workflow.log_saver.S3FileLogSaver.open')
    @mock.patch('os.path.exists')
    @mock.patch('__builtin__.open')
    def test_events(self, open_mock, exists_mock, s3_open_mock):
        file_mock = mock.MagicMock()
        open_mock.return_value = file_mock
        file_mock.__enter__.return_value = file_mock

        exists_mock.return_value = True
        some_event = Event(creator='some_creator')
        some_other_event = Event(creator='some_other_creator')
        self._executor.job.events = [some_event, some_other_event]
        self.assertTrue(self._executor.prepare())
        self.assertEqual(1, len(self._executor.job.history))
        execution_record = self._executor.job.history[0]
        self.assertEqual([some_event, some_other_event],
                         execution_record.events)
        self.assertEqual(s3_open_mock.call_count, 2)

    def test_disabled(self):
        self._executor.job.disabled = True
        self.assertTrue(self._executor.prepare())
        self.assertEqual(1, len(self._executor.job.history))
        execution_record = self._executor.job.history[0]
        self.assertEqual('DISABLED', execution_record.info)

    @mock.patch('subprocess.Popen')
    def test_execute_cleanup(self, subprocess_mock):
        self._executor.job.cleanup_template = 'cleanup %(kill_id)s'
        execution_record = ExecutionRecord()
        execution_record.properties['kill_id'] = ['123', '456']
        self._executor.job.history = [execution_record]
        self._executor._execute_cleanup()
        env = os.environ.copy()
        env.pop('DJANGO_SETTINGS_MODULE', None)
        subprocess_mock.assert_called_with('cleanup 123,456',
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE,
                                           shell=True,
                                           env=env,
                                           preexec_fn=os.setsid)

    @mock.patch('time.time')
    def test_check_timeout_noop(self, time_mock):
        execution_record = ExecutionRecord(start_time=10)
        self._executor.job.history = [execution_record]

        time_mock.return_value = 15
        self._executor._check_timeouts()
        self.assertEqual(
            0, self._emailer.send_job_timeout_warning_message.call_count)

        time_mock.return_value = 25
        self._data_builder.get_schedule.return_value = None
        job_execution_data = mock.Mock()
        self._data_builder.get_execution.return_value = job_execution_data
        self._executor._check_timeouts()
        self._data_builder.get_schedule.assert_called_once_with(
            'some_workflow')
        self._data_builder.get_execution.assert_called_once_with(
            'some_workflow', '123', 'some_job', 0)
        self._emailer.send_job_timeout_warning_message.assert_called_once_with(
            ['some_email@pinterest.com'], job_execution_data)

        time_mock.return_value = 35
        self._executor._check_timeouts()
        self.assertTrue(self._executor._aborted)

    @mock.patch('pinball.workflow.log_saver.S3FileLogSaver._get_or_create_s3_key')
    @mock.patch('os.path.exists')
    @mock.patch('__builtin__.open')
    def test_execute(self, open_mock, exists_mock, get_s3_key_mock):
        file_mock = mock.MagicMock()
        open_mock.return_value = file_mock
        file_mock.__enter__.return_value = file_mock

        s3_key_mock = mock.MagicMock()
        get_s3_key_mock.return_value = s3_key_mock
        s3_key_mock.__enter__.return_value = s3_key_mock

        self.assertTrue(self._executor.prepare())
        self.assertTrue(self._executor.execute())

        file_mock.write.assert_has_calls(
            [mock.call('line1\n'), mock.call('line2\n'), mock.call('line3'),
             mock.call('line1\n'), mock.call('line2')],
            any_order=True)
        self.assertEqual(file_mock.write.call_count, 5)

        self.assertEqual(1, len(self._executor.job.history))
        execution_record = self._executor.job.history[0]
        self.assertEqual(0, execution_record.exit_code)

        self.assertEqual(2, get_s3_key_mock.call_count)

    @mock.patch('pinball.workflow.log_saver.S3FileLogSaver._get_or_create_s3_key')
    @mock.patch('os.makedirs')
    @mock.patch('__builtin__.open')
    def test_abort(self, open_mock, _, get_s3_key_mock):
        file_mock = mock.MagicMock()
        open_mock.return_value = file_mock
        file_mock.__enter__.return_value = file_mock

        s3_key_mock = mock.MagicMock()
        get_s3_key_mock.return_value = s3_key_mock
        s3_key_mock.__enter__.return_value = s3_key_mock

        self.assertTrue(self._executor.prepare())
        self._executor.abort()
        self.assertFalse(self._executor.execute())

        self.assertEqual(1, len(self._executor.job.history))
        execution_record = self._executor.job.history[0]
        self.assertEqual(1, execution_record.exit_code)

    @mock.patch('pinball.workflow.log_saver.S3FileLogSaver._get_or_create_s3_key')
    @mock.patch('os.path.exists')
    @mock.patch('__builtin__.open')
    def test_execute_long_line(self, open_mock, exists_mock, get_s3_key_mock):
        file_mock = mock.MagicMock()
        open_mock.return_value = file_mock
        file_mock.__enter__.return_value = file_mock

        s3_key_mock = mock.MagicMock()
        get_s3_key_mock.return_value = s3_key_mock
        s3_key_mock.__enter__.return_value = s3_key_mock

        job = ShellJob(name='some_job',
                       command="printf \"%s\"" % ('a' * 20000),
                       emails=['some_email@pinterest.com'],
                       warn_timeout_sec=10,
                       abort_timeout_sec=20)
        executor = ShellJobExecutor('some_workflow', '123', 'some_job',
                                    job, self._data_builder,
                                    self._emailer)

        self.assertTrue(executor.prepare())
        self.assertTrue(executor.execute())

        file_mock.write.assert_has_calls(
            [mock.call('a' * 16384), mock.call('a' * 3616)])

        self.assertEqual(1, len(executor.job.history))
        execution_record = executor.job.history[0]
        self.assertEqual(0, execution_record.exit_code)

        self.assertEqual(2, get_s3_key_mock.call_count)

    def test_process_log_line(self):
        job = ShellJob(name='some_job',
                       command="echo ok",
                       emails=['some_email@pinterest.com'],
                       warn_timeout_sec=10,
                       abort_timeout_sec=20)
        executor = ShellJobExecutor('some_workflow', '123', 'some_job', job,
                                    self._data_builder,
                                    self._emailer)
        import time
        execution_record = ExecutionRecord(instance=123456,
                                           start_time=time.time())
        executor.job.history.append(execution_record)

        executor._process_log_line("PINBALL:kv_job_url=j_id1|j_url1\n")
        executor._process_log_line("PINBALL:kv_job_url=j_id2|j_url2\n")
        executor._process_log_line("PINBALL:kv_job_url=j_id2|j_url2\n")
        executor._process_log_line("PINBALL:kill_id=qubole1/123\n")
        executor._process_log_line("PINBALL:kill_id=qubole2/456\n")
        executor._process_log_line("PINBALL:kill_id=qubole1/123\n")

        erp = executor._get_last_execution_record().properties
        self.assertEqual(len(erp), 2)

        self.assertIn('kv_job_url', erp.keys())
        self.assertEqual(type(erp['kv_job_url']), list)
        self.assertEqual(len(erp['kv_job_url']), 2)
        self.assertEqual(erp['kv_job_url'], ['j_id1|j_url1', 'j_id2|j_url2'])

        self.assertIn('kill_id', erp.keys())
        self.assertEqual(type(erp['kill_id']), list)
        self.assertEqual(len(erp['kill_id']), 2)
        self.assertEqual(erp['kill_id'], ['qubole1/123', 'qubole2/456'])
