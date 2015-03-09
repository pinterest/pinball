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

import mock
import unittest

from pinball.workflow.emailer import Emailer
from pinball.config.pinball_config import PinballConfig
from pinball.ui.data import JobData
from pinball.ui.data import JobExecutionData
from pinball.ui.data import Status
from pinball.ui.data import WorkflowInstanceData


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class EmailerTestCase(unittest.TestCase):
    @mock.patch('pinball.workflow.emailer.smtplib')
    def test_send_instance_end_message(self, smtplib_mock):
        emailer = Emailer('some_host', '8080')
        instance_data = WorkflowInstanceData(workflow='some_workflow',
                                             instance='123',
                                             status=Status.FAILURE,
                                             start_time=10,
                                             end_time=100)
        parent_job_data = JobData('some_workflow',
                                  '123',
                                  'parent',
                                  'ShellJob',
                                  False,
                                  'some_command',
                                  ['workflow_input'],
                                  ['parent_output'],
                                  ['some_email@pinterest.com'],
                                  1,
                                  2,
                                  100,
                                  200,
                                  1,
                                  Status.SUCCESS,
                                  10,
                                  50)

        child_job_data = JobData('some_workflow',
                                 '123',
                                 'child',
                                 'ShellJob',
                                 False,
                                 'some_command',
                                 ['parent_output'],
                                 [],
                                 ['some_email@pinterest.com'],
                                 1,
                                 2,
                                 100,
                                 200,
                                 2,
                                 Status.FAILURE,
                                 50,
                                 100)

        smtp = mock.Mock()
        smtplib_mock.SMTP.return_value = smtp

        sendmail = mock.Mock()
        smtp.sendmail = sendmail

        emailer.send_instance_end_message(['some_email@pinterest.com',
                                           'some_other_email@pinterest.com'],
                                          instance_data, [child_job_data,
                                                          parent_job_data])

        self.assertEqual(PinballConfig.DEFAULT_EMAIL, sendmail.call_args[0][0])
        self.assertEqual(
            ['some_email@pinterest.com', 'some_other_email@pinterest.com'],
            sendmail.call_args[0][1])
        msg = sendmail.call_args[0][2]
        self.assertTrue('FAILURE for workflow some_workflow' in msg)
        self.assertTrue('123' in msg)
        self.assertTrue('parent' in msg)
        self.assertTrue('child' in msg)
        self.assertTrue('SUCCESS' in msg)

        # Make sure that jobs are ordered on the end time.
        parent_index = msg.find('parent')
        child_index = msg.find('child')
        self.assertLess(parent_index, child_index)

    @mock.patch('pinball.workflow.emailer.smtplib')
    def test_send_job_execution_end_message(self, smtplib_mock):
        emailer = Emailer('some_host', '8080')
        execution_data = JobExecutionData(workflow='some_workflow',
                                          instance='123',
                                          job='some_job',
                                          execution=1,
                                          info='some_info',
                                          exit_code=1,
                                          start_time=10,
                                          end_time=100,
                                          logs={'stdout': '/some/out',
                                                'stderr': '/some/error'})

        smtp = mock.Mock()
        smtplib_mock.SMTP.return_value = smtp

        sendmail = mock.Mock()
        smtp.sendmail = sendmail

        emailer.send_job_execution_end_message(
            ['some_email@pinterest.com', 'some_other_email@pinterest.com'],
            execution_data)

        self.assertEqual(PinballConfig.DEFAULT_EMAIL, sendmail.call_args[0][0])
        self.assertEqual(
            ['some_email@pinterest.com', 'some_other_email@pinterest.com'],
            sendmail.call_args[0][1])
        msg = sendmail.call_args[0][2]
        self.assertTrue('Workflow some_workflow\'s job some_job finished with '
                        'exit code 1' in msg)
        self.assertTrue('123' in msg)
        self.assertTrue('some_job' in msg)
        self.assertTrue('stdout' in msg)
        self.assertTrue('stderr' in msg)

    @mock.patch('pinball.workflow.emailer.smtplib')
    def test_send_job_timeout_warning_message(self, smtplib_mock):
        emailer = Emailer('some_host', '8080')
        execution_data = JobExecutionData(workflow='some_workflow',
                                          instance='123',
                                          job='some_job',
                                          execution=1,
                                          info='some_info',
                                          start_time=10,
                                          logs={'stdout': '/some/out',
                                                'stderr': '/some/error'})

        smtp = mock.Mock()
        smtplib_mock.SMTP.return_value = smtp

        sendmail = mock.Mock()
        smtp.sendmail = sendmail

        emailer.send_job_timeout_warning_message(
            ['some_email@pinterest.com', 'some_other_email@pinterest.com'],
            execution_data)

        self.assertEqual(PinballConfig.DEFAULT_EMAIL, sendmail.call_args[0][0])
        self.assertEqual(
            ['some_email@pinterest.com', 'some_other_email@pinterest.com'],
            sendmail.call_args[0][1])
        msg = sendmail.call_args[0][2]
        self.assertTrue('Workflow some_workflow\'s job some_job exceeded '
                        'timeout' in msg)
        self.assertTrue('123' in msg)
        self.assertTrue('some_job' in msg)
        self.assertTrue('stdout' in msg)
        self.assertTrue('stderr' in msg)

    @mock.patch('pinball.workflow.emailer.smtplib')
    def test_send_too_many_running_instances_warning_message(self, smtplib_mock):
        emailer = Emailer('some_host', '8080')
        workflow = "some_workflow"
        number_running_instances = 3
        max_running_instances = 3

        smtp = mock.Mock()
        smtplib_mock.SMTP.return_value = smtp

        sendmail = mock.Mock()
        smtp.sendmail = sendmail

        emailer.send_too_many_running_instances_warning_message(
            ['some_email@pinterest.com', 'some_other_email@pinterest.com'],
            workflow,
            number_running_instances,
            max_running_instances)

        self.assertEqual(PinballConfig.DEFAULT_EMAIL, sendmail.call_args[0][0])
        self.assertEqual(
            ['some_email@pinterest.com', 'some_other_email@pinterest.com'],
            sendmail.call_args[0][1])
        msg = sendmail.call_args[0][2]
        self.assertTrue('3' in msg)
        self.assertTrue('some_workflow' in msg)
