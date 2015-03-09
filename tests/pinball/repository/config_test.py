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

"""Validation tests for configurations."""
import unittest

from pinball.config.utils import PinballException
from pinball.repository.config import JobConfig
from pinball.repository.config import WorkflowScheduleConfig


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class WorkflowScheduleConfigTestCase(unittest.TestCase):
    _SCHEDULE_JSON = """{
    "emails": [
        "some_email@pinterest.com",
        "some_other_email@pinterest.com"
    ],
    "overrun_policy": "DELAY",
    "recurrence": "1d",
    "start_date": "2012-01-01",
    "time": "00.00.01.000",
    "workflow": "some_workflow"
}"""

    def test_from_json(self):
        schedule_config = WorkflowScheduleConfig.from_json(
            WorkflowScheduleConfigTestCase._SCHEDULE_JSON)
        self.assertEqual(['some_email@pinterest.com',
                          'some_other_email@pinterest.com'],
                         schedule_config.emails)
        self.assertEqual('DELAY', schedule_config.overrun_policy)
        self.assertEqual('1d', schedule_config.recurrence)
        self.assertEqual('2012-01-01', schedule_config.start_date)
        self.assertEqual('00.00.01.000', schedule_config.time)
        self.assertEqual('some_workflow', schedule_config.workflow)

    def test_format(self):
        schedule_config = WorkflowScheduleConfig.from_json(
            WorkflowScheduleConfigTestCase._SCHEDULE_JSON)
        formatted_config = schedule_config.format()
        self.assertEqual(['some_email@pinterest.com',
                          'some_other_email@pinterest.com'],
                         formatted_config['emails'])
        self.assertEqual('DELAY', formatted_config['overrun_policy'])
        self.assertEqual('1d', formatted_config['recurrence'])
        self.assertEqual('2012-01-01', formatted_config['start_date'])
        self.assertEqual('00.00.01.000', formatted_config['time'])
        self.assertEqual('some_workflow', formatted_config['workflow'])


class JobConfigTestCase(unittest.TestCase):
    _REQUIRED_JOB_JSON = """
    "emails": [
        "some_email@pinterest.com",
        "some_other_email@pinterest.com"
    ],
    "is_condition": false,
    "job": "some_job",
    "max_attempts": 10,
    "parents": [
        "some_parent_job",
        "some_other_parent_job"
    ],
    "priority": 123,
    "retry_delay_sec": 30,
    "template": "some_template",
    "template_params": {"some_param": "some_value"},
    "workflow": "some_workflow"
"""

    _OPTIONAL_JOB_JSON = """
    "warn_timeout_sec": 10,
    "abort_timeout_sec": 20
"""

    def test_from_json(self):
        job_json = '{%s, %s}' % (JobConfigTestCase._REQUIRED_JOB_JSON,
                                 JobConfigTestCase._OPTIONAL_JOB_JSON)
        job_config = JobConfig.from_json(job_json)
        self.assertFalse(job_config.is_condition)
        self.assertEqual(['some_email@pinterest.com',
                          'some_other_email@pinterest.com'],
                         job_config.emails)
        self.assertEqual('some_job', job_config.job)
        self.assertEqual(10, job_config.max_attempts)
        self.assertEqual(30, job_config.retry_delay_sec)
        self.assertEqual(['some_parent_job', 'some_other_parent_job'],
                         job_config.parents)
        self.assertEqual(123, job_config.priority)
        self.assertEqual('some_template', job_config.template)
        template_params = {'some_param': 'some_value'}
        self.assertEqual(template_params, job_config.template_params)
        self.assertEqual('some_workflow', job_config.workflow)
        self.assertEqual(10, job_config.warn_timeout_sec)
        self.assertEqual(20, job_config.abort_timeout_sec)

    def test_required_from_json(self):
        job_json = '{%s}' % JobConfigTestCase._REQUIRED_JOB_JSON
        job_config = JobConfig.from_json(job_json)
        self.assertEqual('some_workflow', job_config.workflow)
        self.assertIsNone(job_config.warn_timeout_sec)
        self.assertIsNone(job_config.abort_timeout_sec)

    def test_fail_from_json(self):
        job_json = '{%s, "unknown": "unknown"}' % (
            JobConfigTestCase._REQUIRED_JOB_JSON)
        self.assertRaises(PinballException, JobConfig.from_json, job_json)

    def test_format(self):
        job_json = '{%s, %s}' % (JobConfigTestCase._REQUIRED_JOB_JSON,
                                 JobConfigTestCase._OPTIONAL_JOB_JSON)
        job_config = JobConfig.from_json(job_json)
        formatted_config = job_config.format()
        self.assertEqual(['some_email@pinterest.com',
                          'some_other_email@pinterest.com'],
                         formatted_config['emails'])
        self.assertEqual('some_job', formatted_config['job'])
        self.assertEqual(10, formatted_config['max_attempts'])
        self.assertEqual(30, formatted_config['retry_delay_sec'])
        self.assertEqual(['some_parent_job', 'some_other_parent_job'],
                         formatted_config['parents'])
        self.assertEqual(123, formatted_config['priority'])
        self.assertEqual('some_template', formatted_config['template'])
        self.assertEqual({'some_param': 'some_value'},
                         formatted_config['template_params'])
        self.assertEqual('some_workflow', formatted_config['workflow'])
