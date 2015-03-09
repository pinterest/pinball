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

"""Validation tests for configuration repository."""
import unittest

from pinball.repository.config import JobConfig
from pinball.repository.config import WorkflowScheduleConfig
from pinball.repository.repository import Repository


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


_SCHEDULE_TEMPLATE = """{
    "emails": [
        "some_email@pinterest.com",
        "some_other_email@pinterest.com"
    ],
    "overrun_policy": "DELAY",
    "recurrence": "%s",
    "start_date": "2012-01-01",
    "time": "00.00.01.000",
    "workflow": "some_workflow"
}"""

_JOB_TEMPLATE = """{
    "abort_timeout_sec": 20,
    "emails": [
        "some_email@pinterest.com",
        "some_other_email@pinterest.com"
    ],
    "is_condition": false,
    "job": "some_job",
    "max_attempts": %d,
    "parents": [
        "some_parent_job",
        "some_other_parent_job"
    ],
    "priority": 123,
    "retry_delay_sec": 10,
    "template": "some_template",
    "template_params": {
        "some_param": "some_value"
    },
    "warn_timeout_sec": 10,
    "workflow": "some_workflow"
}"""


class FakeRepository(Repository):
    def __init__(self):
        self.configs = {}

    def _get_config(self, path):
        if path == '/workflow/some_workflow/schedule':
            return _SCHEDULE_TEMPLATE % "1d"
        elif path == '/workflow/some_workflow/job/some_job':
            return _JOB_TEMPLATE % 10
        assert False, 'unrecognized path %s' % path

    def _put_config(self, path, content):
        self.configs[path] = content

    def _delete_config(self, path):
        del self.configs[path]

    def _list_directory(self, directory, allow_not_found):
        if directory == '/workflow/':
            return ['some_workflow/', 'some_other_workflow/']
        elif directory == '/workflow/some_other_workflow/':
            return ['job/']
        elif directory == '/workflow/some_workflow/':
            return ['job/']
        elif directory == '/workflow/some_workflow/job/':
            return ['some_job']
        elif directory == '/workflow/some_other_workflow/job/':
            return ['some_other_job', 'yet_another_job']
        assert False, 'unrecognized directory %s' % directory


class RepositoryTestCase(unittest.TestCase):
    def setUp(self):
        self._repository = FakeRepository()

    def test_get_schedule(self):
        schedule_config = self._repository.get_schedule('some_workflow')
        self.assertEqual('some_workflow', schedule_config.workflow)
        self.assertEqual('1d', schedule_config.recurrence)

    def test_put_schedule(self):
        schedule_config = WorkflowScheduleConfig.from_json(
            _SCHEDULE_TEMPLATE % '1w')
        self._repository.put_schedule(schedule_config)
        self.assertEqual(1, len(self._repository.configs))
        self.assertEqual(
            _SCHEDULE_TEMPLATE % '1w',
            self._repository.configs['/workflow/some_workflow/schedule'])

    def test_delete_schedule(self):
        schedule_config = WorkflowScheduleConfig.from_json(
            _SCHEDULE_TEMPLATE % 100)
        self._repository.put_schedule(schedule_config)
        self._repository.delete_schedule('some_workflow')
        self.assertEqual({}, self._repository.configs)

    def test_get_job(self):
        job_config = self._repository.get_job('some_workflow', 'some_job')
        self.assertEqual('some_job', job_config.job)
        self.assertEqual('some_workflow', job_config.workflow)
        self.assertEqual(10, job_config.max_attempts)

    def test_put_job(self):
        job_config = JobConfig.from_json(_JOB_TEMPLATE % 100)
        self._repository.put_job(job_config)
        self.assertEqual(1, len(self._repository.configs))
        self.assertEqual(
            _JOB_TEMPLATE % 100,
            self._repository.configs['/workflow/some_workflow/job/some_job'])

    def test_delete_job(self):
        job_config = JobConfig.from_json(_JOB_TEMPLATE % 100)
        self._repository.put_job(job_config)
        self._repository.delete_job('some_workflow', 'some_job')
        self.assertEqual({}, self._repository.configs)

    def test_get_workflow_names(self):
        self.assertEqual(['some_workflow', 'some_other_workflow'],
                         self._repository.get_workflow_names())

    def test_get_job_names(self):
        self.assertEqual(['some_job'],
                         self._repository.get_job_names('some_workflow'))
        self.assertEqual(['some_other_job', 'yet_another_job'],
                         self._repository.get_job_names('some_other_workflow'))
