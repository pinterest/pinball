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

"""Validation tests for parser extracting configs from a repository."""
import mock
import pickle
import unittest

from pinball.master.thrift_lib.ttypes import Token
from pinball.parser.repository_config_parser import \
    RepositoryConfigParser
from pinball.repository.config import JobConfig
from pinball.repository.config import WorkflowScheduleConfig
from pinball.scheduler.overrun_policy import OverrunPolicy
from pinball.workflow.name import Name


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class SomeJobTemplate(object):
    def __init__(self, name, max_attempts=None, emails=None, priority=None):
        assert name == 'some_job'
        assert max_attempts == 10
        assert emails == ['some_email@pinterest.com',
                          'some_other_email@pinterest.com']
        assert priority == 100

    def get_pinball_job(self, inputs, outputs, params=None):
        assert inputs == ['__WORKFLOW_START__']
        assert outputs == []
        assert params == {'some_param': 'some_value'}

        return Token(name='/workflow/some_workflow/job/waiting/some_job')


class RepositoryConfigParserTestCase(unittest.TestCase):
    def setUp(self):
        self._call_count = 0

    @mock.patch('time.time')
    @mock.patch('pinball.parser.repository_config_parser.'
                'GithubRepository')
    def test_get_schedule_token(self, repository_mock, time_mock):
        repository = mock.Mock()
        repository_mock.return_value = repository

        schedule_config = WorkflowScheduleConfig()
        schedule_config.workflow = 'some_workflow'
        schedule_config.start_date = '2012-01-01'
        schedule_config.time = '00.00.01.000'
        schedule_config.recurrence = '1d'
        schedule_config.overrun_policy = 'START_NEW'
        schedule_config.emails = ['some_email@pinterest.com',
                                  'some_other_email@pinterest.com']
        repository.get_schedule.return_value = schedule_config

        time_mock.return_value = 100.  # the value must be a float

        parser = RepositoryConfigParser()

        schedule_token = parser.get_schedule_token('some_workflow')

        self.assertEqual('/schedule/workflow/some_workflow',
                         schedule_token.name)
        # 1325376000 = 01 Jan 2012 00:00:00 UTC
        self.assertEqual(1325376000, schedule_token.expirationTime)
        schedule = pickle.loads(schedule_token.data)
        self.assertEqual(1325376000, schedule.next_run_time)
        self.assertEqual(24 * 60 * 60, schedule.recurrence_seconds)
        self.assertEqual(OverrunPolicy.START_NEW, schedule.overrun_policy)
        self.assertEqual('some_workflow', schedule.workflow)
        self.assertEqual(['some_email@pinterest.com',
                          'some_other_email@pinterest.com'], schedule.emails)

        repository.get_schedule.assert_called_once_with('some_workflow')

    @mock.patch('pinball.parser.repository_config_parser.'
                'GithubRepository')
    def test_get_workflow_tokens(self, repository_mock):
        repository = mock.Mock()
        repository_mock.return_value = repository

        repository.get_job_names.return_value = ['some_job']

        job_config = JobConfig()
        job_config.workflow = 'some_workflow'
        job_config.job = 'some_job'
        job_config.is_condition = False
        job_config.template = ('tests.pinball.parser.'
                               'repository_config_parser_test.SomeJobTemplate')
        job_config.template_params = {'some_param': 'some_value'}
        job_config.parents = []
        job_config.emails = ['some_email@pinterest.com',
                             'some_other_email@pinterest.com']
        job_config.max_attempts = 10
        job_config.retry_delay_sec = 20
        job_config.priority = 100
        repository.get_job.return_value = job_config

        parser = RepositoryConfigParser()

        workflow_tokens = parser.get_workflow_tokens('some_workflow')

        self.assertEqual(2, len(workflow_tokens))

        # Verify the triggering event token.
        if Name.from_event_token_name(workflow_tokens[0].name).workflow:
            event_token = workflow_tokens[0]
        else:
            event_token = workflow_tokens[1]

        event_name = Name.from_event_token_name(event_token.name)
        self.assertEqual('some_workflow', event_name.workflow)
        self.assertEqual('some_job', event_name.job)
        self.assertEqual('__WORKFLOW_START__', event_name.input)

        event = pickle.loads(event_token.data)
        self.assertEqual('repository_config_parser', event.creator)

        repository.get_job_names.assert_called_once_with('some_workflow')
        repository.get_job.assert_called_once_with('some_workflow', 'some_job')
