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

import unittest

from pinball_ext.job_templates import CommandConditionTemplate
from pinball_ext.job_templates import CommandJobTemplate
from pinball_ext.job_templates import JobTemplate


class JobTemplateTestCase(unittest.TestCase):
    def test_job_template(self):
        job_template = JobTemplate(
            'MyJobClassName',
            'qubole',
            {
                'USER_APP_JAR': '/my/app/jar.jar',
            })
        pb_job = job_template.get_pinball_job(
            [],
            [],
            {
                'end_date': '2015-02-25',
                'job_repo_dir': '/mnt/job_repo/',
                'job_import_dirs_config': 'pinball_ext.example.JOB_IMPORT_DIRS',
            })
        self.assertEqual(
            pb_job.command,
            'cd /mnt/job_repo/ && '
            'python -m pinball_ext.job.job_runner '
            '--job_import_dirs_config=pinball_ext.example.JOB_IMPORT_DIRS '
            '--job_class_name=MyJobClassName '
            '--job_params="end_date=2015-02-25" '
            '--executor=qubole '
            '--executor_config="USER_APP_JAR=/my/app/jar.jar" ')


class CommandJobTemplatesTestCase(unittest.TestCase):
    def test_command_job_template(self):
        """Test the config produced by a command job template."""
        job_template = CommandJobTemplate('some_job', 'some_command')
        pb_job = job_template.get_pinball_job([], [], params={
           'end_date': '2015-02-25',
        })
        self.assertEqual('some_command', pb_job.command)


class ConditionTemplatesTestCase(unittest.TestCase):
    def test_command_condition_template(self):
        """Test the config produced by a command condition template."""
        condition = CommandConditionTemplate('some_condition')
        condition_params = {'command': 'some_command'}
        condition_job = condition.get_pinball_condition(None, condition_params)
        self.assertEqual('some_command', condition_job.command)
