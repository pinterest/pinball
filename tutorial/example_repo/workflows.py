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

from datetime import datetime
from datetime import timedelta

from pinball_ext.workflow.config import JobConfig
from pinball_ext.workflow.config import WorkflowConfig
from pinball_ext.workflow.config import ScheduleConfig
from pinball_ext.job_templates import JobTemplate
from pinball_ext.job_templates import CommandJobTemplate
from tutorial.example_repo.job_templates import ExampleEMRJobTemplate
from tutorial.example_repo.job_templates import ExampleQuboleJobTemplate


# A template for a placeholder final job to add to the end of
# workflows.
FINAL_JOB = CommandJobTemplate('final', 'echo success')

WORKFLOWS = {
    'tutorial_workflow': WorkflowConfig(
        jobs={
            'example_python_job':
            JobConfig(JobTemplate('ExamplePythonJob'), []),
            'example_command_job':
            JobConfig(JobTemplate('ExampleCommandJob'), ['example_python_job']),
            'example_quoble_hive_job':
            JobConfig(ExampleQuboleJobTemplate('ShowTableHiveJob'), ['example_command_job']),
            'example_emr_hive_job':
            JobConfig(ExampleEMRJobTemplate('RandomUsersHiveJob'), ['example_command_job']),
            'example_emr_hadoop_job':
            JobConfig(ExampleEMRJobTemplate('EmrWordCount'), ['example_emr_hive_job']),
        },
        final_job_config=JobConfig(FINAL_JOB),
        schedule=ScheduleConfig(recurrence=timedelta(days=1),
                                reference_timestamp=datetime(
                                year=2015, month=2, day=1, second=1)),
        notify_emails='your@email.com')
}
