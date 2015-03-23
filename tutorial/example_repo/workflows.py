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
        notify_emails='your@email.com'),

    # Pinball allows an upstream job to pass attributes to downstream dependants.
    # Pinball executor has the ability to interpret specially formatted job output.
    # E.g., printing PINBALL:EVENT_ATTR:akey=avalue to the job output will embed a
    # akey-avalue pair in the job output event. Output events traverse along job
    # output edges and they are accessible by downstream jobs. Event attributes are
    # used to customize the job command line. E.g., command “echo %(akey)s” will be
    # translated to “echo avalue” by the job executor.
    'pass_params_between_jobs': WorkflowConfig(
        jobs={
            'cmd_parent': JobConfig(CommandJobTemplate('ExamplePinballMagicCMD',
                                                       'echo PINBALL:EVENT_ATTR:a_cmd_key=a_cmd_value'), []),
            'python_parent': JobConfig(JobTemplate('ExamplePinballMagicPythonJob'), []),
            'child': JobConfig(CommandJobTemplate('CHILD', 'echo "child: %%(a_cmd_key)s %%(a_python_key)s"'),
                               ['cmd_parent', 'python_parent']),
        },
        final_job_config=JobConfig(FINAL_JOB),
        schedule=ScheduleConfig(recurrence=timedelta(days=1),
                                reference_timestamp=datetime(
                                year=2015, month=1, day=1, second=1)),
        notify_emails='your@email.com'),
}
