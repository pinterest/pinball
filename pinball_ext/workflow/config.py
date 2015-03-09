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

"""Defines configurations for a workflow, class and schedule.
"""


__author__ = 'Mao Ye, Jooseong Kim'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class WorkflowConfig(object):
    """Configures a workflow.

    This class configures a workflow. This configuration includes jobs that
    belong to the workflow, the schedule the workflow runs on and the email
    addresses any notification needs to be sent to.


    Attributes:
        jobs: Dict of job name to JobConfig objects.
            Each (job name, job config) in the dict expresses one job that
            belongs to this workflow.
        final_job_config: A config for a job that will be executed as the final
            stage of the workflow. This job is executed once all other jobs of
            the workflow runs to completion successfully. This is configured by
            an instance of JobConfig.
        schedule: The schedule on which this workflow will run. This is an
            instance of ScheduleConfig.
        notify_emails: A comma separated string of email addresses. Any
            notification will be sent to these email addressses.

    """
    def __init__(self, jobs, final_job_config, schedule, notify_emails=None):
        self.jobs = jobs
        self.final_job_config = final_job_config
        self.schedule = schedule
        self.notify_emails = notify_emails


class JobConfig(object):
    """Configures a job.

    This class configures a job. It includes the job template and the name of
    other jobs that needs to successfully complete before this job starts.

    Attributes:
        template: The job template for this job. A job template defines what
            the job does.
        dependency: List of names of jobs that needs to precede this job. Only
            jobs from the same workflow can be depended on.
    """
    def __init__(self, template, dependency=None):
        self.template = template
        self.dependency = dependency


class ScheduleConfig(object):
    """Configures a schedule.

    This class configures the schedule of running this workflow.

    Attributes:
        recurrence: Recurrence or interval of running a workflow. This is
            configured by an instance of datetime.timedelta.
        reference_timestamp: The timestamp that becomes the basis of scheduling
            future jobs. Future jobs will be run periodically as specified by
            recurrence since this reference point.
        overrun_policy: Policy governing what to do if the previous workflow
            is still running. Refer to class OverrunPolicy for possible values.
        max_running_instances: Maximum number of workflow instances that can
            run at the same time.
    """
    def __init__(self, recurrence, reference_timestamp, overrun_policy=None,
                 max_running_instances=None):
        self.recurrence = recurrence
        self.reference_timestamp = reference_timestamp
        self.overrun_policy = overrun_policy
        self.max_running_instances = max_running_instances
