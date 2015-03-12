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

"""Parser that reads configs from a repository."""
import collections
import pickle

from pinball.config.pinball_config import PinballConfig
from pinball.master.thrift_lib.ttypes import Token
from pinball.parser.config_parser import ConfigParser
from pinball.parser.utils import recurrence_str_to_sec
from pinball.parser.utils import schedule_to_timestamp
from pinball.repository.github_repository import GithubRepository
from pinball.scheduler.overrun_policy import OverrunPolicy
from pinball.scheduler.schedule import WorkflowSchedule
from pinball.workflow.event import Event
from pinball.workflow.name import Name
from pinball.workflow.utils import get_unique_workflow_instance
from pinball.workflow.utils import load_path


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class RepositoryConfigParser(ConfigParser):
    def __init__(self, params=None):
        self._repository = GithubRepository()

    @staticmethod
    def _job_config_to_job(job_config, outputs):
        """Create a job from a job config.

        Args:
            job_config: The job config.
            outputs: The list of job output names.
        Returns:
            Pinball job constructed from the config.
        """
        # load template
        job_template = load_path(job_config.template)(
            job_config.job,
            max_attempts=job_config.max_attempts,
            emails=job_config.emails,
            priority=job_config.priority)
        return job_template.get_pinball_job(job_config.parents,
                                            outputs,
                                            job_config.template_params)

    @staticmethod
    def _condition_config_to_condition(condition_config, outputs):
        """Create a condition from a condition config.

        Args:
            condition_config: The condition config.
            outputs: The list of condition output names.
        Returns:
            Pinball job constructed from the config.
        """
        # load template
        condition_template = load_path(condition_config.template)(
            condition_config.job,
            max_attempts=condition_config.max_attempts,
            retry_delay_sec=condition_config.retry_delay_sec,
            emails=condition_config.emails,
            priority=condition_config.priority)
        return condition_template.get_pinball_condition(
            outputs, params=condition_config.template_params)

    @staticmethod
    def _job_config_to_token(workflow, instance, job_config, job_outputs):
        """Create a job token from a job config.

        Args:
            workflow: The workflow name.
            instance: The workflow instance.
            job_config: The job config to create token from.
            job_outputs: The names of the job outputs.
        Returns:
            Job token constructed from the job config.
        """
        if job_config.is_condition:
            job = RepositoryConfigParser._condition_config_to_condition(
                job_config, job_outputs)
        else:
            job = RepositoryConfigParser._job_config_to_job(job_config,
                                                            job_outputs)
        name = Name(workflow=workflow, instance=instance,
                    job_state=Name.WAITING_STATE, job=job_config.job)
        job_token = Token(name=name.get_job_token_name(),
                          data=pickle.dumps(job))
        return job_token

    def get_schedule_token(self, workflow):
        schedule_config = self._repository.get_schedule(workflow)
        timestamp = schedule_to_timestamp(schedule_config.time,
                                          schedule_config.start_date)
        recurrence = recurrence_str_to_sec(schedule_config.recurrence)
        overrun_policy = OverrunPolicy.from_string(
            schedule_config.overrun_policy)
        schedule = WorkflowSchedule(
            next_run_time=timestamp,
            recurrence_seconds=recurrence,
            overrun_policy=overrun_policy,
            workflow=schedule_config.workflow,
            emails=schedule_config.emails,
            #TODO(mao): to make it flexible that allow users specify through UI
            max_running_instances=PinballConfig.DEFAULT_MAX_WORKFLOW_RUNNING_INSTANCES)
        schedule.advance_next_run_time()
        timestamp = schedule.next_run_time
        token_name = (
            Name(workflow=schedule_config.workflow
                 ).get_workflow_schedule_token_name())
        return Token(name=token_name, owner='parser',
                     expirationTime=timestamp,
                     data=pickle.dumps(schedule))

    def get_workflow_tokens(self, workflow):
        # TODO(pawel): add workflow connectivity check.
        job_configs = {}
        top_level_job_names = []
        job_names = self._repository.get_job_names(workflow)
        for job_name in job_names:
            job_config = self._repository.get_job(workflow, job_name)
            job_configs[job_name] = job_config
            if not job_config.parents:
                top_level_job_names.append(job_name)
                job_config.parents = [Name.WORKFLOW_START_INPUT]

        job_outputs = collections.defaultdict(list)
        for job_config in job_configs.values():
            for parent_job_name in job_config.parents:
                job_outputs[parent_job_name].append(job_config.job)

        result = []
        instance = get_unique_workflow_instance()

        # Convert job configs to job tokens.
        for job_config in job_configs.values():
            token = RepositoryConfigParser._job_config_to_token(
                workflow,
                instance,
                job_config,
                job_outputs[job_config.job])
            result.append(token)

        # Create triggering events for top-level jobs.
        for job_name in top_level_job_names:
            event = Event(creator='repository_config_parser')
            event_name = Name(workflow=workflow,
                              instance=instance,
                              job=job_name,
                              input_name=Name.WORKFLOW_START_INPUT,
                              event='workflow_start_event')
            result.append(Token(name=event_name.get_event_token_name(),
                                data=pickle.dumps(event)))

        return result

    def get_workflow_names(self):
        return self._repository.get_workflow_names()
