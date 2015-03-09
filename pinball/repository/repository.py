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

"""Repository stores configurations."""
import abc
import json

from pinball.config.utils import PinballException
from pinball.repository.config import JobConfig
from pinball.repository.config import WorkflowScheduleConfig
from pinball.repository.path import Path


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class Repository(object):
    """An interface for persistent configuration containers."""
    __metaclass__ = abc.ABCMeta

    @staticmethod
    def _json_pretty_print(obj):
        """Pretty print json representation of an object.

        Args:
            obj: The object to pretty print.
        Return:
            Json representation of the object.
        """
        return json.dumps(obj, sort_keys=True, indent=4,
                          separators=(',', ': '))

    @abc.abstractmethod
    def _get_config(self, path):
        """Retrieve config stored in a given path.

        Args:
            path: The path of the config to retrieve.
        Returns:
            The content of the config.
        """
        return

    @abc.abstractmethod
    def _put_config(self, path, content):
        """Add or replace a config in the repository.

        Args:
            path: The path of the config to update.
            content: The new config content.
        """
        return

    @abc.abstractmethod
    def _delete_config(self, path):
        """Remove a config from the repository.

        Args:
            path: The path of the config to remove.
        """
        return

    @abc.abstractmethod
    def _list_directory(self, directory, allow_not_found):
        """List content of a directory.

        Args:
            directory: The directory to list. It must end with a slash.
            allow_not_found: Indicates if a non-existent directory path should
                trigger an exception.
        Returns:
            List of files in the directory.  Subdirectories end with a slash.
        """
        return

    def get_schedule(self, workflow):
        """Retrieve schedule config for a given workflow.

        Args:
            workflow: The workflow name whose schedule should be retrieved.
        Returns:
            The schedule config.
        """
        path = Path(workflow=workflow)
        schedule_json = self._get_config(path.get_schedule_path())
        return WorkflowScheduleConfig.from_json(schedule_json)

    def put_schedule(self, schedule_config):
        """Add or replace a workflow schedule.

        Args:
            schedule_config: The schedule config to add or replace.
        """
        path = Path(workflow=schedule_config.workflow)
        schedule_config_json = Repository._json_pretty_print(
            schedule_config.format())
        self._put_config(path.get_schedule_path(), schedule_config_json)

    def delete_schedule(self, workflow):
        """Delete schedule config for a given workflow.

        Args:
            workflow: The workflow name where the job is defined.
        """
        path = Path(workflow=workflow)
        self._delete_config(path.get_schedule_path())

    def get_job(self, workflow, job):
        """Retrieve config for a given job.

        Args:
            workflow: The workflow name where the job is defined.
            job: The name of the job whose config should be retrieved.
        Returns:
            The job config.
        """
        path = Path(workflow=workflow, job=job)
        job_json = self._get_config(path.get_job_path())
        return JobConfig.from_json(job_json)

    def put_job(self, job_config):
        """Add or replace a job.

        Args:
            job_config: The job config to add or replace.
        """
        path = Path(workflow=job_config.workflow, job=job_config.job)
        job_config_json = Repository._json_pretty_print(job_config.format())
        self._put_config(path.get_job_path(), job_config_json)

    def delete_job(self, workflow, job):
        """Delete config for a given job.

        Args:
            workflow: The workflow name where the job is defined.
            job: The name of the job whose config should be deleted.
        """
        path = Path(workflow=workflow, job=job)
        self._delete_config(path.get_job_path())

    def get_workflow_names(self):
        """Retrieve names of all workflows.

        Returns:
            List of workflow names.
        """
        result = []
        workflow_dirs = self._list_directory(Path.WORKFLOW_PREFIX, True)
        for workflow_dir in workflow_dirs:
            if workflow_dir[-1] != Path.DELIMITER:
                raise PinballException('found unexpected file in workflows '
                                       'directory %s' % Path.WORKFLOW_PREFIX)
            result.append(workflow_dir[:-1])
        return result

    def get_job_names(self, workflow):
        """Retrieve names of all jobs in a given workflow.

        Returns:
            List of job names.
        """
        result = []
        path = Path(workflow=workflow)
        workflow_prefix = path.get_workflow_prefix()
        # It happens that user create a workflow and run a few days,
        # and then delete them. To adaptive this case, we allow the
        # workflow doesn't exists even there is token in PAL.
        self._list_directory(workflow_prefix, True)
        job_prefix = path.get_job_prefix()
        jobs = self._list_directory(job_prefix, True)
        for job in jobs:
            if job[-1] == Path.DELIMITER:
                raise PinballException('found unexpected dir in jobs '
                                       'directory %s' % job_prefix)
            result.append(job)
        return result
