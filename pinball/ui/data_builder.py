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

"""Logic building data objects representing token aggregates."""
# TODO(pawel): move this file under workflow/
import collections
import operator
import pickle
import sys
import time

from pinball.config.utils import get_log
from pinball.config.utils import PinballException
from pinball.scheduler.overrun_policy import OverrunPolicy
from pinball.ui.data import JobData
from pinball.ui.data import JobExecutionData
from pinball.ui.data import Status
from pinball.ui.data import TokenData
from pinball.ui.data import TokenPathData
from pinball.ui.data import WorkflowScheduleData
from pinball.ui.data import WorkflowData
from pinball.ui.data import WorkflowInstanceData
from pinball.workflow import log_saver
from pinball.workflow.name import Name
from pinball.workflow.signaller import Signal


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = ['Pawel Garbacki', 'Mao Ye']
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.ui.data_builder')


class DataBuilder(object):
    # TODO(pawel): change use_cache default to True after we gain enough
    # confidence that it is bug free.
    def __init__(self, store, use_cache=False):
        self._store = store
        self.use_cache = use_cache

    @staticmethod
    def _parse_job_token_name(token_name):
        name = Name.from_job_token_name(token_name)
        if name.workflow:
            return name
        return None

    @staticmethod
    def _get_instance_prefixes(token_names):
        prefixes = set()
        for token_name in token_names:
            name = DataBuilder._parse_job_token_name(token_name)
            if name:
                prefix = name.get_instance_prefix()
                assert prefix, ('Instance prefix not found in job token name '
                                '%s' % token_name)
                prefixes.add(prefix)
        return list(prefixes)

    @staticmethod
    def _job_status_from_job_token(job_token):
        """Extract job status from a job token.

        Args:
            job_token: The token to extract status from.
        Returns:
            Status of the job.
        """
        name = Name.from_job_token_name(job_token.name)
        job = pickle.loads(job_token.data)
        if not job.history:
            return Status.DISABLED if job.disabled else Status.NEVER_RUN
        last_execution_record = job.history[-1]
        if (name.job_state == Name.RUNNABLE_STATE and
                not last_execution_record.end_time):
            return Status.RUNNING
        if job.disabled:
            return Status.DISABLED
        if last_execution_record.exit_code != 0:
            return Status.FAILURE
        return Status.SUCCESS

    @staticmethod
    def _get_progress(execution_history, instance_start_time,
                      instance_end_time):
        """Extract info required to construct job execution progress bar.

        Args:
            execution_history: The list of execution records describing runs of
                this job.
            instance_start_time: The start time of the workflow instance that
                this job belongs to.
            instance_end_time: The end time of the workflow instance that this
                job belongs to or the current time if the instance did not yet
                finish.
        Returns:
            The list of (percentage, status) tuples where status describes
            the state that the job was in on the timeline of the instance,
            while the percentage tells for how long this status lasted as a
            percentage of the total instance run time.  Ordering of the tuples
            on the list follows the job state transitions so the job execution
            timeline can be reconstructed from its contents.
        """
        result = []
        instance_duration = instance_end_time - instance_start_time
        if instance_duration == 0:
            return result
        still_running = False
        first_start_time = None
        for record in execution_history:
            assert record.start_time, record
            if not first_start_time:
                first_start_time = record.start_time
            if record.end_time:
                execution_duration = record.end_time - record.start_time
                status = (Status.to_string(Status.SUCCESS) if
                          record.exit_code == 0 else
                          Status.to_string(Status.FAILURE))
            else:
                assert not still_running
                still_running = True
                execution_duration = instance_end_time - record.start_time
                status = Status.to_string(Status.RUNNING)
            percentage = int(100. * execution_duration / instance_duration)
            percentage = max(1, percentage)
            result.append((percentage, status))

        if first_start_time:
            duration = first_start_time - instance_start_time
        else:
            duration = instance_duration
        result.insert(0, (int(100. * duration / instance_duration), ''))
        return result

    @staticmethod
    def _job_data_from_job_token(job_token, instance_start_time,
                                 instance_end_time):
        """Extract job data from a job token.

        Args:
            job_token: The job token that should be converted to data.
            instance_start_time: The start time of the workflow instance that
                this job belongs to.
            instance_end_time: The end time of the workflow instance that this
                job belongs to or the current time if the instance did not yet
                finish.
        Returns:
            The job data extracted from the token.
        """
        status = DataBuilder._job_status_from_job_token(job_token)
        job = pickle.loads(job_token.data)
        if job.history:
            last_execution_record = job.history[-1]
            last_start_time = last_execution_record.start_time
            last_end_time = last_execution_record.end_time
        else:
            last_start_time = None
            last_end_time = None
        name = Name.from_job_token_name(job_token.name)
        progress = DataBuilder._get_progress(job.history,
                                             instance_start_time,
                                             instance_end_time)
        # TODO(mao): Change the status name from FAILURE to PENDING
        # if the condition is in pending status.
        return JobData(workflow=name.workflow,
                       instance=name.instance,
                       job=name.job,
                       job_type=job.__class__.__name__,
                       is_condition=job.IS_CONDITION,
                       info=job.info(),
                       inputs=job.inputs,
                       outputs=job.outputs,
                       emails=job.emails,
                       max_attempts=job.max_attempts,
                       retry_delay_sec=job.retry_delay_sec,
                       warn_timeout_sec=job.warn_timeout_sec,
                       abort_timeout_sec=job.abort_timeout_sec,
                       priority=job_token.priority,
                       status=status,
                       last_start_time=last_start_time,
                       last_end_time=last_end_time,
                       progress=progress)

    @staticmethod
    def _jobs_data_from_job_tokens(job_tokens):
        instance_start_time = sys.maxint
        instance_end_time = 0
        now = time.time()
        for job_token in job_tokens:
            job = pickle.loads(job_token.data)
            if job.history:
                start_time = job.history[0].start_time
                instance_start_time = min(instance_start_time, start_time)
                end_time = (job.history[-1].end_time
                            if job.history[-1].end_time else now)
                instance_end_time = max(instance_end_time, end_time)
        if instance_start_time == sys.maxint:
            instance_start_time = now
        if instance_end_time == 0:
            instance_end_time = now
        result = []
        for job_token in job_tokens:
            job_data = DataBuilder._job_data_from_job_token(
                job_token, instance_start_time, instance_end_time)
            result.append(job_data)
        return result

    @staticmethod
    def _job_data_less_than(job_data1, job_data2):
        """A comparator for job data objects.

        Jobs are sorted based on the execution time.  Finished jobs sort on the
        end time.  A running job sorts after a finished job.  Running jobs sort
        on the start time.  Jobs that never run sort on qualified name.
        """
        if (not job_data1.last_start and not job_data2.last_start and
                not job_data1.last_end and not job_data2.last_end):
            # neither job ever run
            name1 = Name(workflow=job_data1.workflow,
                         instance=job_data1.instance,
                         job_state=Name.WAITING_STATE,
                         job=job_data1.job).get_job_token_name()
            name2 = Name(workflow=job_data2.workflow,
                         instance=job_data2.instance,
                         job_state=Name.WAITING_STATE,
                         job=job_data2.job).get_job_token_name()
            return name1 < name2

        if not job_data1.last_start and job_data2.last_start:
            # job1 never run, job2 did
            return True
        if job_data1.last_start and not job_data2.last_start:
            # job2 never run, job1 did
            return False
        if not job_data1.last_end and not job_data2.last_end:
            # both jobs are running
            return job_data1.last_start < job_data2.last_start
        if not job_data1.last_end and job_data2.last_end:
            # only job1 is running
            return False
        if job_data1.last_end and not job_data2.last_end:
            # only job2 is running
            return True
        # both jobs run in the past but neither is running now
        return job_data1.last_end < job_data2.last_end

    def _get_signal(self, workflow, instance, action, active):
        signal = Signal.action_to_string(action)
        name = Name(workflow=workflow, instance=instance, signal=signal)
        signal_token_name = name.get_signal_token_name()
        if active:
            tokens = self._store.read_active_tokens(
                name_prefix=signal_token_name)
        else:
            tokens = self._store.read_archived_tokens(
                name_prefix=signal_token_name)
        if not tokens:
            return None
        assert len(tokens) == 1
        assert tokens[0].name == signal_token_name
        return pickle.loads(tokens[0].data)

    def _instance_data_from_job_tokens(self, job_tokens):
        """Extract instance data from job tokens in that instance.

        Args:
            job_tokens: Job tokens that belong to a single workflow instance.
        Returns:
            Workflow data describing the workflow instance identified by the
            input job tokens.
        """
        assert job_tokens
        start_time = time.time()
        end_time = 0
        failed = False
        for job_token in job_tokens:
            job = pickle.loads(job_token.data)
            if job.history:
                first_execution_record = job.history[0]
                if (first_execution_record.start_time and
                        first_execution_record.start_time < start_time):
                    start_time = first_execution_record.start_time
                last_execution_record = job.history[-1]
                if not last_execution_record.end_time:
                    end_time = sys.maxint
                else:
                    if last_execution_record.end_time > end_time:
                        end_time = last_execution_record.end_time
                    if (not job.disabled and
                            last_execution_record.exit_code != 0):
                        failed = True
        if not job_tokens:
            is_active = False
        else:
            is_active = True
            job_name = job_tokens[0].name
            archived_tokens = self._store.read_archived_tokens(
                name_prefix=job_name)
            for token in archived_tokens:
                if token.name == job_name:
                    is_active = False
                    break

        name = Name.from_job_token_name(job_tokens[0].name)
        is_scheduled_for_archive = False
        abort_signal = None
        if is_active:
            archive_signal = self._get_signal(name.workflow,
                                              name.instance,
                                              Signal.ARCHIVE,
                                              True)
            is_scheduled_for_archive = (archive_signal and
                                        Signal.TIMESTAMP_ATTR in
                                        archive_signal.attributes)
        else:
            abort_signal = self._get_signal(name.workflow,
                                            name.instance,
                                            Signal.ABORT,
                                            False)
        if abort_signal:
            status = Status.ABORTED
            if end_time == 0:
                # This can happen only if all jobs have an empty history.
                timestamp = abort_signal.attributes.get(Signal.TIMESTAMP_ATTR)
                start_time = timestamp
                end_time = timestamp
        elif (end_time == 0 or end_time == sys.maxint or
              (is_active and not is_scheduled_for_archive)):
            status = Status.RUNNING
            end_time = None
        elif failed:
            status = Status.FAILURE
        else:
            status = Status.SUCCESS
        return WorkflowInstanceData(name.workflow,
                                    name.instance,
                                    status,
                                    start_time,
                                    end_time)

    def _instances_data_from_job_tokens(self, job_tokens):
        """Extract instance data from job tokens.

        Args:
            job_tokens: Job tokens, potentially from different instances.
        Returns:
            List of workflow instance data defined by input job tokens.
        """
        result = []
        job_tokens_per_instance = collections.defaultdict(list)
        for job_token in job_tokens:
            name = Name.from_job_token_name(job_token.name)
            job_tokens_per_instance[name.get_instance_prefix()].append(
                job_token)
        for _, tokens in job_tokens_per_instance.items():
            result.append(
                self._instance_data_from_job_tokens(tokens))
        return result

    @staticmethod
    def _workflow_data_from_instances_data(instances):
        """Extract workflow data from its instances data.

        Args:
            instances: Workflow instances to extract workflow data from.
            Instances must belong to the same workflow.
        Returns:
            Workflow data defined by input instances.
        """
        assert instances
        start_time = 0
        end_time = 0
        last_instance = None
        running_instance_number = 0
        for instance in instances:
            if not instance.end_time:
                end_time = None
                status = Status.RUNNING
                running_instance_number += 1
                if instance.start_time > start_time:
                    start_time = instance.start_time
                    last_instance = instance.instance
            elif end_time is not None and instance.end_time > end_time:
                start_time = instance.start_time
                end_time = instance.end_time
                status = instance.status
                last_instance = instance.instance
        return WorkflowData(instances[0].workflow,
                            status,
                            start_time,
                            end_time,
                            last_instance,
                            running_instance_number)

    def _workflows_data_from_instances_data(self, instances):
        """Extract workflows data from instances data.

        Args:
            instances: Workflows instances to extract workflows data from.
        Returns:
            List of workflows data defined by input instances data.
        """
        instances_per_workflow = collections.defaultdict(list)
        for instance in instances:
            instances_per_workflow[instance.workflow].append(instance)
        result = []
        for _, instances in instances_per_workflow.items():
            result.append(
                DataBuilder._workflow_data_from_instances_data(instances))
        return result

    def _workflows_data_from_job_tokens(self, job_tokens):
        """Extract workflows data from job tokens.

        Args:
            job_tokens: Job tokens to extract workflows data from.
        Returns:
            List of workflows data defined by input job tokens.
        """
        assert job_tokens
        instances_data = \
            self._instances_data_from_job_tokens(job_tokens)
        return self._workflows_data_from_instances_data(instances_data)

    def _get_job_tokens(self, workflow=None, instance=None, job_state=None,
                        job=None):
        """Extract job tokens from the store.

        Args:
            workflow: The name of the workflow whose jobs we are interested in.
            instance: The name of the instance whose jobs we are interested in.
            job_state: The state of the jobs we are interested in.
            job: The name of the job we are interested in.
        Returns:
            List of jobs matching the specification.
        """
        name = Name(workflow=workflow, instance=instance, job_state=job_state,
                    job=job)
        if name.job:
            prefix = name.get_job_token_name()
        elif name.job_state:
            prefix = name.get_job_state_prefix()
        elif name.instance:
            prefix = name.get_job_prefix()
        elif name.workflow:
            prefix = name.get_workflow_prefix()
        else:
            prefix = ''
        tokens = self._store.read_tokens(name_prefix=prefix)
        result = []
        for token in tokens:
            token_name = Name.from_job_token_name(token.name)
            if token_name.get_job_token_name():
                # This is a job token.
                if not job or job == token_name.job:
                    # We matched the prefix so if we are looking for a specific
                    # job, its names must match exactly.
                    result.append(token)
        return result

    def _get_job(self, workflow, instance, job):
        """Get job definition from the store.

        Args:
            workflow: The name of the job workflow.
            instance: The name of the job instance.
            job: The name of the job.
        Returns:
            Matching job definition.
        """
        job_token = None
        for state in [Name.WAITING_STATE, Name.RUNNABLE_STATE]:
            # There is a small chance that a job will move from one state to
            # another in between the iterations and the method will return None
            # although the job actually exists.
            job_tokens = self._get_job_tokens(workflow=workflow,
                                              instance=instance,
                                              job_state=state,
                                              job=job)
            assert len(job_tokens) <= 1
            if len(job_tokens) == 1:
                job_token = job_tokens[0]
                break
        if job_token:
            return pickle.loads(job_token.data)
        return None

    def _get_jobs(self, workflow, job):
        """Get job definitions from the store across all workflow instances.

        Args:
            workflow: The name of the job workflow.
            instance: The name of the job instance.
            job: The name of the job.
        Returns:
            Matching job definition.
        """
        name = Name(workflow=workflow)
        name_prefix = name.get_workflow_prefix()
        # This is a bit hacky since we bypass the Name module where all the
        # token naming logic is supposed to be located.
        # TODO(pawel): extend the Name module to support abstractions needed
        # here.
        name_infix = '/job/'
        name_suffix = '/%s' % job
        job_tokens = self._store.read_tokens(name_prefix=name_prefix,
                                             name_infix=name_infix,
                                             name_suffix=name_suffix)
        result = []
        for job_token in job_tokens:
            job_record = pickle.loads(job_token.data)
            result.append(job_record)
        return result

    def _get_workflows_using_cache(self):
        """Get workflows, preferably fetching instances from the cache.

        As a side effect, archived instances that do not exist in the cache
        will be added to the cache.

        Returns:
            List of workflows data.
        """
        instances_token_names = self._store.read_token_names(
            name_prefix=Name.WORKFLOW_PREFIX)
        instances_prefixes = DataBuilder._get_instance_prefixes(
            instances_token_names)
        instances = []
        for prefix in instances_prefixes:
            name = Name.from_instance_prefix(prefix)
            assert name.workflow and name.instance, (
                'Expected instance prefix, found %s' % prefix)
            instances.append(self.get_instance(name.workflow, name.instance))
        return self._workflows_data_from_instances_data(instances)

    def get_workflows(self):
        """Get all workflows data from the store.

        Returns:
            List of workflows data.
        """
        if self.use_cache:
            return self._get_workflows_using_cache()
        all_tokens = self._get_job_tokens()
        if not all_tokens:
            return []
        return self._workflows_data_from_job_tokens(all_tokens)

    def get_workflow(self, workflow):
        """Get workflow data from the store.

        Args:
            workflow: The name of the workflow whose instance should be
                retrieved.
        Returns:
            Workflow data or None if it was not found.
        """
        instances = self.get_instances(workflow)
        if not instances:
            return None
        return DataBuilder._workflow_data_from_instances_data(instances)

    def _get_instances_using_cache(self, workflow):
        """Get workflow instances, preferably from the cache.

        As a side effect, archived instances that do not exist in the cache
        will be added to the cache.

        Args:
            workflow: The name of the workflow whose instances we are
                interested in.
        Returns:
            List of instances for the given workflow.
        """
        name = Name(workflow=workflow)
        workflow_prefix = name.get_workflow_prefix()
        workflow_token_names = self._store.read_token_names(
            name_prefix=workflow_prefix)
        instances_prefixes = DataBuilder._get_instance_prefixes(
            workflow_token_names)
        result = []
        for prefix in instances_prefixes:
            name = Name.from_instance_prefix(prefix)
            assert name.workflow and name.instance, (
                'Expected instance prefix, found %s' % prefix)
            result.append(self.get_instance(name.workflow, name.instance))
        return result

    def get_instances(self, workflow):
        """Get from the store workflow instances data for a given workflow.

        Args:
            workflow: The name of the workflow whose instances we are
                interested in.
        Returns:
            List of instances for the given workflow.
        """
        if self.use_cache:
            return self._get_instances_using_cache(workflow)
        workflow_tokens = self._get_job_tokens(workflow=workflow)
        if not workflow_tokens:
            return []
        return self._instances_data_from_job_tokens(workflow_tokens)

    def get_latest_instance(self, workflow):
        """Get latest workflow instance.

        Args:
            workflow: The name of the workflow whose instance we are
                interested in.
        Returns:
            The workflow instance or None if it was not found.
        """
        instances = self.get_instances(workflow)
        return max(instances, key=operator.attrgetter('start_time'))

    def _get_instance_no_cache(self, workflow, instance):
        """Get workflow instance, bypass the cache.

        Args:
            workflow: The name of the workflow whose instance we are
                interested in.
            instance: The instance we are interested in.
        Returns:
            The workflow instance or None if it was not found.
        """
        instance_tokens = self._get_job_tokens(workflow=workflow,
                                               instance=instance)
        if not instance_tokens:
            return None
        return self._instance_data_from_job_tokens(instance_tokens)

    def _get_instance_using_cache(self, workflow, instance):
        """Get workflow instance, preferably from the cache.

        As a side effect, if the instance is archived and it does not exist in
        the cache, it will be added to the cache.

        Args:
            workflow: The name of the workflow whose instance we are
                interested in.
            instance: The instance we are interested in.
        Returns:
            The workflow instance or None if it was not found.
        """
        name = Name(workflow=workflow, instance=instance)
        instance_prefix = name.get_instance_prefix()
        data = self._store.get_cached_data(instance_prefix)
        if data:
            instance_data = pickle.loads(data)
        else:
            # Cache only archived instances.
            if self._store.read_archived_token_names(
                    name_prefix=instance_prefix):
                # The ordering of operations is important.  We need to make
                # sure that we add to the cache instance data constructed from
                # the archived tokens.
                instance_data = self._get_instance_no_cache(workflow, instance)
                self._store.set_cached_data(instance_prefix,
                                            pickle.dumps(instance_data))
            else:
                instance_data = self._get_instance_no_cache(workflow, instance)
        return instance_data

    def get_instance(self, workflow, instance):
        """Get workflow instance data from the store.

        Args:
            workflow: The name of the workflow whose instance we are
                interested in.
            instance: The instance we are interested in.
        Returns:
            The workflow instance or None if it was not found.
        """
        if self.use_cache:
            return self._get_instance_using_cache(workflow, instance)
        return self._get_instance_no_cache(workflow, instance)

    def get_jobs(self, workflow, instance):
        """Get from the store jobs data for a given workflow and instance.

        Args:
            workflow: The name of the workflow whose jobs we are interested in.
            instance: The name of the instance whose jobs we are interested in.
        Returns:
            List of jobs for the given workflow and instance.
        """
        instance_tokens = self._get_job_tokens(workflow=workflow,
                                               instance=instance)
        if not instance_tokens:
            return []
        return DataBuilder._jobs_data_from_job_tokens(instance_tokens)

    @staticmethod
    def _execution_record_to_execution_data(workflow, job, execution,
                                            execution_record):
        return JobExecutionData(
            workflow=workflow,
            instance=execution_record.instance,
            job=job,
            execution=execution,
            info=execution_record.info,
            exit_code=execution_record.exit_code,
            cleanup_exit_code=execution_record.cleanup_exit_code,
            start_time=execution_record.start_time,
            end_time=execution_record.end_time,
            properties=execution_record.properties,
            logs=execution_record.logs)

    def get_executions(self, workflow, instance, job):
        """Get from the store executions data for a given job.

        Args:
            workflow: The name of the workflow whose executions we are
                interested in.
            instance: The name of the instance whose executions we are
                interested in.
            job: The name of the job whose executions we are interested in.
        Returns:
            List of executions for the given workflow, instance, and job.
        """
        job_record = self._get_job(workflow, instance, job)
        result = []
        if not job_record:
            return result
        i = 0
        for execution_record in job_record.history:
            result.append(DataBuilder._execution_record_to_execution_data(
                workflow, job, i, execution_record))
            i += 1
        return result

    def get_executions_across_instances(self, workflow, job):
        """Get from the store executions data for a given job across instances.

        Args:
            workflow: The name of the workflow whose executions we are
                interested in.
            job: The name of the job whose executions we are interested in.
        Returns:
            List of executions for the given workflow and job.
        """
        job_records = self._get_jobs(workflow, job)
        executions_data = []
        if not job_records:
            return executions_data
        for job_record in job_records:
            i = 0
            for execution_record in job_record.history:
                executions_data.append(
                    DataBuilder._execution_record_to_execution_data(
                        workflow, job, i, execution_record))
                i += 1
        sorted_executions_data = sorted(executions_data,
                                        key=lambda execution_data:
                                        (execution_data.start_time,
                                         execution_data.instance,
                                         execution_data.execution))
        # The same execution can be present in multiple tokens if the the
        # workflow was retried.  Therefore we need to de-dup the records.
        result = []
        last_execution_data = None
        for execution_data in sorted_executions_data:
            if (not last_execution_data or
                    execution_data.instance != last_execution_data.instance or
                    execution_data.execution != last_execution_data.execution):
                result.append(execution_data)
                last_execution_data = execution_data
        return result

    def get_execution(self, workflow, instance, job, execution):
        """Get execution data from the store.

        Args:
            workflow: The name of the workflow whose execution we are
                interested in.
            instance: The name of the instance whose execution we are
                interested in.
            job: The name of the job whose execution we are interested in.
            execution: The execution number.
        Returns:
            The execution matching the input parameters.
        """
        job_record = self._get_job(workflow, instance, job)
        if not job_record or len(job_record.history) <= execution:
            return None
        execution_record = job_record.history[execution]
        return JobExecutionData(
            workflow=workflow,
            instance=instance,
            job=job,
            execution=execution,
            info=execution_record.info,
            exit_code=execution_record.exit_code,
            cleanup_exit_code=execution_record.cleanup_exit_code,
            start_time=execution_record.start_time,
            end_time=execution_record.end_time,
            properties=execution_record.properties,
            logs=execution_record.logs)

    def get_file_content(self, workflow, instance, job, execution, log_type):
        """Get content of a given execution log file.

        Args:
            workflow: The name of the workflow whose log we are interested in.
            instance: The name of the instance whose log we are interested in.
            job: The name of the job whose log we are interested in.
            execution: The execution whose log we are interested in.
            log_type: The type of the log we are interested in.
        Returns:
            The content of the matching log file.
        """
        job_record = self._get_job(workflow, instance, job)
        if not job_record:
            return ''
        execution_record = job_record.history[execution]
        if log_type in execution_record.logs:
            file_name = execution_record.logs[log_type]
            f = log_saver.FileLogSaver.from_path(file_name)
            try:
                f.open('r')
                return f.read()
            except:
                LOG.exception('')
            finally:
                # If the log is local file, then we need to close the file.
                if f and not file_name.startswith('s3n://'):
                    f.close()
        else:
            return ''

    def get_schedules(self):
        """Get all workflow schedules data from the store."""
        tokens = self._store.read_tokens(
            name_prefix=Name.WORKFLOW_SCHEDULE_PREFIX)
        result = []
        for token in tokens:
            schedule = pickle.loads(token.data)
            overrun_policy_help = (
                OverrunPolicy.get_help(schedule.overrun_policy))
            result.append(WorkflowScheduleData(
                next_run_time=schedule.next_run_time,
                recurrence_seconds=schedule.recurrence_seconds,
                overrun_policy=schedule.overrun_policy,
                overrun_policy_help=overrun_policy_help,
                workflow=schedule.workflow,
                parser_params=schedule.parser_params,
                emails=schedule.emails,
                max_running_instances=schedule.max_running_instances))
        return result

    def get_schedule(self, workflow):
        """Get workflow schedule data from the store.

        Args:
            workflow: The name of the workflow whose schedule should be
                retrieved.
        Returns:
            The workflow schedule or None if it was not found.
        """
        name = Name(workflow=workflow)
        schedule_token_name = name.get_workflow_schedule_token_name()
        tokens = self._store.read_tokens(name_prefix=schedule_token_name)
        if tokens:
            for token in tokens:
                if token.name == schedule_token_name:
                    schedule = pickle.loads(token.data)
                    overrun_policy_help = OverrunPolicy.get_help(
                        schedule.overrun_policy)
                    return WorkflowScheduleData(
                        next_run_time=schedule.next_run_time,
                        recurrence_seconds=schedule.recurrence_seconds,
                        overrun_policy=schedule.overrun_policy,
                        overrun_policy_help=overrun_policy_help,
                        workflow=schedule.workflow,
                        workflows_config=schedule.workflows_config,
                        emails=schedule.emails,
                        max_running_instances=schedule.max_running_instances)
        return None

    def get_token_paths(self, path):
        """Get token paths data from the store.

        Args:
            path: The path is the name prefix of the parent whose direct
                children should be returned.
        Returns:
            List of direct path descendants of the parent.
        """
        if not path.startswith(Name.DELIMITER):
            raise PinballException('incorrectly formatted path %s' % path)
        # TODO(pawel): this is a bit inefficient as it may load names of quite
        # a few tokens into the memory.
        token_names = self._store.read_token_names(name_prefix=path)
        counts = collections.defaultdict(int)
        path_len = len(path)
        for token_name in token_names:
            index = token_name.find(Name.DELIMITER, path_len)
            if index == -1:
                index = len(token_name)
            else:
                index += 1
            group = token_name[:index]
            counts[group] += 1
        result = []
        for path, count in counts.items():
            result.append(TokenPathData(path, count))
        return result

    def get_token(self, name):
        """Get token data from the store.

        Args:
            path: The name of the token to retrieve.
        Returns:
            The token or None if it was not found.
        """
        tokens = self._store.read_tokens(name)
        token = None
        for token in tokens:
            if token.name == name:
                break
        if not token or token.name != name:
            raise PinballException("didn't find any tokens with name %s" %
                                   name)
        return TokenData(name=token.name,
                         version=token.version,
                         owner=token.owner,
                         expiration_time=token.expirationTime,
                         priority=token.priority,
                         data=token.data)

    def is_signal_set(self, workflow, instance, action):
        """Check if a signal is set.

        Args:
            workflow: The workflow whose signal should be checked.  If None,
                signals at the global level are checked.
            instance: The workflow instance whose signal should be checked.  If
                not None, a matching workflow name must be provided.
                If None, signals at the workflow and the global level are
                checked.
            action: The signal action to check.
        Returns:
            True iff the signal exists in the specified context.
        """
        for (workflow_name, instance_name) in [(workflow, instance),
                                               (workflow, None),
                                               (None, None)]:
            name = Name(workflow=workflow_name, instance=instance_name,
                        signal=Signal.action_to_string(action))
            token_name = name.get_signal_token_name()
            tokens = self._store.read_tokens(token_name)
            assert len(tokens) <= 1
            if tokens:
                return True
        return False
