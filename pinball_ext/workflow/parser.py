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

"""Define the worklfow model.

Workflow is represented as a DAG of jobs. In order to express cross-workflow
job dependencies where a job in a workflow may depend on a job in another
workflow (typically a data dependence), a condition job can be used.
A condition job is a light weight job that can be periodically evaluated to
check whether the output data of an upstream job in another workflow is
available.
"""

import calendar
import pickle

from pinball.config.pinball_config import PinballConfig
from pinball.master.thrift_lib.ttypes import Token
from pinball.parser.config_parser import ConfigParser
from pinball.workflow.event import Event
from pinball.workflow.name import Name
from pinball.workflow.utils import get_unique_workflow_instance
from pinball.workflow.utils import load_path
from pinball_ext.job_templates import JobTemplateBase
from pinball_ext.job_templates import ConditionTemplateBase


__author__ = 'Pawel Garbacki, Mao Ye, Jooseong Kim'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


_NAME_DELIMITER = '.'


def _is_name_qualified(job_name):
    """Check if job name is qualified."""
    return _NAME_DELIMITER in job_name


def _get_qualified_name(workflow_name, job_name):
    """Construct a qualified name from workflow name and job name."""
    return workflow_name + _NAME_DELIMITER + job_name


class PyWorkflowParser(ConfigParser):
    """Workflow config parser implementation.

    Attributes:
        workflows_config: the fully qualified name of the workflows config dict
        workflows: dict from workflow name to WorkflowDef object of the
            workflow

        The object represented by workflows_config is structured as follows:

        {
            workflow_name_0 (string): WorkflowConfig(
                jobs={
                    job_name_0 (string): JobConfig(
                        JobTemplate(...),
                        [parent_job_name_0 (string), ...]
                    ),
                    job_name_1 (string): JobConfig(...),
                    ...
                }
                final_job_config=FinalJobConfig(...),
                schedule=ScheduleConfig(
                    recurrence=recurrence_spec (string),
                    time=start_timestamp (string),
                    start_date=start_date (string),
                    ...),
                notify_emails=email_addresses (string)
            ),
            workflow_name_1 (string): WorkflowConfig(...),

            ...

            workflow_name_2 (string): WorkflowConfig(...),
            workflow_name_3 (string): WorkflowConfig(...),
        },
        ...

        For more documentation on WorkflowConfig, JobConfig and ScheduleConfig,
        refer to workflow/config.py
    """
    def __init__(self, params):
        """Initializes PyWorkflowParser object.

        Args:
            params - a dict contains the following keys:
                'workflows_config': maps to the fully qualified name of the
                    workflows config object described above.
                'job_repo_dir'[optional]: the root dir of the repo where all
                    user jobs are stored.
                'job_import_dirs_config'[optional]: maps to the fully qualified
                    name of the object that stores the list of dirs where user
                    jobs are defined. The dirs are relative path to
                    'job_repo_dir'.
        """
        assert 'workflows_config' in params
        super(PyWorkflowParser, self).__init__(params)

        self.parser_params = params
        self.workflows_config_str = params['workflows_config']
        self.workflows_config = load_path(self.workflows_config_str)
        self.workflows = {}

    def get_schedule_token(self, workflow):
        if not self.workflows:
            self.parse_workflows()
        workflow_def = self.workflows.get(workflow)
        if not workflow_def:
            return None
        return workflow_def.get_schedule_token()

    def get_workflow_tokens(self, workflow):
        if not self.workflows:
            self.parse_workflows()
        workflow_def = self.workflows.get(workflow)
        if not workflow_def:
            return None
        return workflow_def.get_workflow_tokens()

    def get_workflow_names(self):
        if not self.workflows:
            self.parse_workflows()
        return self.workflows.keys()

    def parse_workflows(self):
        """Parse workflow configs converting them to WorkflowDefs."""
        for workflow_name, workflow_config in self.workflows_config.items():
            jobs = {}

            workflow_schedule = self.parse_schedule(workflow_name,
                                                    workflow_config)
            workflow_def = WorkflowDef(workflow_name, workflow_schedule,
                                       workflow_config.notify_emails)
            self.workflows[workflow_name] = workflow_def

            for job_name, job_config in workflow_config.jobs.items():
                # For now, we just pass parser params down to JobDef to generate
                # job template params based on that.
                job_def = JobDef(job_name,
                                 job_config.template,
                                 workflow_def,
                                 self.parser_params)
                workflow_def.add_job(job_def)
                jobs[job_name] = job_def

            # add dependency
            for job_name, job_config in workflow_config.jobs.items():
                child_job = jobs[job_name]
                for parent in job_config.dependency:
                    child_job.add_dep(jobs[parent])

            # add final job
            final_job_def = JobDef('final',
                                   workflow_config.final_job_config.template,
                                   workflow_def)
            leaf_jobs = workflow_def.get_leaf_jobs()

            for leaf_job in leaf_jobs:
                final_job_def.add_dep(leaf_job)

            workflow_def.add_job(final_job_def)

        self._verify_workflows()

    def parse_schedule(self, workflow_name, workflow):
        """Parse schedule config and create workflow schedule."""
        recurrence = workflow.schedule.recurrence.total_seconds()

        overrun_policy = workflow.schedule.overrun_policy
        if overrun_policy is None:
            overrun_policy = OverrunPolicy.SKIP

        max_running_instances = workflow.schedule.max_running_instances
        if max_running_instances is None:
            max_running_instances =\
                PinballConfig.DEFAULT_MAX_WORKFLOW_RUNNING_INSTANCES

        notify_emails = workflow.notify_emails.split(',')

        return WorkflowSchedule(
            next_run_time=int(calendar.timegm(
                workflow.schedule.reference_timestamp.timetuple())),
            recurrence_seconds=recurrence,
            overrun_policy=overrun_policy,
            parser_params=self.parser_params,
            workflow=workflow_name,
            emails=notify_emails,
            max_running_instances=max_running_instances)

    def _verify_workflows(self):
        """Verify all workflows."""
        for workflow in self.workflows.values():
            workflow.verify()


class WorkflowVerificationException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class WorkflowDef(object):
    """Workflow is composed of jobs."""

    def __init__(self, name, schedule, notify_emails):
        self.name = name
        self.schedule = schedule
        self.notify_emails = notify_emails
        self.jobs = {}

    def __str__(self):
        return ('workflow:%s, schedule:%s, notify_emails:%s, jobs:%s' %
                (self.name, self.schedule, self.notify_emails, str(self.jobs)))

    def __repr__(self):
        return self.__str__()

    def add_job(self, job):
        """Add a job to the workflow."""
        if job.name in self.jobs:
            raise WorkflowVerificationException(
                "job %s already exists in workflow %s" % (job.name, self.name))
        self.jobs[job.name] = job

    def get_leaf_jobs(self):
        """Get all jobs that no one depends on."""
        leaf_jobs = set(self.jobs.values())
        for _, job in self.jobs.items():
            leaf_jobs = leaf_jobs.difference(job.inputs)
        return list(leaf_jobs)

    def _get_top_level_jobs(self):
        all_jobs = self._get_transitive_deps()
        result = []
        for job in all_jobs:
            if not job.inputs or job.workflow.name != self.name:
                result.append(job)
        return result

    def verify(self):
        """Make sure that the workflow is properly structured."""
        self._check_has_single_final_job()
        # make sure that the workflow is connected and acyclic
        self._check_acyclic()

    def _check_has_single_final_job(self):
        """Make sure that the workflow has a single job that no one depends on.
        """
        num_leaf_jobs = len(self.get_leaf_jobs())
        if num_leaf_jobs != 1:
            raise WorkflowVerificationException(
                "expected exactly one final job in workflow %s, but found %i "
                "final jobs" % (self.name, num_leaf_jobs))

    def _get_transitive_deps(self):
        """Find all jobs that the workflow depends on.

        Return all jobs in the workflow and any other job that a job in the
        workflow depends on directly or indirectly.
        """
        def _dfs(job, path, visited):
            visited.add(job)
            if job in path:
                raise WorkflowVerificationException(
                    "job %s is part of a cycle" % job.get_qualified_name())
            path.append(job)
            if job.workflow.name == self.name:
                # Do not traverse deps of jobs that belong to a different
                # workflow.  External dependencies should go one-level deep.
                for dep in job.inputs:
                    _dfs(dep, path, visited)
            path.pop()

        final_jobs = self.get_leaf_jobs()
        assert len(final_jobs) == 1
        visited = set()
        _dfs(final_jobs[0], [], visited)
        return visited

    def _check_acyclic(self):
        """Make sure that job dependencies do not have a cycle."""
        if self.jobs:
            self._get_transitive_deps()

    def get_schedule_token(self):
        """Create a token describing workflow execution schedule."""
        self.schedule.advance_next_run_time()
        timestamp = self.schedule.next_run_time
        token_name = (
            Name(workflow=self.name).get_workflow_schedule_token_name())
        return Token(name=token_name, owner='parser',
                     expirationTime=timestamp,
                     data=pickle.dumps(self.schedule))

    def get_workflow_tokens(self):
        """Create Pinball tokens representing a workflow instance.

        Convert workflow jobs to tokens and create event tokens in inputs of
        top-level jobs.

        Returns:
            A list of job and event tokens representing a workflow instance.
        """
        all_jobs = self._get_transitive_deps()
        instance = get_unique_workflow_instance()
        result = []
        for job in all_jobs:
            result.append(job.get_job_token(self.name, instance))
        top_level_jobs = self._get_top_level_jobs()
        for job in top_level_jobs:
            event = Event(creator='parser')
            event_name = Name(workflow=self.name,
                              instance=instance,
                              job=job.name,
                              input_name=Name.WORKFLOW_START_INPUT,
                              event='workflow_start_event')
            result.append(Token(name=event_name.get_event_token_name(),
                                data=pickle.dumps(event)))
        return result


class JobDef(object):
    """Job is defined by a template that translates to a config."""

    def __init__(self, job_name, job_template, workflow, job_params=None):
        self.name = job_name
        self.template = job_template
        self.workflow = workflow
        self.job_params = job_params
        self.inputs = []
        self.outputs = []
        # If the job does not have a specified priority, we default to 1
        if job_template.priority:
            self.priority = job_template.priority
        else:
            self.priority = 1.0
        self.dependents = None

    def __hash__(self):
        return self.get_qualified_name().__hash__()

    def __str__(self):
        deps_str = ','.join(dep.get_qualified_name() for dep in self.inputs)

        return ('job:%s, deps:[%s]' % (self.get_qualified_name(), deps_str))

    def __repr__(self):
        return self.__str__()

    def add_dep(self, job):
        self.inputs.append(job)
        job.outputs.append(self)

    def _get_dependents(self):
        if not self.dependents:
            self.dependents = set([self])
            for job in self.outputs:
                self.dependents = self.dependents.union(job._get_dependents())
        return self.dependents

    def compute_score(self):
        """Each node's score is computed as the sum of the priorities of each
        of the nodes in the node's sub-tree"""
        dependents = self._get_dependents()
        return sum((dependent.priority for dependent in dependents))

    def get_qualified_name(self):
        """Get fully qualified job name that includes the workflow name."""
        return _get_qualified_name(self.workflow.name, self.name)

    def get_canonical_name(self, workflow_name):
        """Get a unique name identifying a job in the context of a workflow.

        Canonical names are required in addition to qualified names because
        the same job may be part of multiple workflows (due to cross-workflow
        job dependencies). Azkaban requires jobs to have globally unique
        names.
        """
        _CANONICAL_DELIMITER = '_'
        canonical_name = self.get_qualified_name().replace(
            _NAME_DELIMITER, _CANONICAL_DELIMITER)
        if workflow_name == self.workflow.name:
            return canonical_name
        return (workflow_name + _CANONICAL_DELIMITER + canonical_name)

    def _get_template_params(self):
        """Get params that will be passed down to job template.
        """
        return self.job_params

    def get_job_token(self, workflow_name, workflow_instance):
        """Convert the job to a pinball token representing its properties.
        Condition is similar to job. We check the template to decide use
        job_templates or condition_templates dynamically.

        Args:
            workflow_name: The name of the workflow in which context this job
                is instantiated.
            workflow_instance: The workflow instance of the output job token.

        Returns:
            A pinball token representing the job.
        """

        if self.workflow.name == workflow_name:
            inputs = [input_job.name for input_job in self.inputs]
        else:
            # If it's an external job, do not include its inputs.
            inputs = []
        if not inputs:
            inputs = [Name.WORKFLOW_START_INPUT]
        outputs = []
        for output_job in self.outputs:
            if output_job.workflow.name == workflow_name:
                outputs.append(output_job.name)

        if issubclass(self.template.__class__, JobTemplateBase):
            params = self._get_template_params()
            job = self.template.get_pinball_job(inputs, outputs, params)
            name = Name(workflow=workflow_name,
                        instance=workflow_instance,
                        job_state=Name.WAITING_STATE,
                        job=self.name)
            result = Token(name=name.get_job_token_name(), data=pickle.dumps(job))
            result.priority = self.compute_score()
        elif issubclass(self.template.__class__, ConditionTemplateBase):
            condition = self.template.get_pinball_condition(outputs)
            name = Name(workflow=workflow_name,
                        instance=workflow_instance,
                        job_state=Name.WAITING_STATE,
                        job=self.name)
            result = Token(name=name.get_job_token_name(), data=pickle.dumps(condition))
        else:
            raise Exception("Template must be a subclass of JobTemplateBase or ConditionTemplateBase!")

        return result


# There is a cyclic import dependency between schedule and workflow.  Placing
# imports of such modules at the end of the file makes them work.
from pinball.scheduler.schedule import OverrunPolicy
from pinball.scheduler.schedule import WorkflowSchedule
