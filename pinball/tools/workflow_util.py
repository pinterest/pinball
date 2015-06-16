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

"""Command line tool to monitor and control workflows."""
import abc
import argparse
import datetime
import getpass
import pickle
import shutil
import socket
import sys
import time

from pinball.common.s3_utils import delete_s3_directory
from pinball.config.pinball_config import PinballConfig
from pinball.config.utils import get_unique_name
from pinball.config.utils import master_name
from pinball.master.factory import Factory
from pinball.master.thrift_lib.ttypes import GroupRequest
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import Query
from pinball.master.thrift_lib.ttypes import QueryAndOwnRequest
from pinball.master.thrift_lib.ttypes import QueryRequest
from pinball.master.thrift_lib.ttypes import Token
from pinball.tools.base import Command
from pinball.tools.base import CommandException
from pinball.tools.base import confirm
from pinball.ui.data import Status
from pinball.ui.data_builder import DataBuilder
from pinball.workflow.analyzer import Analyzer
from pinball.workflow.name import Name
from pinball.workflow.signaller import Signal
from pinball.workflow.signaller import Signaller
from pinball.workflow.utils import get_logs_dir
from pinball.workflow.utils import get_unique_workflow_instance
from pinball.workflow.utils import load_path


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


def _get_all_tokens(workflow, instance, client):
    """Get all tokens in a given workflow instance.

    Args:
        workflow: The name of the workflow whose tokens should be returned.
        instance: The workflow instance whose tokens should be returned.
        client: The client to use when communicating with the master.
    Returns:
        List of tokens in a given workflow instance.
    """
    name = Name(workflow=workflow, instance=instance)
    prefix = name.get_instance_prefix()
    query = Query(namePrefix=prefix)
    request = QueryRequest(queries=[query])
    response = client.query(request)
    assert len(response.tokens) == 1
    return response.tokens[0]


class Start(Command):
    """Parse and start a new workflow instance."""
    def __init__(self):
        self._workflow = None

    def prepare(self, options):
        self._workflow = options.workflow
        if not self._workflow:
            raise CommandException('start command takes name of '
                                   'workflow to start')

    def execute(self, client, store):
        if not _check_workflow_instances(store, self._workflow):
            raise CommandException('failed to start a new instance for '
                                   'workflow %s due to too many instances '
                                   'running!' % self._workflow)

        config_parser = load_path(PinballConfig.PARSER)(PinballConfig.PARSER_PARAMS)
        workflow_tokens = config_parser.get_workflow_tokens(self._workflow)
        if not workflow_tokens:
            return 'workflow %s not found in %s\n' % (
                self._workflow, str(PinballConfig.PARSER_PARAMS))
        request = ModifyRequest()
        request.updates = workflow_tokens
        assert request.updates
        token = request.updates[0]
        name = Name.from_job_token_name(token.name)
        if not name.instance:
            name = Name.from_event_token_name(token.name)
        client.modify(request)
        return 'exported workflow %s instance %s.  Its tokens are under %s' % (
            name.workflow, name.instance, name.get_instance_prefix())


class Stop(Command):
    """Stop a running workflow instance."""
    def __init__(self):
        self._workflow = None
        self._force = None
        self._instance = None

    def prepare(self, options):
        self._force = options.force
        self._workflow = options.workflow
        self._instance = options.instance
        if not self._workflow or not self._instance:
            raise CommandException('stop command takes name of '
                                   'workflow and instance')

    def execute(self, client, store):
        instance_tokens = _get_all_tokens(self._workflow,
                                          self._instance,
                                          client)
        if not instance_tokens:
            return 'workflow %s instance %s not found\n' % (self._workflow,
                                                            self._instance)
        message = 'Remove workflow %s instance %s' % (self._workflow,
                                                      self._instance)
        output = ''
        if self._force or confirm(message):
            MAX_TRIES = 10
            i = 0
            while i < MAX_TRIES:
                i += 1
                request = ModifyRequest(deletes=instance_tokens)
                try:
                    client.modify(request)
                    break
                except:
                    # This can happen because someone else could have updated
                    # one of the tokens in the meantime.
                    # TODO(pawel): come up with a mechanism to prevent the need
                    # to retry.
                    instance_tokens = _get_all_tokens(self._workflow,
                                                      self._instance,
                                                      client)
            output += 'removed %d token(s) in %d tries\n' % (
                len(instance_tokens), i)
        return output


class Pause(Command):
    """Pause a running workflow instance."""
    def __init__(self):
        self._workflow = None
        self._force = None
        self._instance = None

    def prepare(self, options):
        self._force = options.force
        self._workflow = options.workflow
        self._instance = options.instance
        if not self._workflow or not self._instance:
            raise CommandException('pause command takes name of '
                                   'workflow and instance')

    @staticmethod
    def _own_tokens(tokens):
        for token in tokens:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            token.owner = 'workflow_util user %s machine %s time %s' % (
                getpass.getuser(), socket.gethostname(), timestamp)
            token.expirationTime = sys.maxint

    def execute(self, client, store):
        instance_tokens = _get_all_tokens(self._workflow,
                                          self._instance,
                                          client)
        if not instance_tokens:
            return 'workflow %s instance %s not found\n' % (self._workflow,
                                                            self._instance)
        message = 'pause workflow %s instance %s' % (self._workflow,
                                                     self._instance)
        output = ''
        if self._force or confirm(message):
            MAX_TRIES = 10
            i = 0
            while i < MAX_TRIES:
                Pause._own_tokens(instance_tokens)
                request = ModifyRequest(updates=instance_tokens)
                try:
                    client.modify(request)
                    break
                except:
                    # This can happen because someone else could have updated
                    # one of the tokens in the meantime.
                    # TODO(pawel): come up with a mechanism to prevent the need
                    # to retry.
                    instance_tokens = _get_all_tokens(self._workflow,
                                                      self._instance,
                                                      client)
                i += 1
            if i < MAX_TRIES:
                output += 'claimed %d token(s) in %d tries\n' % (
                    len(instance_tokens), i + 1)
            else:
                output += 'failed to claim token(s) in %d tries' % MAX_TRIES
        return output


class Resume(Command):
    """Resume a paused workflow instance."""
    def __init__(self):
        self._workflow = None
        self._force = None
        self._instance = None

    def prepare(self, options):
        self._force = options.force
        self._workflow = options.workflow
        self._instance = options.instance
        if not self._workflow or not self._instance:
            raise CommandException('resume command takes name of '
                                   'workflow and instance')

    def execute(self, client, store):
        instance_tokens = _get_all_tokens(self._workflow,
                                          self._instance,
                                          client)
        if not instance_tokens:
            return 'workflow %s instance %s not found\n' % (self._workflow,
                                                            self._instance)
        message = 'resume workflow %s instance %s' % (self._workflow,
                                                      self._instance)
        output = ''
        if self._force or confirm(message):
            for token in instance_tokens:
                token.owner = None
                token.expirationTime = None
            request = ModifyRequest(updates=instance_tokens)
            client.modify(request)
            output += ('released ownership of %d token(s)\n' %
                       len(instance_tokens))
        return output


class Retry(Command):
    """Retry failed jobs."""
    def __init__(self):
        self._workflow = None
        self._force = None
        self._instance = None

    def prepare(self, options):
        self._force = options.force
        self._workflow = options.workflow
        self._instance = options.instance
        if not self._workflow or not self._instance:
            raise CommandException('retry command takes name of workflow and '
                                   'instance')

    @staticmethod
    def _is_job_failed(job_token):
        job = pickle.loads(job_token.data)
        if not job.history:
            return False
        last_execution_record = job.history[-1]
        return last_execution_record.exit_code != 0

    @staticmethod
    def _prepare_runnable_job(job):
        if job.history:
            last_execution_record = job.history[-1]
            if last_execution_record.events:
                job.events = last_execution_record.events

    def _retry_active(self, client):
        # Retrieve waiting jobs and ARCHIVE signal from the master.
        query_request = QueryRequest(queries=[])

        signal_name = Name(workflow=self._workflow, instance=self._instance,
                           signal=Signal.action_to_string(Signal.ARCHIVE))
        signal_prefix = signal_name.get_signal_token_name()
        query_request.queries.append(Query(namePrefix=signal_prefix))

        jobs_name = Name(workflow=self._workflow, instance=self._instance,
                         job_state=Name.WAITING_STATE)
        jobs_prefix = jobs_name.get_job_state_prefix()
        query_request.queries.append(Query(namePrefix=jobs_prefix))

        query_response = client.query(query_request)
        assert len(query_response.tokens) == 2

        modify_request = ModifyRequest(updates=[], deletes=[])

        # Remove the ARCHIVE signal.
        if query_response.tokens[0]:
            assert len(query_response.tokens[0]) == 1
            signal_token = query_response.tokens[0][0]
            modify_request.deletes.append(signal_token)

        # Make failed jobs runnable.
        for job_token in query_response.tokens[1]:
            if Retry._is_job_failed(job_token):
                modify_request.deletes.append(job_token)
                runnable_job_name = Name.from_job_token_name(job_token.name)
                runnable_job_name.job_state = Name.RUNNABLE_STATE
                runnable_job = pickle.loads(job_token.data)
                self._prepare_runnable_job(runnable_job)
                runnable_job_token = Token(
                    name=runnable_job_name.get_job_token_name(),
                    priority=job_token.priority,
                    data=pickle.dumps(runnable_job))
                modify_request.updates.append(runnable_job_token)

        if not modify_request.updates and not modify_request.deletes:
            return 'no failed jobs found in workflow %s instance %s\n' % (
                self._workflow, self._instance)
        if not modify_request.updates and modify_request.deletes:
            return 'found ARCHIVE token but no failed jobs in workflow %s ' \
                   'instance %s.  Not changing anything this time\n' % (
                       self._workflow, self._instance)

        client.modify(modify_request)

        if len(modify_request.updates) == len(modify_request.deletes):
            return 'retried %d job(s) and removed an ARCHIVE token from ' \
                   'workflow %s instance %s\n' % (len(modify_request),
                                                  self._workflow,
                                                  self._instance)
        assert (len(modify_request.updates) == len(modify_request.deletes) - 1)
        return 'retried %d job(s) in workflow %s instance %s\n' % (
            len(modify_request.updates), self._workflow, self._instance)

    def _retry_archived(self, client, store):
        instance_name = Name(workflow=self._workflow, instance=self._instance)
        instance_tokens = store.read_archived_tokens(
            name_prefix=instance_name.get_instance_prefix())
        if not instance_tokens:
            return 'workflow %s instance %s not found\n' % (self._workflow,
                                                            self._instance)

        request = ModifyRequest(updates=[])
        new_instance = get_unique_workflow_instance()

        has_failed_jobs = False
        for token in instance_tokens:
            token_name = token.name

            event_name = Name.from_event_token_name(token_name)
            if event_name.workflow:
                # it is an event token
                event_name.instance = new_instance
                event_token = Token(name=event_name.get_event_token_name(),
                                    priority=token.priority,
                                    data=token.data)
                request.updates.append(event_token)
                continue

            job_name = Name.from_job_token_name(token_name)
            if job_name.workflow:
                # it is a job token
                if Retry._is_job_failed(token):
                    has_failed_jobs = True
                    job_name.job_state = Name.RUNNABLE_STATE
                job_name.instance = new_instance
                job_token = Token(name=job_name.get_job_token_name(),
                                  priority=token.priority,
                                  data=token.data)
                request.updates.append(job_token)
                continue

            signal_name = Name.from_signal_token_name(token_name)
            assert signal_name.workflow
            # it is a signal token.  We ignore those.

        if not has_failed_jobs:
            return 'no failed jobs found in workflow %s instance %s\n' % (
                self._workflow, self._instance)

        client.modify(request)
        new_instance_name = Name(workflow=self._workflow,
                                 instance=new_instance)
        return 'retried workflow %s instance %s.  Its tokens are under ' \
               '%s\n' % (self._workflow,
                         self._instance,
                         new_instance_name.get_instance_prefix())

    def execute(self, client, store):
        if not _check_workflow_instances(store, self._workflow):
            raise CommandException('failed to retry workflow %s instance %s due '
                                   'to too many instances are running!' %
                                   (self._workflow, self._instance))

        message = 'retry workflow %s instance %s' % (self._workflow,
                                                     self._instance)
        output = ''
        if self._force or confirm(message):
            name = Name(workflow=self._workflow, instance=self._instance)
            request = GroupRequest(namePrefix=name.get_job_prefix(),
                                   groupSuffix=Name.DELIMITER)
            response = client.group(request)
            if response.counts:
                output += self._retry_active(client)
            else:
                output += self._retry_archived(client, store)
        return output


class Redo(Command):
    """Redo a previously executed job."""
    def __init__(self):
        self._force = None
        self._workflow = None
        self._instance = None
        self._job = None
        self._execution = None

    def prepare(self, options):
        self._force = options.force
        self._workflow = options.workflow
        self._instance = options.instance
        self._job = options.jobs
        self._execution = int(options.execution)
        if ',' in self._job or ' ' in self._job:
            raise CommandException('redo command takes a single job')
        if (not self._workflow or not self._instance or not self._job):
            raise CommandException('redo command takes name of workflow, '
                                   'instance, job, and execution')

    def _get_execution_record(self, job):
        if not job.history:
            return None
        if len(job.history) <= self._execution:
            return None
        return job.history[self._execution]

    def execute(self, client, store):
        output = ''
        message = 'redo execution %d of job %s in workflow %s instance %s' % (
            self._execution, self._job, self._workflow, self._instance)
        if self._force or confirm(message):
            # Retrieve job token.
            owner = get_unique_name()
            expiration_time = time.time() + 60
            job_name = Name(workflow=self._workflow, instance=self._instance,
                            job_state=Name.WAITING_STATE, job=self._job)
            query = Query(namePrefix=job_name.get_job_token_name())
            query_and_own_request = QueryAndOwnRequest(
                owner=owner, expirationTime=expiration_time, query=query)
            query_and_own_response = client.query_and_own(
                query_and_own_request)
            if not query_and_own_response.tokens:
                return 'workflow must be running, the job must be finished ' \
                       'and it cannot be runnable'

            # Find the job token.
            modify_request = ModifyRequest(updates=[])
            waiting_job = None
            for job_token in query_and_own_response.tokens:
                token_name = Name.from_job_token_name(job_token.name)
                if token_name.job == self._job:
                    assert not waiting_job
                    waiting_job = job_token
                else:
                    job_token.owner = None
                    job_token.expirationTime = None
                    modify_request.updates.append(job_token)

            # Make the job runnable.
            job = pickle.loads(waiting_job.data)
            execution_record = self._get_execution_record(job)
            if not execution_record:
                # Unown the job token.
                waiting_job.owner = None
                waiting_job.expirationTime = None
                modify_request.updates.append(waiting_job)
                output = ('could not find execution %d in job history\n' %
                          self._execution)
            else:
                job_name.job_state = Name.RUNNABLE_STATE
                job.events = execution_record.events
                runnable_job = Token(name=job_name.get_job_token_name(),
                                     data=pickle.dumps(job))
                modify_request.updates.append(runnable_job)
                modify_request.deletes = [waiting_job]
                output = ('redoing execution %d of job %s in workflow %s '
                          'instance %s\n' % (self._execution, self._job,
                                             self._workflow, self._instance))
            client.modify(modify_request)

        return output


class Poison(Command):
    """Run poisoned jobs."""
    def __init__(self):
        self._workflow = None
        self._force = None
        self._jobs = None
        self._instance = None

    def prepare(self, options):
        self._force = options.force
        self._workflow = options.workflow
        self._instance = options.instance
        self._jobs = options.jobs.split(',')
        if (not (self._workflow and self._instance and self._jobs) and
                not (self._workflow and self._jobs and
                     PinballConfig.PARSER_PARAMS)):
            raise CommandException('retry command takes name of workflow, '
                                   'instance, and a list of jobs or workflow, '
                                   'parser_param config, and a list of jobs')

    def _get_archive_token(self, client):
        assert self._instance
        name = Name(workflow=self._workflow,
                    instance=self._instance,
                    signal=Signal.action_to_string(Signal.ARCHIVE))
        prefix = name.get_signal_token_name()
        query = Query(namePrefix=prefix)
        request = QueryRequest(queries=[query])
        response = client.query(request)
        assert len(response.tokens) == 1
        if not response.tokens[0]:
            return None
        assert len(response.tokens[0]) == 1
        return response.tokens[0][0]

    def _poison_active(self, client):
        analyzer = Analyzer.from_client(client, self._workflow, self._instance)
        if not analyzer.get_tokens():
            return 'workflow %s instance %s not found\n' % (self._workflow,
                                                            self._instance)
        analyzer.poison(self._jobs)
        event_tokens = analyzer.get_new_event_tokens()
        archive_token = self._get_archive_token(client)
        archive_tokens = []
        if archive_token:
            archive_tokens.append(archive_token)
        request = ModifyRequest(updates=event_tokens, deletes=archive_tokens)

        client.modify(request)
        return 'poisoned workflow %s instance %s roots %s\n' % (
            self._workflow, self._instance, self._jobs)

    def _poison_inactive(self, client, store):
        if self._instance:
            analyzer = Analyzer.from_store(store,
                                           self._workflow,
                                           self._instance)
            if not analyzer.get_tokens():
                return 'workflow %s instance %s not found\n' % (
                    self._workflow, self._instance)
        else:
            analyzer = Analyzer.from_parser_params(self._workflow)
            if not analyzer.get_tokens():
                return 'workflow %s not found in %s\n' % (
                    self._workflow, str(PinballConfig.PARSER_PARAMS))
        analyzer.clear_job_histories()
        analyzer.poison(self._jobs)
        new_instance = get_unique_workflow_instance()
        analyzer.change_instance(new_instance)
        tokens = analyzer.get_tokens()
        request = ModifyRequest(updates=tokens)
        client.modify(request)
        new_instance_name = Name(workflow=self._workflow,
                                 instance=new_instance)
        return 'poisoned workflow %s roots %s.  Tokens of the new ' \
               'instance are under %s\n' % (
                   self._workflow,
                   self._jobs,
                   new_instance_name.get_instance_prefix())

    def execute(self, client, store):
        if self._instance:
            message = 'poison workflow %s instance %s roots %s' % (
                self._workflow, self._instance, self._jobs)
        else:
            message = 'poison workflow %s roots %s parser_params config %s' % (
                self._workflow, self._jobs, str(PinballConfig.PARSER_PARAMS))
        if self._force or confirm(message):
            active = False
            if self._instance:
                name = Name(workflow=self._workflow, instance=self._instance)
                request = GroupRequest(namePrefix=name.get_job_prefix(),
                                       groupSuffix=Name.DELIMITER)
                response = client.group(request)
                if response.counts:
                    active = True
            if active:
                return self._poison_active(client)
            else:
                return self._poison_inactive(client, store)
        return ''


class ModifySignal(Command):
    """Add or remove a signal.

    A signal is added to or removed from the master.
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, action, add):
        self._action = action
        # If not add then remove.
        self._add = add
        self._workflow = None
        self._force = None
        self._instance = None

    def prepare(self, options):
        self._force = options.force
        self._workflow = options.workflow
        self._instance = options.instance
        if not self._workflow and self._instance:
            raise CommandException('workflow must be provided if instance is '
                                   'set')

    def execute(self, client, store):
        action_name = Signal.action_to_string(self._action)
        if self._add:
            # Prefix to put in front of the action name to differentiate
            # between adding and removing the action.
            action_prefix = ''
        else:
            action_prefix = 'UN'
        if self._instance:
            assert self._workflow
            message = '%s%s workflow %s instance %s' % (action_prefix,
                                                        action_name,
                                                        self._workflow,
                                                        self._instance)
        elif self._workflow:
            message = '%s%s all instances of workflow %s' % (action_prefix,
                                                             action_name,
                                                             self._workflow)
        else:
            message = '%s%s all workflows' % (action_prefix, action_name)
        output = ''
        if self._force or confirm(message):
            if self._workflow:
                workflow = self._workflow
            else:
                workflow = None
            if self._instance:
                instance = self._instance
            else:
                instance = None
            signaller = Signaller(client, workflow, instance)
            if self._add and signaller.is_signal_present(self._action):
                output += ('%s has been already set.  Not changing '
                           'anything this time\n' % action_name)
            elif (not self._add and
                  not signaller.is_signal_present(self._action)):
                output += ('%s has been already removed.  Not changing '
                           'anything this time\n' % action_name)
            else:
                name = Name(workflow=workflow, instance=instance,
                            signal=action_name)
                if self._add:
                    signaller.set_action(self._action)
                    output += ('set %s.  Its token is %s\n' % (action_name,
                               name.get_signal_token_name()))
                else:
                    signaller.remove_action(self._action)
                    output += ('removed %s from %s\n' % (action_name,
                               name.get_signal_token_name()))
        return output


class Drain(ModifySignal):
    """Stop kicking off new jobs."""
    def __init__(self):
        super(Drain, self).__init__(Signal.DRAIN, True)


class UnDrain(ModifySignal):
    """Start accepting new jobs after calling Drain."""
    def __init__(self):
        super(UnDrain, self).__init__(Signal.DRAIN, False)


class Abort(ModifySignal):
    """Cancel all jobs running jobs, fail workflows."""
    def __init__(self):
        super(Abort, self).__init__(Signal.ABORT, True)


class UnAbort(ModifySignal):
    """Stop canceling jobs and failing workflows after calling Abort."""
    def __init__(self):
        super(UnAbort, self).__init__(Signal.ABORT, False)


class Exit(ModifySignal):
    """Exit the workers without failing workflows."""
    def __init__(self):
        super(Exit, self).__init__(Signal.EXIT, True)


class UnExit(ModifySignal):
    """Stop exiting workers after calling Exit."""
    def __init__(self):
        super(UnExit, self).__init__(Signal.EXIT, False)


class ModifySchedule(Command):
    """Base class for operations on workflow schedules."""
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self._workflow = None

    @staticmethod
    def _get_schedule_tokens(client, workflows):
        request = QueryRequest(queries=[])
        for workflow in workflows:
            name = Name(workflow=workflow)
            prefix = name.get_workflow_schedule_token_name()
            query = Query(namePrefix=prefix)
            request.queries.append(query)
        response = client.query(request)
        assert len(response.tokens) == len(workflows)
        result = []
        for i in range(0, len(workflows)):
            schedule_tokens = response.tokens[i]
            if schedule_tokens:
                token_found = False
                for schedule_token in schedule_tokens:
                    name = Name.from_workflow_schedule_token_name(
                        schedule_token.name)
                    if name.workflow == workflows[i]:
                        result.append(schedule_token)
                        token_found = True
                        break
                if not token_found:
                    result.append(None)
            else:
                result.append(None)
        return result


class ReSchedule(ModifySchedule):
    """Update workflow schedule.

    Replace the workflow schedule stored in the master with one parsed from
    the configuration.
    """
    def __init__(self):
        super(ReSchedule, self).__init__()
        self._force = None

    def prepare(self, options):
        self._workflow = options.workflow
        self._force = options.force

    def execute(self, client, store):
        config_parser = load_path(PinballConfig.PARSER)(PinballConfig.PARSER_PARAMS)
        workflow_names = config_parser.get_workflow_names()
        if (self._workflow and not self._workflow in workflow_names):
            return 'workflow %s not found\n' % self._workflow
        workflows = ([self._workflow] if self._workflow else workflow_names)
        if not workflows:
            return 'no workflows found in %s' % str(PinballConfig.PARSER_PARAMS)
        message = 'reschedule workflows %s' % workflows
        output = ''
        if self._force or confirm(message):
            request = ModifyRequest(updates=[])
            old_schedule_tokens = ModifySchedule._get_schedule_tokens(
                client, workflows)
            schedule_token_names = []
            rescheduled_workflows = []
            for i in range(0, len(workflows)):
                workflow = workflows[i]
                new_schedule_token = config_parser.get_schedule_token(workflow)
                old_schedule_token = old_schedule_tokens[i]
                old_schedule = None
                new_schedule = None
                if old_schedule_token:
                    assert old_schedule_token.name == new_schedule_token.name
                    new_schedule_token.version = old_schedule_token.version
                    old_schedule = pickle.loads(old_schedule_token.data)
                    new_schedule = pickle.loads(new_schedule_token.data)
                if (not old_schedule or
                        (old_schedule and  # new_schedule and
                         not old_schedule.corresponds_to(new_schedule))):
                    request.updates.append(new_schedule_token)
                    schedule_token_names.append(new_schedule_token.name)
                    rescheduled_workflows.append(workflow)
            client.modify(request)
            output = ('rescheduled workflows %s.  Their schedule tokens are '
                      '%s\n' % (rescheduled_workflows, schedule_token_names))
        return output


class UnSchedule(ModifySchedule):
    """Remove a workflow from the schedule."""
    def __init__(self):
        super(UnSchedule, self).__init__()
        self._force = None

    def prepare(self, options):
        self._workflow = options.workflow
        if not self._workflow:
            raise CommandException('unschedule command takes name of workflow '
                                   'to remove from the schedule')
        self._force = options.force

    def execute(self, client, store):
        schedule_tokens = self._get_schedule_tokens(client, [self._workflow])
        assert len(schedule_tokens) == 1
        schedule_token = schedule_tokens[0]
        if not schedule_token:
            return 'schedule for workflow %s not found\n' % self._workflow
        message = 'remove schedule for workflow %s' % self._workflow
        output = ''
        if self._force or confirm(message):
            request = ModifyRequest(deletes=[schedule_token])
            client.modify(request)
            output = 'removed schedule for workflow %s\n' % self._workflow
        return output


class Reload(Command):
    """Reload jobs configuration."""
    _LEASE_TIME_SEC = 5 * 60

    def __init__(self):
        super(Reload, self).__init__()
        self._workflow = None
        self._instance = None
        self._jobs = None
        self._force = None
        self._output = ''

    def prepare(self, options):
        self._force = options.force
        self._workflow = options.workflow
        self._instance = options.instance
        self._jobs = options.jobs.split(',') if options.jobs else None
        if (not self._workflow or not self._instance or
                not PinballConfig.PARSER_PARAMS):
            raise CommandException('reload command requires workflow name, '
                                   'instance, and parser_params config')

    @staticmethod
    def _unown_tokens(client, tokens):
        if not tokens:
            return
        for token in tokens:
            token.owner = None
            token.expirationTime = None
        request = ModifyRequest(updates=tokens)
        client.modify(request)

    @staticmethod
    def _reload_job_token(job_token, new_job_token):
        job_token.priority = new_job_token.priority
        job = pickle.loads(job_token.data)
        new_job = pickle.loads(new_job_token.data)
        job.reload(new_job)
        job_token.data = pickle.dumps(job)

    def _own_selected_job_tokens(self, client):
        assert self._jobs
        result = []
        for job in self._jobs:
            token = None
            for state in [Name.RUNNABLE_STATE, Name.WAITING_STATE]:
                name = Name(workflow=self._workflow, instance=self._instance,
                            job_state=state, job=job)
                query = Query(namePrefix=name.get_job_token_name(),
                              maxTokens=1)
                request = QueryAndOwnRequest(
                    owner='workflow_util',
                    expirationTime=time.time() + Reload._LEASE_TIME_SEC,
                    query=query)
                response = client.query_and_own(request)
                if response.tokens:
                    assert len(response.tokens) == 1
                    if response.tokens[0].name == name.get_job_token_name():
                        token = response.tokens[0]
                        break
            if token:
                result.append(token)
            else:
                self._output += ('job %s in workflow %s instance %s either '
                                 'not found or already owned' %
                                 (job, self._workflow, self._instance))
                Reload._unown_tokens(client, result)
                return []
        return result

    def _own_all_job_tokens(self, client):
        assert not self._jobs
        name = Name(workflow=self._workflow, instance=self._instance)
        group_request = GroupRequest(namePrefix=name.get_job_prefix(),
                                     groupSuffix=Name.DELIMITER)
        group_response = client.group(group_request)
        counts = group_response.counts
        if not counts:
            self._output += ('workflow %s instance %s not found or already '
                             'archived' % (self._workflow, self._instance))
            return []
        assert len(counts) <= 2  # runnable and waiting
        num_job_tokens = 0
        for count in counts.values():
            num_job_tokens += count
        query = Query(namePrefix=name.get_job_prefix())
        query_and_own_request = QueryAndOwnRequest(
            owner='workflow_util',
            expirationTime=time.time() + Reload._LEASE_TIME_SEC,
            query=query)
        query_and_own_response = client.query_and_own(query_and_own_request)
        tokens = query_and_own_response.tokens
        assert len(tokens) <= num_job_tokens
        if len(tokens) < num_job_tokens:
            self._output += ('only %d out of %d job tokens in workflow %s '
                             'instance %s could be claimed' % (len(tokens),
                                                               num_job_tokens,
                                                               self._workflow,
                                                               self._instance))
            Reload._unown_tokens(client, tokens)
            return []
        return tokens

    def _update_job_tokens(self, job_tokens, config_parser):
        workflow_tokens = config_parser.get_workflow_tokens(self._workflow)
        new_job_tokens = {}
        for token in workflow_tokens:
            name = Name.from_job_token_name(token.name)
            if name.job:
                # Make sure it is a job not an event token.
                new_job_tokens[name.job] = token
        new_job_names = set(new_job_tokens.keys())
        job_names = set([Name.from_job_token_name(token.name).job for token in
                         job_tokens])
        missing_job_names = job_names.difference(new_job_names)
        if missing_job_names:
            self._output = 'jobs %s not found in workflow %s defined in %s' % (
                missing_job_names, self._workflow, str(PinballConfig.PARSER_PARAMS))
            return False
        for job_token in job_tokens:
            name = Name.from_job_token_name(job_token.name)
            assert name.job
            new_job_token = new_job_tokens[name.job]
            Reload._reload_job_token(job_token, new_job_token)
        return True

    def execute(self, client, store):
        config_parser = load_path(PinballConfig.PARSER)(PinballConfig.PARSER_PARAMS)
        workflow_names = config_parser.get_workflow_names()
        if self._workflow not in workflow_names:
            return 'workflow %s not found in %s\n' % (
                self._workflow, str(PinballConfig.PARSER_PARAMS))
        if self._jobs:
            job_tokens = self._own_selected_job_tokens(client)
        else:
            job_tokens = self._own_all_job_tokens(client)
        if not job_tokens:
            return self._output
        if self._update_job_tokens(job_tokens, config_parser):
            self._output = ('reloaded %s in workflow %s instance %s' % (
                'jobs %s' % self._jobs if self._jobs else 'all jobs',
                self._workflow,
                self._instance))
        self._unown_tokens(client, job_tokens)
        return self._output


class Alter(Command):
    """Enable or disable jobs."""
    DISABLE, ENABLE = range(2)
    _MODE = None
    _MODE_NAMES = {
        DISABLE: 'disable',
        ENABLE: 'enable'
    }

    def __init__(self):
        self._workflow = None
        self._instance = None
        self._jobs = None
        self._force = None

    def prepare(self, options):
        self._force = options.force
        self._workflow = options.workflow
        self._instance = options.instance
        self._jobs = options.jobs.split(',')
        if not self._workflow or not self._instance or not self._jobs:
            raise CommandException('%s command takes name of workflow, '
                                   'instance, and a list of jobs' %
                                   Alter._MODE_NAMES[self._MODE])

    def _get_job_tokens(self, client):
        name = Name(workflow=self._workflow, instance=self._instance)
        prefix = name.get_job_prefix()
        query = Query(namePrefix=prefix)
        request = QueryRequest(queries=[query])
        response = client.query(request)
        # For a more efficient lookup.
        jobs = set(self._jobs)
        result = {}
        for job_token in response.tokens[0]:
            job_name = Name.from_job_token_name(job_token.name)
            if job_name.job in jobs:
                result[job_name.job] = job_token
        return result

    @staticmethod
    def _is_owned(token):
        return (token.owner and token.expirationTime and
                token.expirationTime > time.time() - 1)

    def _alter_job_tokens(self, job_tokens, client):
        request = ModifyRequest(updates=[])
        for job_token in job_tokens:
            # TODO(pawel): to prevent others from modifying the tokens while we
            # manipulate them, it would be better to own them.
            if Alter._is_owned(job_token):
                return False
            job = pickle.loads(job_token.data)
            if self._MODE == Alter.DISABLE:
                job.disabled = True
            elif self._MODE == Alter.ENABLE:
                job.disabled = False
            else:
                assert False, 'unrecognized mode %d' % self._MODE
            job_token.data = pickle.dumps(job)
            request.updates.append(job_token)
        try:
            client.modify(request)
        except:
            # This can happen because someone else could have updated one of
            # the tokens in the meantime.
            return False
        return True

    def execute(self, client, store):
        job_tokens = self._get_job_tokens(client)
        assert len(job_tokens) <= len(self._jobs)
        jobs_set = set(self._jobs)
        job_tokens_set = set(job_tokens.keys())
        output = ''
        delta = jobs_set.difference(job_tokens_set)
        mode = Alter._MODE_NAMES[self._MODE]
        if delta:
            return 'job(s) %s not found in the master.  Note that only jobs ' \
                   'of a running workflow can be %sd' % (list(delta), mode)
        message = '%s %d jobs in workflow %s instance %s' % (mode,
                                                             len(self._jobs),
                                                             self._workflow,
                                                             self._instance)
        output = ''
        if self._force or confirm(message):
            MAX_TRIES = 10
            i = 0
            while i < MAX_TRIES:
                if self._alter_job_tokens(job_tokens.values(), client):
                    break
                job_tokens = self._get_job_tokens(client)
                i += 1
            if i < MAX_TRIES:
                output += '%sd %d job(s) in %d tries\n' % (mode,
                                                           len(job_tokens),
                                                           i + 1)
            else:
                output += 'failed to %s job(s) in %d tries' % (mode, MAX_TRIES)
        return output


class Disable(Alter):
    _MODE = Alter.DISABLE


class Enable(Alter):
    _MODE = Alter.ENABLE


class Cleanup(Command):
    """Remove old workflows from the database."""
    def __init__(self):
        self._timestamp = None
        self._force = None

    def prepare(self, options):
        self._force = options.force
        if options.age_days < 7:
            raise CommandException('age of instances to remove must be at '
                                   'least 7 days')
        delta = datetime.timedelta(days=options.age_days).total_seconds()
        self._timestamp = time.time() - delta

    def execute(self, client, store):
        data_builder = DataBuilder(store, use_cache=True)
        workflows = data_builder.get_workflows()
        instances_to_delete = []
        directories_to_delete = []
        for workflow in workflows:
            instances = data_builder.get_instances(workflow.workflow)
            for instance in instances:
                if ((instance.status == Status.FAILURE or
                     instance.status == Status.SUCCESS) and instance.end_time
                        and instance.end_time < self._timestamp):
                    instances_to_delete.append(instance)
                    directories_to_delete.append(
                        get_logs_dir(instance.workflow, instance.instance))
                    if PinballConfig.S3_LOGS_DIR:
                        directories_to_delete.append(get_logs_dir(instance.workflow,
                                                                  instance.instance,
                                                                  PinballConfig.S3_LOGS_DIR))
        tokens_to_delete = []
        for instance in instances_to_delete:
            name = Name(workflow=instance.workflow, instance=instance.instance)
            tokens_to_delete += store.read_archived_tokens(
                name_prefix=name.get_instance_prefix())
        deleted_tokens = 0
        deleted_directories = 0
        output = ''
        if not tokens_to_delete:
            output += 'no tokens need to be cleaned up\n'
        else:
            print 'removing tokens:'
            for token in tokens_to_delete:
                print '\t%s' % token.name
            print 'removing directories:'
            for directory in directories_to_delete:
                print '\t%s' % directory
            message = 'remove %d tokens and %d directories' % (
                len(tokens_to_delete), len(directories_to_delete))
            if self._force or confirm(message):
                store.delete_archived_tokens(tokens_to_delete)
                deleted_tokens = len(tokens_to_delete)
                for directory in directories_to_delete:
                    self._delete_directory(directory)
                deleted_directories = len(directories_to_delete)
        output += 'removed %d token(s) and %d directory(ies)\n' % (
            deleted_tokens, deleted_directories)
        return output

    def _delete_directory(self, directory):
        """Delete the given directory."""
        if directory.startswith('s3n://'):
            delete_s3_directory(directory)
        else:
            shutil.rmtree(directory, ignore_errors=True)


class RebuildCache(Command):
    """Rebuild the data cache."""
    def __init__(self):
        self._force = None

    def prepare(self, options):
        self._force = options.force

    def execute(self, client, store):
        cache_size = len(store.read_cached_data_names())
        message = 'rebuild cache with %d data items' % cache_size
        if self._force or confirm(message):
            store.clear_cached_data()
            data_builder = DataBuilder(store, use_cache=True)
            data_builder.get_workflows()
            cache_size = len(store.read_cached_data_names())
            return 'rebuilt data cache.  It now has %d data items' % cache_size
        return ''


def _check_workflow_instances(store, workflow):
    """Check the number of running instances of the workflow.

    Args:
        store: The store to retrieve runs status.
        workflow: Name of the workflow.

    Returns:
        False if running instance number exceeds the max_running_instances setting;
        Otherwise, True.
    """
    data_builder = DataBuilder(store, use_cache=True)
    instances = data_builder.get_instances(workflow)
    schedule = data_builder.get_schedule(workflow)
    number_running_instances = sum(1 for instance in instances
                                   if instance.status == Status.RUNNING)
    if schedule.max_running_instances and number_running_instances \
            >= schedule.max_running_instances:
        return False
    return True


def run_command(options):
    command = _COMMANDS[options.command]()
    command.prepare(options)
    factory = Factory(master_hostname=PinballConfig.MASTER_HOST,
                      master_port=PinballConfig.MASTER_PORT)
    client = factory.get_client()

    # The reason why these imports are not at the top level is that some of the
    # imported code (db models initializing table names) depends on parameters
    # passed on the command line (master name).  Those imports need to be delayed
    # until after command line parameter parsing.
    from pinball.persistence.store import DbStore
    store = DbStore()
    return command.execute(client, store)


_COMMANDS = {'start': Start, 'stop': Stop, 'pause': Pause, 'resume': Resume,
             'retry': Retry, 'redo': Redo, 'poison': Poison, 'drain': Drain,
             'undrain': UnDrain, 'abort': Abort, 'unabort': UnAbort,
             'exit': Exit, 'unexit': UnExit, 'reschedule': ReSchedule,
             'unschedule': UnSchedule, 'reload': Reload, 'disable': Disable,
             'enable': Enable, 'cleanup': Cleanup,
             'rebuild_cache': RebuildCache}


def main():
    parser = argparse.ArgumentParser(
        description='Manipulate Pinball workflows.')
    parser.add_argument('-c',
                        '--config_file',
                        dest='config_file',
                        required=True,
                        help='full path to the pinball setting configure file')
    parser.add_argument('-f',
                        '--force',
                        dest='force',
                        action='store_true',
                        default=False,
                        help='do not ask for confirmation')
    parser.add_argument('-w',
                        '--workflow',
                        dest='workflow',
                        help='workflow name')
    parser.add_argument('-i',
                        '--instance',
                        dest='instance',
                        help='workflow instance')
    parser.add_argument('-j',
                        '--jobs',
                        dest='jobs',
                        help='comma separated list of jobs')
    parser.add_argument('-e',
                        '--execution',
                        dest='execution',
                        help='job execution number, 0-based, increasing from '
                             'the oldest to the most recent')
    parser.add_argument('-a',
                        '--age_days',
                        dest='age_days',
                        type=int,
                        default=28,
                        help='minimum age of workflow instances to clean up.')
    parser.add_argument('command',
                        choices=_COMMANDS.keys(),
                        help='command name')
    options = parser.parse_args(sys.argv[1:])

    PinballConfig.parse(options.config_file)

    if hasattr(PinballConfig, 'MASTER_NAME') and PinballConfig.MASTER_NAME:
        master_name(PinballConfig.MASTER_NAME)

    print run_command(options)


if __name__ == '__main__':
    main()
