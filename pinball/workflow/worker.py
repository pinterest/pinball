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

"""Implementation of workflow worker handling job execution.

Workflow is represented by a collection of job tokens.  Jobs have one or more
inputs and zero or more outputs.  An output of an upstream job connects to the
input of a downstream job.  A special type of input representing workflow
start is defined for top level jobs.  In practice, an input is a name prefix
shared by related event tokens of relevance to a specific job.

A job at a given point time is in one of two states: waiting or runnable.
All jobs start in the waiting state.  If there is at least one event in each
input of a job, the job can be made runnable.  When a worker makes a job
runnable, it consumes (i.e., removes) one event token from each of its inputs.
We call those events triggering events.

Runnable job tokens are claimed by idle workers and executed.  During execution
the ownership of the claimed job token is renewed periodically.  If a worker
fails to renew the ownership, the job token is claimed and executed by someone
else.  In this model we assume job idempotence.
"""
import pickle
import random
import socket
import threading
import time

from thrift.transport import TTransport

from pinball.config.pinball_config import PinballConfig
from pinball.config.utils import get_log
from pinball.config.utils import get_unique_name

from pinball.master.snapshot import Snapshot
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import Query
from pinball.master.thrift_lib.ttypes import QueryAndOwnRequest
from pinball.master.thrift_lib.ttypes import QueryRequest
from pinball.master.thrift_lib.ttypes import Token
from pinball.master.thrift_lib.ttypes import TokenMasterException

from pinball.ui.data_builder import DataBuilder
from pinball.workflow.archiver import Archiver
from pinball.workflow.event import Event
from pinball.workflow.inspector import Inspector
from pinball.workflow.job_executor import JobExecutor
from pinball.workflow.name import Name
from pinball.workflow.signaller import Signal
from pinball.workflow.signaller import Signaller


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.workflow.worker')


class Worker(object):
    # Worker renews the ownership of the job token it owns every so often.
    _LEASE_TIME_SEC = 20 * 60  # 20 minutes

    # Delay between subsequent queries to the master.
    _INTER_QUERY_DELAY_SEC = 5

    def __init__(self, client, store, emailer):
        self._client = client
        self._emailer = emailer
        self._data_builder = DataBuilder(store)
        self._owned_job_token = None
        self._name = get_unique_name()
        self._inspector = Inspector(client)
        # The lock synchronizes access to shared attributes between the worker
        # thread and the lease renewer thread.
        self._lock = threading.Lock()
        self._lease_renewer = None
        self._executor = None
        self._test_only_end_if_no_runnable = False

    @staticmethod
    def _get_triggering_events(inputs):
        """Get a list of triggering events.

        Args:
            inputs: A list of lists where the elements of the outer list
                represent inputs of a job, while the elements of inner lists
                are names of events in those inputs.

        Returns:
            A list of event tokens, one per input, that may be used to trigger
            the job.  If any of the inputs has no events in it, the result list
            will be empty.

        Example:
            inputs = [[token('/workflows/wf/events/j/i1/e1'),
                       token('/workflows/wf/events/j/i1/e2')].
                      [token('/workflows/wf/events/j/i2/e3')]]
            return: [token('/workflows/wf/events/j/i1/e1'),
                     token('/workflows/wf/events/j/i2/e3')]

            inputs = [[token('/workflows/wf/events/j/i1/e1'),
                       token('/workflows/wf/events/j/i1/e2')].
                      []]
            return: []
        """
        triggering_events = []
        for events in inputs:
            if not events:
                return []
            triggering_events.append(events[0])
        return triggering_events

    def _move_job_token_to_runnable(self, job_token, triggering_event_tokens):
        """Move a job token to the runnable branch of the token tree.

        Token tree is the global, hierarchically structured token namespace.
        Args:
            job_token: The job token to make runnable.
            triggering_event_tokens: The list of events used to trigger the
                job.  These events will be removed from the master in the same
                call to that makes the job token runnable.
        Returns:
            True on success, otherwise False.
        """
        name = Name.from_job_token_name(job_token.name)
        name.job_state = Name.RUNNABLE_STATE
        job = pickle.loads(job_token.data)
        Worker._add_events_to_job(job, triggering_event_tokens)
        runnable_job_token = Token(name=name.get_job_token_name(),
                                   priority=job_token.priority,
                                   data=pickle.dumps(job))
        request = ModifyRequest(updates=[runnable_job_token],
                                deletes=triggering_event_tokens + [job_token])
        return self._send_request(request)

    @staticmethod
    def _add_events_to_job(job, triggering_event_tokens):
        """Put triggering events inside the job.

        Args:
            job: The job which should be augmented with the events.
            triggering_event_tokens: List of event tokens that triggered the
                job.
        """
        assert not job.events
        for event_token in triggering_event_tokens:
            if event_token.data:
                event = pickle.loads(event_token.data)
                # Optimization to make the job data structure smaller: do not
                # append events with no attributes.
                if event.attributes:
                    job.events.append(event)
            else:
                # This logic is here for backwards compatibility.
                # TODO(pawel): remove this logic after the transition to the
                # new model has been completed.
                name = Name.from_event_token_name(event_token.name)
                assert name.input == Name.WORKFLOW_START_INPUT

    def _make_job_runnable(self, job_token):
        """Attempt to make a job runnable.

        Query event tokens in job inputs.  If a combination of triggering
        events exist, remove those events and make the job runnable.
        Otherwise, do nothing.

        Args:
            job_token: The job token to make runnable.
        Returns:
            True if there were no errors during communication with the master,
            otherwise False.
        """
        job = pickle.loads(job_token.data)
        name = Name.from_job_token_name(job_token.name)
        request = QueryRequest(queries=[])
        # TODO(pawel): handle jobs with no dependencies
        assert job.inputs
        for input_name in job.inputs:
            prefix = Name()
            prefix.workflow = name.workflow
            prefix.instance = name.instance
            prefix.job = name.job
            prefix.input = input_name
            query = Query()
            query.namePrefix = prefix.get_input_prefix()
            query.maxTokens = 1
            request.queries.append(query)
        try:
            response = self._client.query(request)
        except TokenMasterException:
            # TODO(pawel): add a retry count and fail if a limit is reached.
            LOG.exception('error sending request %s', request)
            return False
        triggering_events = Worker._get_triggering_events(response.tokens)
        if triggering_events:
            return self._move_job_token_to_runnable(job_token,
                                                    triggering_events)
        return True

    def _make_runnable(self, workflow, instance):
        """Attempt to make jobs in a given workflow instance runnable.

        Go over all waiting jobs in a given workflow instance and try to make
        them runnable.

        Args:
            workflow: The name of the workflow whose jobs should be considered.
            instance: The workflow instance whose jobs should be considered.
        Returns:
            True if there were no errors during communication with the master,
            otherwise False.
        """
        name = Name()
        name.workflow = workflow
        name.instance = instance
        name.job_state = Name.WAITING_STATE
        query = Query(namePrefix=name.get_job_state_prefix())
        # TODO(pawel): to prevent multiple workers from trying to make the
        # same job runnable at the same time, this should be a
        # QueryAndOwnRequest.  Note that the current implementation is correct,
        # just inefficient.
        request = QueryRequest(queries=[query])
        try:
            response = self._client.query(request)
        except TokenMasterException:
            LOG.exception('error sending request %s', request)
            return False
        assert len(response.tokens) == 1
        for token in response.tokens[0]:
            if not self._make_job_runnable(token):
                return False
        return True

    def _has_no_runnable_jobs(self, workflow, instance):
        """Check if the workflow instance does not contain runnable jobs.

        Returns:
            True if we are certain that the workflow has no runnable jobs.
            Otherwise False.  If there were any errors during communication
            with the master, the return value is False.
        """
        name = Name(workflow=workflow,
                    instance=instance,
                    job_state=Name.RUNNABLE_STATE)
        query = Query(namePrefix=name.get_job_state_prefix())
        request = QueryRequest(queries=[query])
        try:
            response = self._client.query(request)
        except TokenMasterException:
            LOG.exception('error sending request %s', request)
            return False
        assert len(response.tokens) == 1
        if response.tokens[0]:
            return False
        return True

    def _is_done(self, workflow, instance):
        """Check if the workflow instance is done.

        A workflow is done if it does not have runnable jobs.

        Returns:
            True if we are certain that the workflow is not running.  Otherwise
            False.  If there were any errors during communication with the
            master, the return value is False.
        """
        # Attempt to make the workflow runnable and verify that no WAITING job
        # tokens were changed in the meantime.
        name = Name(workflow=workflow,
                    instance=instance,
                    job_state=Name.WAITING_STATE)
        query = Query(namePrefix=name.get_job_state_prefix())
        request = QueryRequest(queries=[query])
        try:
            snapshot = Snapshot(self._client, request)
        except:
            LOG.exception('error sending request %s', request)
            return False
        if not self._make_runnable(workflow, instance):
            return False
        if not self._has_no_runnable_jobs(workflow, instance):
            return False
        try:
            return not snapshot.refresh()
        except:
            LOG.exception('error sending request %s', request)
            return False

    def _process_signals(self, workflow, instance):
        """Process signals for a given workflow instance.

        Args:
            workflow: The workflow whose signals should be processed.
            instance: The instance whose signals should be processed.
        Returns:
            True if the worker should execute jobs in this instance.  Otherwise
            False.
        """
        signaller = Signaller(self._client, workflow, instance)
        archiver = Archiver(self._client, workflow, instance)
        if signaller.is_action_set(Signal.EXIT):
            return False
        if (signaller.is_action_set(Signal.ARCHIVE) and
                self._is_done(workflow, instance)):
            # TODO(pawel): enable this for all workflows after we gain
            # confidence that the master has enough memory to delay workflow
            # archiving.
            if workflow == 'indexing':
                ARCHIVE_DELAY_SEC = 7 * 24 * 60 * 60  # 7 days
            else:
                ARCHIVE_DELAY_SEC = 12 * 60 * 60  # 12 hours
            expiration_timestamp = int(time.time()) + ARCHIVE_DELAY_SEC
            if signaller.set_attribute_if_missing(Signal.ARCHIVE,
                                                  Signal.TIMESTAMP_ATTR,
                                                  expiration_timestamp):
                self._send_instance_end_email(workflow, instance)
            else:
                expiration_timestamp = signaller.get_attribute(
                    Signal.ARCHIVE, Signal.TIMESTAMP_ATTR)
                archiver.archive_if_expired(expiration_timestamp)
            return False
        if signaller.is_action_set(Signal.ABORT):
            if archiver.archive_if_aborted():
                self._send_instance_end_email(workflow, instance)
            return False
        if signaller.is_action_set(Signal.DRAIN):
            return False
        return True

    def _query_and_own_runnable_job_token(self, workflow, instance):
        """Attempt to own a runnable job token from a given workflow instance.

        Try to own a runnable job token in a given workflow instance.  The
        ownership of the qualifying job token lasts for a limited time so it
        has to be periodically renewed.

        Args:
            workflow: The name of the workflow whose jobs should be considered.
            instance: The workflow instance whose jobs should be considered.
        """
        assert not self._owned_job_token
        name = Name(workflow=workflow,
                    instance=instance,
                    job_state=Name.RUNNABLE_STATE)
        query = Query()
        query.namePrefix = name.get_job_state_prefix()
        query.maxTokens = 1
        request = QueryAndOwnRequest()
        request.query = query
        request.expirationTime = time.time() + Worker._LEASE_TIME_SEC
        request.owner = self._name
        try:
            response = self._client.query_and_own(request)
            if response.tokens:
                assert len(response.tokens) == 1
                self._owned_job_token = response.tokens[0]
        except TokenMasterException:
            LOG.exception('error sending request %s', request)

    def _own_runnable_job_token(self):
        """Attempt to own a runnable job token from any workflow."""
        assert not self._owned_job_token
        workflow_names = self._inspector.get_workflow_names()
        # Shuffle workflows to address starvation.
        random.shuffle(workflow_names)
        for workflow in workflow_names:
            instances = self._inspector.get_workflow_instances(workflow)
            time.sleep(Worker._INTER_QUERY_DELAY_SEC)
            random.shuffle(instances)
            for instance in instances:
                if self._process_signals(workflow, instance):
                    self._make_runnable(workflow, instance)
                    self._query_and_own_runnable_job_token(workflow, instance)
                    if self._owned_job_token:
                        return
            time.sleep(Worker._INTER_QUERY_DELAY_SEC)

    def _abort(self):
        """Abort the running job."""
        assert self._executor
        self._executor.abort()

    def _process_abort_signals(self):
        """Check if the running job should be aborted.

        Returns:
            False iff the job has been aborted.
        """
        name = Name.from_job_token_name(self._owned_job_token.name)
        abort = False
        try:
            signaller = Signaller(self._client, name.workflow, name.instance)
            abort = signaller.is_action_set(Signal.ABORT)
        except (TTransport.TTransportException, socket.timeout, socket.error):
            # We need this exception handler only in logic located in the
            # Timer thread.  If that thread fails, we should abort the process
            # and let the main thread decide what to do.
            LOG.exception('')
            abort = True
        if abort:
            self._abort()
        return not abort

    def _refresh_job_properties(self):
        """Record job properties in the master if they changed.

        If there are communication issues with the master, the running job
        gets aborted.

        Returns:
            False iff there was an error during communication with the master.
        """
        assert self._executor
        if self._executor.job_dirty:
            # The ordering here is important - we need to reset the changed
            # flag before updating the token.
            self._executor.job_dirty = False
            self._owned_job_token.data = pickle.dumps(self._executor.job)
            if not self._update_owned_job_token():
                self._abort()
                return False
        return True

    def _renew_ownership(self):
        """Periodic job token ownership renewal routine."""
        assert self._owned_job_token

        if not self._process_abort_signals():
            return

        if not self._refresh_job_properties():
            return

        now = time.time()
        if (self._owned_job_token.expirationTime <
                now + Worker._LEASE_TIME_SEC / 2):
            self._owned_job_token.expirationTime = (now +
                                                    Worker._LEASE_TIME_SEC)
            if not self._update_owned_job_token():
                self._abort()
                return

        with self._lock:
            if self._lease_renewer:
                self._lease_renewer = threading.Timer(
                    Worker._randomized_worker_polling_time(),
                    self._renew_ownership)
                self._lease_renewer.start()

    def _start_renew_ownership(self):
        """Start periodic renewal of the claimed job token ownership."""
        assert not self._lease_renewer
        self._lease_renewer = threading.Timer(
            Worker._randomized_worker_polling_time(),
            self._renew_ownership)
        self._lease_renewer.start()

    def _stop_renew_ownership(self):
        """Stop periodic renewal of the claimed job token ownership."""
        with self._lock:
            assert self._lease_renewer
            self._lease_renewer.cancel()
            lease_renewer = self._lease_renewer
            self._lease_renewer = None
        lease_renewer.join()

    def _send_request(self, request):
        """Send a modify request to the master.

        Args:
            request: The modify request to send.
        Returns:
            True on success, otherwise False.
        """
        try:
            self._client.modify(request)
            return True
        except TokenMasterException:
            LOG.exception('error sending request %s', request)
            return False

    def _get_output_event_tokens(self, job):
        """Create output event tokens for the owned job token.

        Args:
            job: The job which output tokens should be generated.
        Returns:
            A list of event tokens corresponding to the outputs of the owned
            job token.
        """
        assert self._owned_job_token
        job_name = Name.from_job_token_name(self._owned_job_token.name)
        output_name = Name()
        output_name.workflow = job_name.workflow
        output_name.instance = job_name.instance
        output_name.input = job_name.job
        event_tokens = []
        for output in job.outputs:
            output_name.job = output
            output_name.event = get_unique_name()
            event = Event(creator=self._name)
            assert job.history
            execution_record = job.history[-1]
            event.attributes = execution_record.get_event_attributes()
            event_tokens.append(Token(name=output_name.get_event_token_name(),
                                      data=pickle.dumps(event)))
        return event_tokens

    def _move_job_token_to_waiting(self, job, succeeded):
        """Move the owned job token to the waiting group.

        If the job succeeded, also post events to job outputs.  If the job
        failed or it is the final job (a job with no outputs),  post an archive
        signal to finish the workflow.

        Args:
            job: The job that should be stored in the data field of the waiting
                job token.
            succeeded: True if the job succeeded, otherwise False.
        """
        assert self._owned_job_token
        name = Name.from_job_token_name(self._owned_job_token.name)
        name.job_state = Name.WAITING_STATE
        waiting_job_token = Token(name=name.get_job_token_name(),
                                  priority=self._owned_job_token.priority,
                                  data=pickle.dumps(job))
        request = ModifyRequest(deletes=[self._owned_job_token],
                                updates=[waiting_job_token])
        if succeeded:
            request.updates.extend(self._get_output_event_tokens(job))
        if not job.outputs or not succeeded:
            # This is either the only job in the workflow with no outputs or a
            # failed job.  In either case, the workflow is done.
            signaller = Signaller(self._client,
                                  workflow=name.workflow,
                                  instance=name.instance)
            if not signaller.is_action_set(Signal.ARCHIVE):
                signal_name = Name(
                    workflow=name.workflow,
                    instance=name.instance,
                    signal=Signal.action_to_string(Signal.ARCHIVE))
                signal = Signal(Signal.ARCHIVE)
                signal_token = Token(name=signal_name.get_signal_token_name())
                signal_token.data = pickle.dumps(signal)
                request.updates.append(signal_token)
        self._send_request(request)

    def _unown(self, token):
        """Reset the ownership of a token.

        Args:
            token: The token whose ownership should be reset.
        """
        token.owner = None
        token.expirationTime = None

    def _keep_job_token_in_runnable(self, job):
        """Keep the owned job token in the runnable group.

        Refresh the job token data field with the provided job object, release
        the ownership of the token, and return it to the runnable group.

        Args:
            job: The job that should be stored in the data field of the job
                token.
        """
        assert self._owned_job_token
        request = ModifyRequest()
        self._owned_job_token.data = pickle.dumps(job)
        retry_delay_sec = job.retry_delay_sec
        if retry_delay_sec > 0:
            self._owned_job_token.expirationTime = (time.time() +
                                                    retry_delay_sec)
        else:
            self._unown(self._owned_job_token)
        request.updates = [self._owned_job_token]
        self._send_request(request)

    def _update_owned_job_token(self):
        """Update owned job token in the master.

        Returns:
            True if the update was successful, otherwise False.
        """
        assert self._owned_job_token
        request = ModifyRequest()
        request.updates = [self._owned_job_token]
        try:
            response = self._client.modify(request)
        except TokenMasterException:
            LOG.exception('error sending request %s', request)
            return False
        assert len(response.updates) == 1
        self._owned_job_token = response.updates[0]
        return True

    def _execute_job(self):
        """Execute the owned job."""
        assert self._owned_job_token
        job = pickle.loads(self._owned_job_token.data)
        name = Name.from_job_token_name(self._owned_job_token.name)
        self._executor = JobExecutor.from_job(name.workflow,
                                              name.instance,
                                              name.job,
                                              job,
                                              self._data_builder,
                                              self._emailer)
        success = self._executor.prepare()
        if success:
            self._owned_job_token.data = pickle.dumps(self._executor.job)
            success = self._update_owned_job_token()
            if success:
                self._start_renew_ownership()
                success = self._executor.execute()
                self._stop_renew_ownership()
        if success:
            self._move_job_token_to_waiting(self._executor.job, True)
        elif self._executor.job.retry():
            self._keep_job_token_in_runnable(self._executor.job)
        else:
            signaller = Signaller(self._client, name.workflow, name.instance)
            # If ARCHIVE is not set, this is the first failed job in the
            # workflow.
            first_failure = not signaller.is_action_set(Signal.ARCHIVE)
            self._move_job_token_to_waiting(self._executor.job, False)
            self._send_job_failure_emails(first_failure)
        self._executor = None
        self._owned_job_token = None
        # If needed, archive the workflow.
        self._process_signals(name.workflow, name.instance)

    def _send_instance_end_email(self, workflow, instance):
        try:
            schedule_data = self._data_builder.get_schedule(workflow)
            if not schedule_data:
                LOG.warning('no schedule found for workflow %s', workflow)
            elif schedule_data.emails:
                instance_data = self._data_builder.get_instance(workflow,
                                                                instance)
                jobs_data = self._data_builder.get_jobs(workflow, instance)
                self._emailer.send_instance_end_message(schedule_data.emails,
                                                        instance_data,
                                                        jobs_data)
        except:
            LOG.exception('error sending instance end email for workflow %s '
                          'instance %s', workflow, instance)

    def _send_job_failure_emails(self, first_failure):
        assert self._owned_job_token
        name = Name.from_job_token_name(self._owned_job_token.name)
        job = self._executor.job
        emails = set(job.emails)
        if first_failure:
            schedule_data = self._data_builder.get_schedule(name.workflow)
            if schedule_data:
                emails.update(schedule_data.emails)
            else:
                LOG.warning('no schedule found for workflow %s', name.workflow)
        if emails:
            execution = len(job.history) - 1
            job_execution_data = self._data_builder.get_execution(
                name.workflow, name.instance, name.job, execution)
            try:
                self._emailer.send_job_execution_end_message(
                    list(emails), job_execution_data)
            except:
                LOG.exception('error sending job failure email for '
                              'workflow %s instance %s job %s execution %d',
                              name.workflow,
                              name.instance,
                              name.job,
                              execution)

    @staticmethod
    def _randomized_worker_polling_time():
        """Generate random worker polling time."""
        return (1.0 + random.random()) * PinballConfig.WORKER_POLL_TIME_SEC

    def run(self):
        """Run the worker."""
        LOG.info('Running worker ' + self._name)
        while True:
            signaller = Signaller(self._client)
            if signaller.is_action_set(Signal.EXIT):
                return
            if not signaller.is_action_set(Signal.DRAIN):
                self._own_runnable_job_token()
            if self._owned_job_token:
                self._execute_job()
            elif self._test_only_end_if_no_runnable:
                return
            else:
                time.sleep(Worker._randomized_worker_polling_time())
        LOG.info('Exiting worker ' + self._name)
