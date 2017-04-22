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

"""Validation tests for the token hierarchy inspector."""
import copy
import mock
import pickle
import random
import time
import unittest

from pinball.master.factory import Factory
from pinball.run_pinball import _pinball_imports
from pinball.run_pinball import _run_worker
from pinball.workflow.event import Event
from pinball.workflow.name import Name
from pinball.workflow.job import ShellJob
from pinball.workflow.worker import Worker
from pinball.master.thrift_lib.ttypes import GroupRequest
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import Token
from pinball.master.thrift_lib.ttypes import Query
from pinball.master.thrift_lib.ttypes import QueryRequest
from pinball.workflow.job_executor import ExecutionRecord
from tests.pinball.persistence.ephemeral_store import EphemeralStore
from pinball.workflow.signaller import Signal


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


def _random_throw_worker_run():
    rand_num = int(random.randrange(1, 100))
    if rand_num % 2 == 0:
        raise Exception("random exception from worker.run")
    return


class WorkerTestCase(unittest.TestCase):
    def setUp(self):
        self._factory = Factory()
        self._store = EphemeralStore()
        self._factory.create_master(self._store)
        self._emailer = mock.Mock()
        self._worker = Worker(self._factory.get_client(), self._store,
                              self._emailer)
        self._client = self._factory.get_client()

    def _get_parent_job_token(self):
        name = Name(workflow='some_workflow',
                    instance='12345',
                    job_state=Name.WAITING_STATE,
                    job='parent_job')
        job = ShellJob(name=name.job,
                       inputs=[Name.WORKFLOW_START_INPUT],
                       outputs=['child_job'],
                       command='echo parent',
                       emails=['some_email@pinterest.com'])
        return Token(name=name.get_job_token_name(), data=pickle.dumps(job))

    def _get_child_job_token(self):
        name = Name(workflow='some_workflow',
                    instance='12345',
                    job_state=Name.WAITING_STATE,
                    job='child_job')
        job = ShellJob(name=name.job,
                       inputs=['parent_job'],
                       outputs=[],
                       command='echo child',
                       emails=['some_email@pinterest.com'])
        return Token(name=name.get_job_token_name(), data=pickle.dumps(job))

    def _post_job_tokens(self):
        """Add waiting job tokens to the master."""
        request = ModifyRequest(updates=[])
        request.updates.append(self._get_parent_job_token())
        request.updates.append(self._get_child_job_token())
        self._client.modify(request)

    def _post_workflow_start_event_token(self):
        name = Name(workflow='some_workflow',
                    instance='12345',
                    job='parent_job',
                    input_name=Name.WORKFLOW_START_INPUT,
                    event='workflow_start_event')
        event = Event(creator='SimpleWorkflowTest')
        token = Token(name=name.get_event_token_name(),
                      data=pickle.dumps(event))
        request = ModifyRequest(updates=[token])
        self._client.modify(request)

    def _verify_token_names(self, names):
        request = GroupRequest(namePrefix='/workflow/')
        response = self._client.group(request)
        names = sorted(names)
        counts = sorted(response.counts.keys())
        self.assertEqual(names, counts)

    def _verify_archived_token_names(self, names):
        active_tokens = self._store.read_active_tokens()
        all_tokens = self._store.read_tokens()
        archived_token_names = []
        for token in all_tokens:
            if not token in active_tokens:
                archived_token_names.append(token.name)
        names = sorted(names)
        archived_token_names = sorted(archived_token_names)
        self.assertEqual(names, archived_token_names)

    def _get_token(self, name):
        query = Query(namePrefix=name)
        request = QueryRequest(queries=[query])
        response = self._client.query(request)
        self.assertEqual(1, len(response.tokens))
        self.assertEqual(1, len(response.tokens[0]))
        return response.tokens[0][0]

    def _get_stored_token(self, name):
        tokens = self._store.read_tokens(name_prefix=name)
        self.assertEqual(1, len(tokens))
        return tokens[0]

    def _verify_parent_job_waiting(self):
        token_names = [
            Name(workflow='some_workflow',
                 instance='12345',
                 job_state=Name.WAITING_STATE,
                 job='parent_job').get_job_token_name(),
            Name(workflow='some_workflow',
                 instance='12345',
                 job_state=Name.WAITING_STATE,
                 job='child_job').get_job_token_name(),
            Name(workflow='some_workflow',
                 instance='12345',
                 job='parent_job',
                 input_name=Name.WORKFLOW_START_INPUT,
                 event='workflow_start_event').get_event_token_name()]
        self._verify_token_names(token_names)

    def _verify_parent_job_runnable(self):
        token_names = [Name(workflow='some_workflow',
                            instance='12345',
                            job_state=Name.RUNNABLE_STATE,
                            job='parent_job').get_job_token_name(),
                       Name(workflow='some_workflow',
                            instance='12345',
                            job_state=Name.WAITING_STATE,
                            job='child_job').get_job_token_name()]
        self._verify_token_names(token_names)

    def test_get_triggering_events(self):
        self.assertEqual([], Worker._get_triggering_events([]))

        self.assertEqual(['a'], Worker._get_triggering_events([['a']]))

        events = Worker._get_triggering_events([['a', 'b']])
        self.assertTrue(events == ['a'] or events == ['b'])

        events = Worker._get_triggering_events([['a', 'b'], ['1', '2']])
        self.assertTrue(events == ['a', '1'] or events == ['a', '2'] or
                        events == ['b', '1'] or events == ['b', '2'])

    def test_move_job_token_to_runnable(self):
        self._post_job_tokens()
        self._post_workflow_start_event_token()
        job_name = Name(workflow='some_workflow',
                        instance='12345',
                        job_state=Name.WAITING_STATE,
                        job='parent_job')
        job_token = self._get_token(job_name.get_job_token_name())
        event_name = Name(workflow='some_workflow',
                          instance='12345',
                          job='parent_job',
                          input_name=Name.WORKFLOW_START_INPUT,
                          event='workflow_start_event')
        event_token = self._get_token(event_name.get_event_token_name())
        self._worker._move_job_token_to_runnable(job_token, [event_token])
        # Event token should have been removed and the parent job should be
        # runnable.
        self._verify_parent_job_runnable()

    def test_make_job_runnable(self):
        self._post_job_tokens()
        self._post_workflow_start_event_token()

        parent_job_name = Name(workflow='some_workflow',
                               instance='12345',
                               job_state=Name.WAITING_STATE,
                               job='parent_job').get_job_token_name()
        child_job_name = Name(workflow='some_workflow',
                              instance='12345',
                              job_state=Name.WAITING_STATE,
                              job='child_job').get_job_token_name()

        parent_job_token = self._get_token(parent_job_name)
        child_job_token = self._get_token(child_job_name)

        self._worker._make_job_runnable(child_job_token)
        # Child job is missing triggering tokens so it cannot be made runnable.
        self._verify_parent_job_waiting()

        self._worker._make_job_runnable(parent_job_token)
        # Parent job has all triggering tokens so it can be made runnable.
        self._verify_parent_job_runnable()

    def test_make_runnable(self):
        self._post_job_tokens()
        self._post_workflow_start_event_token()

        self._worker._make_runnable('some_other_workflow', '12345')
        # Workflow instance does not exist so nothing should have changed.
        self._verify_parent_job_waiting()

        self._worker._make_runnable('some_workflow', 'some_other_instance')
        # Workflow instance does not exist so nothing should have changed.
        self._verify_parent_job_waiting()

        self._worker._make_runnable('some_workflow', '12345')
        self._verify_parent_job_runnable()

    def test_own_runnable_job_token(self):
        self._post_job_tokens()

        self._worker._own_runnable_job_token()
        # Event token is not present so nothing should have changed.
        token_names = [Name(workflow='some_workflow',
                            instance='12345',
                            job_state=Name.WAITING_STATE,
                            job='parent_job').get_job_token_name(),
                       Name(workflow='some_workflow',
                            instance='12345',
                            job_state=Name.WAITING_STATE,
                            job='child_job').get_job_token_name()]
        self._verify_token_names(token_names)
        self.assertIsNone(self._worker._owned_job_token)

        self._post_workflow_start_event_token()
        self._worker._own_runnable_job_token()
        # Worker should now own a runnable job token.
        self._verify_parent_job_runnable()
        parent_token = self._get_token(
            Name(workflow='some_workflow',
                 instance='12345',
                 job_state=Name.RUNNABLE_STATE,
                 job='parent_job').get_job_token_name())
        self.assertEqual(parent_token, self._worker._owned_job_token)

    def _add_history_to_owned_token(self):
        job = pickle.loads(self._worker._owned_job_token.data)
        execution_record = ExecutionRecord(start_time=123456,
                                           end_time=1234567,
                                           exit_code=0)
        job.history.append(execution_record)
        self._worker._owned_job_token.data = pickle.dumps(job)

    def test_get_output_event_tokens(self):
        self._post_job_tokens()
        self._post_workflow_start_event_token()
        self._worker._own_runnable_job_token()
        self.assertIsNotNone(self._worker._owned_job_token)

        job = pickle.loads(self._worker._owned_job_token.data)
        execution_record = ExecutionRecord(start_time=123456,
                                           end_time=1234567,
                                           exit_code=0)
        job.history.append(execution_record)

        event_tokens = self._worker._get_output_event_tokens(job)
        self.assertEqual(1, len(event_tokens))
        event_token_name = Name.from_event_token_name(event_tokens[0].name)
        expected_prefix = Name(workflow='some_workflow',
                               instance='12345',
                               job='child_job',
                               input_name='parent_job').get_input_prefix()
        self.assertEqual(expected_prefix, event_token_name.get_input_prefix())

    def test_move_job_token_to_waiting(self):
        self._post_job_tokens()
        self._post_workflow_start_event_token()
        self._worker._own_runnable_job_token()

        job = pickle.loads(self._worker._owned_job_token.data)
        execution_record = ExecutionRecord(start_time=123456,
                                           end_time=1234567,
                                           exit_code=0)
        job.history.append(execution_record)
        self._worker._owned_job_token.data = pickle.dumps(job)

        self._worker._move_job_token_to_waiting(job, True)

        parent_token = self._get_token(
            Name(workflow='some_workflow',
                 instance='12345',
                 job_state=Name.WAITING_STATE,
                 job='parent_job').get_job_token_name())
        job = pickle.loads(parent_token.data)
        self.assertEqual(1, len(job.history))
        self.assertEqual(execution_record.start_time,
                         job.history[0].start_time)

    def test_keep_job_token_in_runnable(self):
        self._post_job_tokens()
        self._post_workflow_start_event_token()
        self._worker._own_runnable_job_token()

        job = pickle.loads(self._worker._owned_job_token.data)
        job.history.append('some_historic_record')

        self._worker._keep_job_token_in_runnable(job)

        self._verify_parent_job_runnable()
        parent_token = self._get_token(
            Name(workflow='some_workflow',
                 instance='12345',
                 job_state=Name.RUNNABLE_STATE,
                 job='parent_job').get_job_token_name())
        job = pickle.loads(parent_token.data)
        self.assertEqual(1, len(job.history))
        self.assertEqual('some_historic_record', job.history[0])

    @staticmethod
    def _from_job(workflow, instance, job_name, job, data_builder, emailer):
        execution_record = ExecutionRecord(start_time=123456,
                                           end_time=1234567,
                                           exit_code=0)
        executed_job = copy.copy(job)
        executed_job.history.append(execution_record)
        job_executor = mock.Mock()
        job_executor.job = executed_job
        job_executor.prepare.return_value = True
        job_executor.execute.return_value = True
        return job_executor

    @mock.patch('pinball.workflow.worker.JobExecutor')
    def test_execute_job(self, job_executor_mock):
        self._post_job_tokens()
        self._post_workflow_start_event_token()
        self._worker._own_runnable_job_token()
        job_executor = mock.Mock()
        job_executor_mock.from_job.return_value = job_executor

        job_executor_mock.from_job.side_effect = WorkerTestCase._from_job

        self._worker._execute_job()

        self.assertIsNone(self._worker._owned_job_token)
        parent_token = self._get_token(
            Name(workflow='some_workflow',
                 instance='12345',
                 job_state=Name.WAITING_STATE,
                 job='parent_job').get_job_token_name())
        job = pickle.loads(parent_token.data)
        self.assertEqual(1, len(job.history))
        execution_record = job.history[0]
        self.assertEqual(0, execution_record.exit_code)
        self.assertEqual(1234567, execution_record.end_time)

    def test_send_instance_end_email(self):
        data_builder = mock.Mock()
        self._worker._data_builder = data_builder

        schedule_data = mock.Mock()
        schedule_data.emails = ['some_email@pinterest.com']
        data_builder.get_schedule.return_value = schedule_data

        instance_data = mock.Mock()
        data_builder.get_instance.return_value = instance_data

        job_data = mock.Mock()
        data_builder.get_jobs.return_value = [job_data]

        self._worker._send_instance_end_email('some_workflow', '12345')

        self._emailer.send_instance_end_message.assert_called_once_with(
            ['some_email@pinterest.com'], instance_data, [job_data])

    def test_send_job_failure_emails(self):
        self._post_job_tokens()
        self._post_workflow_start_event_token()
        self._worker._own_runnable_job_token()

        job = pickle.loads(self._worker._owned_job_token.data)
        job.history.append('some_historic_record')
        executor = mock.Mock()
        self._worker._executor = executor
        executor.job = job

        data_builder = mock.Mock()
        self._worker._data_builder = data_builder

        schedule_data = mock.Mock()
        schedule_data.emails = ['some_other_email@pinterest.com']
        data_builder.get_schedule.return_value = schedule_data

        execution_data = mock.Mock()
        data_builder.get_execution.return_value = execution_data

        self._worker._send_job_failure_emails(True)

        self._emailer.send_job_execution_end_message.assert_any_call(
            ['some_email@pinterest.com',
             'some_other_email@pinterest.com'], execution_data)

    @mock.patch('pinball.workflow.worker.JobExecutor')
    def test_run(self, job_executor_mock):
        self._post_job_tokens()
        self._post_workflow_start_event_token()

        job_executor_mock.from_job.side_effect = WorkerTestCase._from_job

        self._worker._test_only_end_if_no_runnable = True
        self._worker.run()
        with mock.patch('pinball.workflow.archiver.time') as time_patch:
            # add one day
            time_patch.time.return_value = time.time() + 24 * 60 * 60
            self._worker.run()

        parent_job_token_name = Name(workflow='some_workflow',
                                     instance='12345',
                                     job_state=Name.WAITING_STATE,
                                     job='parent_job').get_job_token_name()
        child_job_token_name = Name(workflow='some_workflow',
                                    instance='12345',
                                    job_state=Name.WAITING_STATE,
                                    job='child_job').get_job_token_name()
        signal_string = Signal.action_to_string(Signal.ARCHIVE)
        signal_token_name = Name(workflow='some_workflow',
                                 instance='12345',
                                 signal=signal_string).get_signal_token_name()

        token_names = [parent_job_token_name,
                       child_job_token_name,
                       signal_token_name]
        self._verify_archived_token_names(token_names)

        self.assertEqual(2, job_executor_mock.from_job.call_count)

        parent_token = self._get_stored_token(parent_job_token_name)
        job = pickle.loads(parent_token.data)
        self.assertEqual(1, len(job.history))
        execution_record = job.history[0]
        self.assertEqual(0, execution_record.exit_code)
        self.assertEqual(1234567, execution_record.end_time)

        child_token = self._get_stored_token(child_job_token_name)
        job = pickle.loads(child_token.data)
        self.assertEqual(1, len(job.history))
        execution_record = job.history[0]
        self.assertEqual(0, execution_record.exit_code)
        self.assertEqual(1234567, execution_record.end_time)

        signal_token = self._get_stored_token(signal_token_name)
        signal = pickle.loads(signal_token.data)
        self.assertEqual(Signal.ARCHIVE, signal.action)

    @mock.patch('pinball.workflow.worker.Worker.run',
                side_effect=_random_throw_worker_run)
    def test_run_worker(self, worker_run_patch):
        _pinball_imports()
        # just sanity check:
        # 1. no exception even if run() throws
        # 2. can finish within short time (no test time out)
        _run_worker(self._factory, self._emailer, self._store)
        self.assertGreater(worker_run_patch.call_count, 0)
