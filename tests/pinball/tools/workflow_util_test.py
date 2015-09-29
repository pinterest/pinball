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

"""Validation tests for workflow_util tool."""
import collections
import copy
import mock
import pickle
import sys
import unittest

from pinball.config.pinball_config import PinballConfig
from pinball.master.thrift_lib.ttypes import GroupRequest
from pinball.master.thrift_lib.ttypes import GroupResponse
from pinball.master.thrift_lib.ttypes import ModifyRequest
from pinball.master.thrift_lib.ttypes import ModifyResponse
from pinball.master.thrift_lib.ttypes import Query
from pinball.master.thrift_lib.ttypes import QueryAndOwnRequest
from pinball.master.thrift_lib.ttypes import QueryAndOwnResponse
from pinball.master.thrift_lib.ttypes import QueryRequest
from pinball.master.thrift_lib.ttypes import QueryResponse
from pinball.master.thrift_lib.ttypes import Token
from pinball.scheduler.schedule import WorkflowSchedule
from pinball.tools.workflow_util import Abort
from pinball.tools.workflow_util import Cleanup
from pinball.tools.workflow_util import Disable
from pinball.tools.workflow_util import Drain
from pinball.tools.workflow_util import Enable
from pinball.tools.workflow_util import Exit
from pinball.tools.workflow_util import Pause
from pinball.tools.workflow_util import Poison
from pinball.tools.workflow_util import RebuildCache
from pinball.tools.workflow_util import Redo
from pinball.tools.workflow_util import Reload
from pinball.tools.workflow_util import ReSchedule
from pinball.tools.workflow_util import Retry
from pinball.tools.workflow_util import Resume
from pinball.tools.workflow_util import Start
from pinball.tools.workflow_util import Stop
from pinball.tools.workflow_util import UnAbort
from pinball.tools.workflow_util import UnDrain
from pinball.tools.workflow_util import UnExit
from pinball.tools.workflow_util import UnSchedule
from pinball.ui.data import Status
from pinball.ui.data import WorkflowData
from pinball.ui.data import WorkflowInstanceData
from pinball.workflow.event import Event
from pinball.workflow.job import ShellJob
from pinball.workflow.job_executor import ExecutionRecord
from pinball.workflow.signaller import Signal


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = ['Pawel Garbacki', 'Mao Ye']
__license__ = 'Apache'
__version__ = '2.0'


def _cmp_job_tokens(test, job_token1, job_token2):
    token1 = copy.copy(job_token1)
    token2 = copy.copy(job_token2)
    job1 = pickle.loads(token1.data)
    job2 = pickle.loads(token2.data)
    test.assertEqual(str(job1), str(job2))
    token1.data = None
    token2.data = None
    test.assertEqual(token1, token2)


class StartTestCase(unittest.TestCase):
    @mock.patch('pinball.parser.utils.load_path')
    @mock.patch('pinball.tools.workflow_util._check_workflow_instances')
    def test_start_non_existent(self, check_workflow_instances_mock, load_path_mock):
        Options = collections.namedtuple('args', 'workflow')
        options = Options(workflow='does_not_exist')
        PinballConfig.PARSER_PARAMS = {'key': 'value'}
        command = Start()
        command.prepare(options)
        check_workflow_instances_mock.return_value = True
        config_parser = mock.Mock()

        def load_path(params):
            self.assertEqual(['caller', 'key'], sorted(params.keys()))
            return config_parser
        load_path_mock.return_value = load_path
        config_parser.get_workflow_tokens.return_value = None

        client = mock.Mock()
        output = command.execute(client, None)

        self.assertEqual('workflow does_not_exist not found in %s\n' %
                         str(PinballConfig.PARSER_PARAMS), output)

    @mock.patch('pinball.parser.utils.load_path')
    @mock.patch('pinball.tools.workflow_util._check_workflow_instances')
    def test_start_workflow(self, check_workflow_instance_mock, load_path_mock):
        Options = collections.namedtuple('args', 'workflow')
        options = Options(workflow='some_workflow')
        PinballConfig.PARSER_PARAMS = {'key': 'value'}
        command = Start()
        command.prepare(options)
        check_workflow_instance_mock.return_value = True
        config_parser = mock.Mock()

        def load_path(params):
            self.assertEqual(['caller', 'key'], sorted(params.keys()))
            return config_parser
        load_path_mock.return_value = load_path
        job_token = Token(
            name='/workflow/some_workflow/123/job/waiting/some_job',
            data='some_data')
        event_token = Token(name=('/workflow/some_workflow/123/input/some_job/'
                                  '__WORKFLOW_START__'),
                            data='some_data')
        config_parser.get_workflow_tokens.return_value = [job_token,
                                                          event_token]
        client = mock.Mock()

        output = command.execute(client, None)

        config_parser.get_workflow_tokens.assert_called_once_with(
            'some_workflow')
        self.assertEqual('exported workflow some_workflow instance 123.  '
                         'Its tokens are under '
                         '/workflow/some_workflow/123/', output)


class StopTestCase(unittest.TestCase):
    def test_stop_non_existent(self):
        Options = collections.namedtuple('args', 'workflow, instance, force')
        options = Options(workflow='does_not_exist', instance='123',
                          force=True)
        command = Stop()
        command.prepare(options)

        response = QueryResponse(tokens=[[]])
        client = mock.Mock()
        client.query.return_value = response

        output = command.execute(client, None)

        query = Query(namePrefix='/workflow/does_not_exist/123/')
        query_request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(query_request)
        self.assertEqual('workflow does_not_exist instance 123 not found\n',
                         output)

    def test_stop_workflow(self):
        Options = collections.namedtuple('args', 'workflow, instance, force')
        options = Options(workflow='some_workflow', instance='123', force=True)
        command = Stop()
        command.prepare(options)

        token = Token(version=1234567,
                      name='/workflow/some_workflow/123/job/waiting/some_job',
                      data='some_data')
        query_response = QueryResponse(tokens=[[token]])
        client = mock.Mock()
        client.query.return_value = query_response
        client.modify.return_value = ModifyResponse()

        output = command.execute(client, None)

        query = Query(namePrefix='/workflow/some_workflow/123/')
        query_request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(query_request)
        modify_request = ModifyRequest(deletes=[token])
        client.modify.assert_called_once_with(modify_request)
        self.assertEqual('removed 1 token(s) in 1 tries\n', output)


class PauseTestCase(unittest.TestCase):
    def test_pause_non_existent(self):
        Options = collections.namedtuple('args', 'workflow, instance, force')
        options = Options(workflow='does_not_exist', instance='123',
                          force=True)
        command = Pause()
        command.prepare(options)

        response = QueryResponse(tokens=[[]])
        client = mock.Mock()
        client.query.return_value = response

        output = command.execute(client, None)

        query = Query(namePrefix='/workflow/does_not_exist/123/')
        query_request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(query_request)
        self.assertEqual('workflow does_not_exist instance 123 not found\n',
                         output)

    def test_pause_workflow(self):
        Options = collections.namedtuple('args', 'workflow, instance, force')
        options = Options(workflow='some_workflow', instance='123', force=True)
        command = Pause()
        command.prepare(options)

        token = Token(version=1234567,
                      name='/workflow/some_workflow/123/job/waiting/some_job',
                      data='some_data')
        original_token = copy.copy(token)
        query_response = QueryResponse(tokens=[[token]])
        client = mock.Mock()
        client.query.return_value = query_response
        client.modify.return_value = ModifyResponse()

        output = command.execute(client, None)

        query = Query(namePrefix='/workflow/some_workflow/123/')
        query_request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(query_request)
        self.assertEqual(1, client.modify.call_count)
        modify_request = client.modify.call_args[0][0]
        self.assertEqual(1, len(modify_request.updates))
        owned_token = modify_request.updates[0]
        self.assertEqual(owned_token.name, original_token.name)
        self.assertEqual(owned_token.version, original_token.version)
        self.assertTrue(owned_token.owner.startswith('workflow_util'))
        self.assertEqual(sys.maxint, owned_token.expirationTime)
        self.assertEqual('claimed 1 token(s) in 1 tries\n', output)


class ResumeTestCase(unittest.TestCase):
    def test_resume_non_existent(self):
        Options = collections.namedtuple('args', 'workflow, instance, force')
        options = Options(workflow='does_not_exist', instance='123',
                          force=True)
        command = Resume()
        command.prepare(options)

        response = QueryResponse(tokens=[[]])
        client = mock.Mock()
        client.query.return_value = response

        output = command.execute(client, None)

        query = Query(namePrefix='/workflow/does_not_exist/123/')
        query_request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(query_request)
        self.assertEqual('workflow does_not_exist instance 123 not found\n',
                         output)

    def test_resume_workflow(self):
        Options = collections.namedtuple('args', 'workflow, instance, force')
        options = Options(workflow='some_workflow', instance='123', force=True)
        command = Resume()
        command.prepare(options)

        token = Token(version=1234567,
                      name='/workflow/some_workflow/123/job/waiting/some_job',
                      data='some_data',
                      owner='some_owner',
                      expirationTime=sys.maxint)
        query_response = QueryResponse(tokens=[[token]])
        client = mock.Mock()
        client.query.return_value = query_response
        client.modify.return_value = ModifyResponse()

        output = command.execute(client, None)

        query = Query(namePrefix='/workflow/some_workflow/123/')
        query_request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(query_request)
        updated_token = copy.copy(token)
        updated_token.owner = None
        updated_token.expirationTime = None
        modify_request = ModifyRequest(updates=[updated_token])
        client.modify.assert_called_once_with(modify_request)
        self.assertEqual('released ownership of 1 token(s)\n', output)


class RetryTestCase(unittest.TestCase):
    def setUp(self):
        self._signal_token = Token(
            name='/workflow/some_workflow/123/__SIGNAL__/ARCHIVE')

        succeeded_job = ShellJob()
        succeeded_job.history = [ExecutionRecord(exit_code=0)]
        self._succeeded_job_token = Token(
            name='/workflow/some_workflow/123/job/waiting/succeeded',
            data=pickle.dumps(succeeded_job))

        failed_job = ShellJob()
        failed_job.history = [ExecutionRecord(exit_code=1)]
        self._failed_job_token = Token(
            name='/workflow/some_workflow/123/job/waiting/failed',
            priority=10,
            data=pickle.dumps(failed_job))

    @mock.patch('pinball.tools.workflow_util._check_workflow_instances')
    def test_retry_non_existent(self, check_workflow_instances_mock):
        Options = collections.namedtuple('args', 'workflow, instance, force')
        options = Options(workflow='does_not_exist', instance='123',
                          force=True)
        command = Retry()
        command.prepare(options)

        response = GroupResponse()
        client = mock.Mock()
        client.group.return_value = response

        check_workflow_instances_mock.return_value = True

        store = mock.Mock()
        store.read_archived_tokens.return_value = []

        output = command.execute(client, store)

        group_request = GroupRequest(
            namePrefix='/workflow/does_not_exist/123/job/', groupSuffix='/')
        client.group.assert_called_once_with(group_request)
        self.assertEqual('workflow does_not_exist instance 123 not found\n',
                         output)

    @mock.patch('pinball.tools.workflow_util._check_workflow_instances')
    def test_retry_active_workflow(self, check_workflow_instances_mock):
        Options = collections.namedtuple('args', 'workflow, instance, force')
        options = Options(workflow='some_workflow', instance='123', force=True)
        command = Retry()
        command.prepare(options)
        check_workflow_instances_mock.return_value = True

        client = mock.Mock()

        group_response = GroupResponse(
            counts={'/workflow/some_workflow/123/job': 2})
        client.group.return_value = group_response

        query_response = QueryResponse(tokens=[
            [self._signal_token],
            [self._succeeded_job_token, self._failed_job_token]])
        client.query.return_value = query_response

        modify_response = ModifyResponse(updates=[self._failed_job_token])
        client.modify.return_value = modify_response

        def side_effect(request):
            self.assertEqual([self._signal_token, self._failed_job_token],
                             request.deletes)

            failed_job_token = copy.copy(self._failed_job_token)
            failed_job_token.name = (
                '/workflow/some_workflow/123/job/runnable/failed')
            self.assertEqual(1, len(request.updates))
            _cmp_job_tokens(self, failed_job_token, request.updates[0])
            # self.assertEqual(failed_job_token, request.updates[0])

        client.modify.side_effect = side_effect

        output = command.execute(client, None)

        group_request = GroupRequest(
            namePrefix='/workflow/some_workflow/123/job/', groupSuffix='/')
        client.group.assert_called_once_with(group_request)

        signal_query = Query(
            namePrefix='/workflow/some_workflow/123/__SIGNAL__/ARCHIVE')
        job_query = Query(
            namePrefix='/workflow/some_workflow/123/job/waiting/')
        query_request = QueryRequest(queries=[signal_query, job_query])
        client.query.assert_called_once_with(query_request)

        self.assertEqual('retried 1 job(s) in workflow some_workflow instance '
                         '123\n', output)

    @mock.patch('pinball.tools.workflow_util.'
                'get_unique_workflow_instance')
    @mock.patch('pinball.tools.workflow_util._check_workflow_instances')
    def test_retry_archived_workflow(self, check_workflow_instances_mock,
                                     get_unique_workflow_instance_mock):
        Options = collections.namedtuple('args', 'workflow, instance, force')
        options = Options(workflow='some_workflow', instance='123', force=True)
        command = Retry()
        command.prepare(options)

        client = mock.Mock()

        group_response = GroupResponse()
        client.group.return_value = group_response

        check_workflow_instances_mock.return_value = True
        get_unique_workflow_instance_mock.return_value = '321'

        store = mock.Mock()
        store.read_archived_tokens.return_value = [self._signal_token,
                                                   self._succeeded_job_token,
                                                   self._failed_job_token]

        succeeded_job_token = copy.copy(self._succeeded_job_token)
        succeeded_job_token.name = (
            '/workflow/some_workflow/123/job/waiting/succeeded')
        failed_job_token = copy.copy(self._failed_job_token)
        failed_job_token.name = (
            '/workflow/some_workflow/123/job/runnable/failed')
        modify_response = ModifyResponse(updates=[succeeded_job_token,
                                                  failed_job_token])
        client.modify.return_value = modify_response

        output = command.execute(client, store)

        group_request = GroupRequest(
            namePrefix='/workflow/some_workflow/123/job/',
            groupSuffix='/')
        client.group.assert_called_once_with(group_request)

        store.read_archived_tokens.assert_called_once_with(
            name_prefix='/workflow/some_workflow/123/')

        succeeded_job_token.name = (
            '/workflow/some_workflow/321/job/waiting/succeeded')
        failed_job_token = copy.copy(self._failed_job_token)
        failed_job_token.name = (
            '/workflow/some_workflow/321/job/runnable/failed')
        modify_request = ModifyRequest(updates=[succeeded_job_token,
                                                failed_job_token])
        client.modify.assert_called_once_with(modify_request)

        self.assertEqual('retried workflow some_workflow instance 123.  Its '
                         'tokens are under /workflow/some_workflow/321/\n',
                         output)


class RedoTestCase(unittest.TestCase):
    @mock.patch('pinball.tools.workflow_util.get_unique_name')
    @mock.patch('time.time')
    def test_redo_non_existent(self, time_mock, get_unique_name_mock):
        get_unique_name_mock.return_value = 'some_owner'
        time_mock.return_value = 10
        Options = collections.namedtuple('args', 'workflow, instance, jobs, '
                                         'execution, force')
        options = Options(workflow='does_not_exist', instance='123',
                          jobs='some_job', execution='1', force=True)
        command = Redo()
        command.prepare(options)

        response = QueryAndOwnResponse()
        client = mock.Mock()
        client.query_and_own.return_value = response

        output = command.execute(client, None)

        query = Query(
            namePrefix='/workflow/does_not_exist/123/job/waiting/some_job')
        request = QueryAndOwnRequest(owner='some_owner',
                                     expirationTime=(10 + 60), query=query)
        client.query_and_own.assert_called_once_with(request)
        self.assertEqual('workflow must be running, the job must be finished '
                         'and it cannot be runnable', output)

    @staticmethod
    def _get_waiting_job_token():
        job = ShellJob()
        execution_record_with_events = ExecutionRecord()
        execution_record_with_events.events = [Event()]
        job.history = [ExecutionRecord(), execution_record_with_events]
        return Token(
            name='/workflow/some_workflow/123/job/waiting/some_job',
            data=pickle.dumps(job))

    @staticmethod
    def _get_runnable_job_token(waiting_job_token):
        job = pickle.loads(waiting_job_token.data)
        job.events = job.history[1].events
        return Token(name='/workflow/some_workflow/123/job/runnable/some_job',
                     data=pickle.dumps(job))

    @mock.patch('pinball.tools.workflow_util.get_unique_name')
    @mock.patch('time.time')
    def test_redo(self, time_mock, get_unique_name_mock):
        get_unique_name_mock.return_value = 'some_owner'
        time_mock.return_value = 10
        Options = collections.namedtuple('args', 'workflow, instance, jobs, '
                                         'execution, force')
        options = Options(workflow='some_workflow', instance='123',
                          jobs='some_job', execution='1', force=True)
        command = Redo()
        command.prepare(options)

        client = mock.Mock()
        waiting_job = RedoTestCase._get_waiting_job_token()
        query_and_own_response = QueryAndOwnResponse(tokens=[waiting_job])
        client.query_and_own.return_value = query_and_own_response

        runnable_job = RedoTestCase._get_runnable_job_token(waiting_job)
        modify_response = ModifyResponse(updates=[runnable_job])
        client.modify.return_value = modify_response

        output = command.execute(client, None)

        query = Query(
            namePrefix='/workflow/some_workflow/123/job/waiting/some_job')
        query_and_own_request = QueryAndOwnRequest(
            owner='some_owner', expirationTime=(10 + 60), query=query)
        client.query_and_own.assert_called_once_with(query_and_own_request)

        modify_request = ModifyRequest(updates=[runnable_job],
                                       deletes=[waiting_job])
        client.modify.assert_called_once_with(modify_request)

        self.assertEqual('redoing execution 1 of job some_job in workflow '
                         'some_workflow instance 123\n', output)


class PoisonTestCase(unittest.TestCase):
    def setUp(self):
        self._signal_token = Token(
            name='/workflow/some_workflow/123/__SIGNAL__/ARCHIVE')

        parent_job = ShellJob(name='parent', inputs=['__WORKFLOW_START__'],
                              outputs=['child'])
        self._parent_job_token = Token(
            name='/workflow/some_workflow/123/job/waiting/parent',
            data=pickle.dumps(parent_job))

        child_job = ShellJob(name='child', inputs=['parent'])
        self._child_job_token = Token(
            name='/workflow/some_workflow/123/job/waiting/child',
            data=pickle.dumps(child_job))

        PinballConfig.PARSER_PARAMS = {'key': 'value'}

    def test_poison_non_existent(self):
        Options = collections.namedtuple(
            'args', 'workflow, instance, jobs, force')
        options = Options(workflow='does_not_exist', instance='123',
                          jobs='parent', force=True)
        command = Poison()
        command.prepare(options)
        client = mock.Mock()

        group_response = GroupResponse(counts={})
        client.group.return_value = group_response

        store = mock.Mock()
        store.read_archived_tokens.return_value = []

        output = command.execute(client, store)

        group_request = GroupRequest(
            namePrefix='/workflow/does_not_exist/123/job/',
            groupSuffix='/')
        client.group.assert_called_once_with(group_request)

        store.read_archived_tokens.assert_called_with(
            name_prefix='/workflow/does_not_exist/123/')
        self.assertEqual('workflow does_not_exist instance 123 not found\n',
                         output)

    def test_poison_from_store(self):
        Options = collections.namedtuple(
            'args', 'workflow, instance, jobs, force')
        options = Options(workflow='some_workflow', instance='123',
                          jobs='parent', force=True)
        command = Poison()
        command.prepare(options)
        client = mock.Mock()

        store = mock.Mock()
        store.read_archived_tokens.return_value = [self._parent_job_token,
                                                   self._child_job_token]

        group_response = GroupResponse(counts={})
        client.group.return_value = group_response

        modify_response = ModifyResponse(updates=[self._parent_job_token,
                                                  self._child_job_token])
        client.modify.return_value = modify_response

        def side_effect(request):
            # Expect two job tokens and one event token.
            self.assertEqual(3, len(request.updates))
            self.assertIsNone(request.deletes)

        client.modify.side_effect = side_effect

        output = command.execute(client, store)

        group_request = GroupRequest(
            namePrefix='/workflow/some_workflow/123/job/',
            groupSuffix='/')
        client.group.assert_called_once_with(group_request)

        store.read_archived_tokens.assert_called_with(
            name_prefix='/workflow/some_workflow/123/')
        self.assertTrue(output.startswith("poisoned workflow some_workflow "
                                          "roots ['parent']."))

    @mock.patch('pinball.tools.workflow_util.Analyzer')
    def test_poison_from_client(self, analyzer_mock):
        Options = collections.namedtuple(
            'args', 'workflow, instance, jobs, force')
        options = Options(workflow='some_workflow', instance='123',
                          jobs='parent', force=True)
        command = Poison()
        command.prepare(options)
        client = mock.Mock()

        analyzer = mock.Mock()
        analyzer_mock.from_client.return_value = analyzer
        analyzer.get_tokens.return_value = [self._parent_job_token,
                                            self._child_job_token]

        event_token = Token(
            version=1234567,
            name='/workflow/some_workflow/123/input/some_job/some_input',
            data='some_data')
        analyzer.get_new_event_tokens.return_value = [event_token]

        group_response = GroupResponse(
            counts={'/workflow/some_workflow/123/job/': 1})
        client.group.return_value = group_response

        query_response = QueryResponse(tokens=[[self._signal_token]])
        client = mock.Mock()
        client.query.return_value = query_response

        modify_response = ModifyResponse(updates=[event_token])
        client.modify.return_value = modify_response

        output = command.execute(client, None)

        group_request = GroupRequest(
            namePrefix='/workflow/some_workflow/123/job/',
            groupSuffix='/')
        client.group.assert_called_once_with(group_request)
        query = Query(
            namePrefix='/workflow/some_workflow/123/__SIGNAL__/ARCHIVE')
        query_request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(query_request)
        modify_request = ModifyRequest(updates=[event_token],
                                       deletes=[self._signal_token])
        client.modify.assert_called_once_with(modify_request)

        self.assertEqual(1, analyzer.get_tokens.call_count)
        self.assertEqual(1, analyzer.get_new_event_tokens.call_count)

        self.assertEqual("poisoned workflow some_workflow instance 123 roots "
                         "['parent']\n", output)

    @mock.patch('pinball.tools.workflow_util.Analyzer')
    def test_poison_from_config(self, analyzer_mock):
        Options = collections.namedtuple(
            'args', 'workflow, instance, jobs, force')
        options = Options(workflow='some_workflow', instance=None,
                          jobs='parent', force=True)
        command = Poison()
        command.prepare(options)

        analyzer = mock.Mock()
        analyzer_mock.from_parser_params.return_value = analyzer
        analyzer.get_tokens.return_value = [self._parent_job_token,
                                            self._child_job_token]

        client = mock.Mock()
        modify_response = ModifyResponse(updates=[self._parent_job_token,
                                                  self._child_job_token])
        client.modify.return_value = modify_response
        store = mock.Mock()

        output = command.execute(client, store)

        analyzer_mock.from_parser_params.assert_called_once_with('some_workflow')
        self.assertEqual(2, analyzer.get_tokens.call_count)
        self.assertEqual(1, analyzer.clear_job_histories.call_count)
        analyzer.poison.assert_called_once_with(['parent'])
        self.assertEqual(1, analyzer.change_instance.call_count)
        modify_request = ModifyRequest(updates=[self._parent_job_token,
                                                self._child_job_token])
        client.modify.assert_called_once_with(modify_request)
        self.assertTrue(output.startswith("poisoned workflow some_workflow "
                                          "roots ['parent']."))


class ModifySignalTestCase(unittest.TestCase):
    def _init(self, command, action):
        self._command = command
        self._action = action

    def _run_test(self, is_present, expect_set, expect_remove, expected_output,
                  signaller_mock):
        Options = collections.namedtuple('args', 'workflow, instance, force')
        options = Options(workflow='some_workflow', instance='123', force=True)
        self._command.prepare(options)

        signaller_mock.is_signal_present.return_value = is_present

        output = self._command.execute(mock.Mock(), None)
        self.assertEqual(expected_output, output)

        if expect_set:
            signaller_mock.set_action.assert_called_with(self._action)

        if expect_remove:
            signaller_mock.remove_action.assert_called_with(self._action)


class DrainTestCase(ModifySignalTestCase):
    def setUp(self):
        self._init(Drain(), Signal.DRAIN)

    @mock.patch('pinball.tools.workflow_util.Signaller')
    def test_set_present(self, signaller_mock):
        signaller = mock.Mock()
        signaller_mock.return_value = signaller
        expected_output = ('DRAIN has been already set.  Not changing '
                           'anything this time\n')
        self._run_test(True, False, False, expected_output, signaller)

    @mock.patch('pinball.tools.workflow_util.Signaller')
    def test_set_missing(self, signaller_mock):
        signaller = mock.Mock()
        signaller_mock.return_value = signaller
        expected_output = ('set DRAIN.  Its token is '
                           '/workflow/some_workflow/123/__SIGNAL__/DRAIN\n')
        self._run_test(False, True, False, expected_output, signaller)


class UnDrainTestCase(ModifySignalTestCase):
    def setUp(self):
        self._init(UnDrain(), Signal.DRAIN)

    @mock.patch('pinball.tools.workflow_util.Signaller')
    def test_remove_present(self, signaller_mock):
        signaller = mock.Mock()
        signaller_mock.return_value = signaller
        expected_output = ('removed DRAIN from '
                           '/workflow/some_workflow/123/__SIGNAL__/DRAIN\n')
        self._run_test(True, False, True, expected_output, signaller)

    @mock.patch('pinball.tools.workflow_util.Signaller')
    def test_remove_missing(self, signaller_mock):
        signaller = mock.Mock()
        signaller_mock.return_value = signaller
        expected_output = ('DRAIN has been already removed.  Not changing '
                           'anything this time\n')
        self._run_test(False, False, False, expected_output, signaller)


class AbortTestCase(ModifySignalTestCase):
    def setUp(self):
        self._init(Abort(), Signal.ABORT)

    @mock.patch('pinball.tools.workflow_util.Signaller')
    def test_set_present(self, signaller_mock):
        signaller = mock.Mock()
        signaller_mock.return_value = signaller
        expected_output = ('ABORT has been already set.  Not changing '
                           'anything this time\n')
        self._run_test(True, False, False, expected_output, signaller)

    @mock.patch('pinball.tools.workflow_util.Signaller')
    def test_set_missing(self, signaller_mock):
        signaller = mock.Mock()
        signaller_mock.return_value = signaller
        expected_output = ('set ABORT.  Its token is '
                           '/workflow/some_workflow/123/__SIGNAL__/ABORT\n')
        self._run_test(False, True, False, expected_output, signaller)


class UnAbortTestCase(ModifySignalTestCase):
    def setUp(self):
        self._init(UnAbort(), Signal.ABORT)

    @mock.patch('pinball.tools.workflow_util.Signaller')
    def test_remove_present(self, signaller_mock):
        signaller = mock.Mock()
        signaller_mock.return_value = signaller
        expected_output = ('removed ABORT from '
                           '/workflow/some_workflow/123/__SIGNAL__/ABORT\n')
        self._run_test(True, False, True, expected_output, signaller)

    @mock.patch('pinball.tools.workflow_util.Signaller')
    def test_remove_missing(self, signaller_mock):
        signaller = mock.Mock()
        signaller_mock.return_value = signaller
        expected_output = ('ABORT has been already removed.  Not changing '
                           'anything this time\n')
        self._run_test(False, False, False, expected_output, signaller)


class ExitTestCase(ModifySignalTestCase):
    def setUp(self):
        self._init(Exit(), Signal.EXIT)

    @mock.patch('pinball.tools.workflow_util.Signaller')
    def test_set_present(self, signaller_mock):
        signaller = mock.Mock()
        signaller_mock.return_value = signaller
        expected_output = ('EXIT has been already set.  Not changing '
                           'anything this time\n')
        self._run_test(True, False, False, expected_output, signaller)

    @mock.patch('pinball.tools.workflow_util.Signaller')
    def test_set_missing(self, signaller_mock):
        signaller = mock.Mock()
        signaller_mock.return_value = signaller
        expected_output = ('set EXIT.  Its token is '
                           '/workflow/some_workflow/123/__SIGNAL__/EXIT\n')
        self._run_test(False, True, False, expected_output, signaller)


class UnExitTestCase(ModifySignalTestCase):
    def setUp(self):
        self._init(UnExit(), Signal.EXIT)

    @mock.patch('pinball.tools.workflow_util.Signaller')
    def test_remove_present(self, signaller_mock):
        signaller = mock.Mock()
        signaller_mock.return_value = signaller
        expected_output = ('removed EXIT from '
                           '/workflow/some_workflow/123/__SIGNAL__/EXIT\n')
        self._run_test(True, False, True, expected_output, signaller)

    @mock.patch('pinball.tools.workflow_util.Signaller')
    def test_remove_missing(self, signaller_mock):
        signaller = mock.Mock()
        signaller_mock.return_value = signaller
        expected_output = ('EXIT has been already removed.  Not changing '
                           'anything this time\n')
        self._run_test(False, False, False, expected_output, signaller)


class ReScheduleTestCase(unittest.TestCase):
    @mock.patch('pinball.parser.utils.load_path')
    def test_reschdule_non_existent(self, load_path_mock):
        Options = collections.namedtuple('args',
                                         'workflow,force')
        options = Options(workflow='does_not_exist', force=True)
        PinballConfig.PARSER_PARAMS = {'key': 'value'}
        command = ReSchedule()
        command.prepare(options)

        config_parser = mock.Mock()

        def load_path(params):
            self.assertEqual(['caller', 'key'], sorted(params.keys()))
            return config_parser
        load_path_mock.return_value = load_path
        config_parser.get_workflow_names.return_value = []

        client = mock.Mock()
        output = command.execute(client, None)

        self.assertEqual('workflow does_not_exist not found\n', output)

    @mock.patch('pinball.parser.utils.load_path')
    def test_reschedule_workflow(self, load_path_mock):
        Options = collections.namedtuple('args',
                                         'workflow,force')
        options = Options(workflow='some_workflow', force=True)
        PinballConfig.PARSER_PARAMS = {'key': 'value'}
        command = ReSchedule()
        command.prepare(options)

        config_parser = mock.Mock()

        def load_path(params):
            self.assertEqual(['caller', 'key'], sorted(params.keys()))
            return config_parser
        load_path_mock.return_value = load_path
        config_parser.get_workflow_names.return_value = ['some_workflow']
        schedule = WorkflowSchedule(next_run_time=10,
                                    recurrence_seconds=10,
                                    workflow='some_workflow',
                                    emails=None)
        old_token = Token(version=1234567,
                          name='/schedule/workflow/some_workflow',
                          data=pickle.dumps(schedule),
                          owner='some_owner',
                          expirationTime=sys.maxint)
        new_token = copy.copy(old_token)
        schedule.recurrence_seconds = 100
        new_token.data = pickle.dumps(schedule)
        config_parser.get_schedule_token.return_value = new_token
        query_response = QueryResponse(tokens=[[old_token]])
        client = mock.Mock()
        client.query.return_value = query_response
        client.modify.return_value = ModifyResponse()

        output = command.execute(client, None)

        query = Query(namePrefix='/schedule/workflow/some_workflow')
        query_request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(query_request)
        modify_request = ModifyRequest(updates=[new_token])
        client.modify.assert_called_once_with(modify_request)
        self.assertEqual("rescheduled workflows ['some_workflow'].  Their "
                         "schedule tokens are "
                         "['/schedule/workflow/some_workflow']\n", output)

    @mock.patch('pinball.parser.utils.load_path')
    def test_reschedule_workflows(self, load_path_mock):
        Options = collections.namedtuple('args',
                                         'workflow, force')
        options = Options(workflow=None, force=True)
        PinballConfig.PARSER_PARAMS = {'key': 'value'}
        command = ReSchedule()
        command.prepare(options)

        config_parser = mock.Mock()

        def load_path(params):
            self.assertEqual(['caller', 'key'], sorted(params.keys()))
            return config_parser
        load_path_mock.return_value = load_path

        config_parser.get_workflow_names.return_value = ['some_workflow',
                                                         'some_other_workflow']

        schedule = WorkflowSchedule(next_run_time=10,
                                    recurrence_seconds=10,
                                    workflow='some_workflow',
                                    emails=None)
        old_token = Token(version=1234567,
                          name='/schedule/workflow/some_workflow',
                          data=pickle.dumps(schedule),
                          owner='some_owner',
                          expirationTime=sys.maxint)
        new_token = copy.copy(old_token)
        schedule.recurrence_seconds = 100
        new_token.data = pickle.dumps(schedule)

        schedule.workflow = 'some_other_workflow'
        other_old_token = Token(version=12345678,
                                name='/schedule/workflow/some_other_workflow',
                                data=pickle.dumps(schedule),
                                owner='some_owner',
                                expirationTime=sys.maxint)
        other_new_token = copy.copy(other_old_token)
        other_new_token.data = pickle.dumps(schedule)

        def get_schedule_token(workflow):
            if workflow == 'some_workflow':
                return new_token
            elif workflow == 'some_other_workflow':
                return other_new_token
        config_parser.get_schedule_token = get_schedule_token

        query_response = QueryResponse(tokens=[[old_token], [other_old_token]])
        client = mock.Mock()
        client.query.return_value = query_response
        client.modify.return_value = ModifyResponse()

        output = command.execute(client, None)

        query = Query(namePrefix='/schedule/workflow/some_workflow')
        other_query = Query(
            namePrefix='/schedule/workflow/some_other_workflow')
        query_request = QueryRequest(queries=[query, other_query])
        client.query.assert_called_once_with(query_request)
        # other_new_token is not in the updates because the schedule of
        # some_other_workflow doesn't change.
        modify_request = ModifyRequest(updates=[new_token])
        client.modify.assert_called_once_with(modify_request)
        self.assertEqual("rescheduled workflows ['some_workflow'].  Their "
                         "schedule tokens are "
                         "['/schedule/workflow/some_workflow']\n", output)


class UnScheduleTestCase(unittest.TestCase):
    def test_unschdule_non_existent(self):
        Options = collections.namedtuple('args', 'workflow,force')
        options = Options(workflow='does_not_exist', force=True)
        command = UnSchedule()
        command.prepare(options)

        query_response = QueryResponse(tokens=[[]])
        client = mock.Mock()
        client.query.return_value = query_response

        output = command.execute(client, None)

        query = Query(namePrefix='/schedule/workflow/does_not_exist')
        query_request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(query_request)
        self.assertEqual('schedule for workflow does_not_exist not found\n',
                         output)

    def test_reschedule_workflow(self):
        Options = collections.namedtuple('args', 'workflow,force')
        options = Options(workflow='some_workflow', force=True)
        command = UnSchedule()
        command.prepare(options)

        token = Token(version=1234567,
                      name='/schedule/workflow/some_workflow',
                      data='some_data',
                      owner='some_owner',
                      expirationTime=sys.maxint)
        query_response = QueryResponse(tokens=[[token]])
        client = mock.Mock()
        client.query.return_value = query_response
        client.modify.return_value = ModifyResponse()

        output = command.execute(client, None)

        query = Query(namePrefix='/schedule/workflow/some_workflow')
        query_request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(query_request)
        modify_request = ModifyRequest(deletes=[token])
        client.modify.assert_called_once_with(modify_request)
        self.assertEqual('removed schedule for workflow some_workflow\n',
                         output)


class ReloadTestCase(unittest.TestCase):
    def setUp(self):
        not_owned_job = ShellJob(max_attempts=5)
        not_owned_job.history = [ExecutionRecord(exit_code=0)]
        self._not_owned_job_token = Token(
            name='/workflow/some_workflow/123/job/runnable/not_owned',
            priority=10,
            data=pickle.dumps(not_owned_job))

        not_owned_job.max_attempts = 6
        self._new_not_owned_job_token = Token(
            name='/workflow/some_workflow/123/job/runnable/not_owned',
            priority=11,
            data=pickle.dumps(not_owned_job))

        not_owned_job.history = []
        self._not_owned_job_def_token = Token(
            name='/workflow/some_workflow/123/job/waiting/not_owned',
            priority=11,
            data=pickle.dumps(not_owned_job))

        owned_job = ShellJob()
        self._owned_job_token = Token(
            name='/workflow/some_workflow/123/job/runnable/owned',
            owner='some_owner',
            expirationTime=1000,
            data=pickle.dumps(owned_job))

        self._owned_job_def_token = Token(
            name='/workflow/some_workflow/123/job/waiting/owned',
            data=pickle.dumps(owned_job))

        PinballConfig.PARSER_PARAMS = {'key': 'value'}

    @mock.patch('pinball.parser.utils.load_path')
    def test_reload_non_existent(self, load_path_mock):
        Options = collections.namedtuple(
            'args', 'workflow, instance, jobs, force')
        options = Options(workflow='does_not_exist',
                          instance='123',
                          jobs=None,
                          force=True)
        PinballConfig.PARSER_PARAMS = {'key': 'value'}
        command = Reload()
        command.prepare(options)

        config_parser = mock.Mock()

        def load_path(params):
            self.assertEqual(['caller', 'key'], sorted(params.keys()))
            return config_parser
        load_path_mock.return_value = load_path
        config_parser.get_workflow_names.return_value = []

        client = mock.Mock()
        output = command.execute(client, None)

        self.assertEqual('workflow does_not_exist not found in %s\n' %
                         str(PinballConfig.PARSER_PARAMS), output)

    @mock.patch('pinball.tools.workflow_util.time')
    @mock.patch('pinball.parser.utils.load_path')
    def test_reload_owned_job(self, load_path_mock, time):
        Options = collections.namedtuple(
            'args', 'workflow, instance, jobs, force')
        options = Options(workflow='some_workflow',
                          instance='123',
                          jobs='owned',
                          force=True)
        PinballConfig.PARSER_PARAMS = {'key': 'value'}
        command = Reload()
        command.prepare(options)

        config_parser = mock.Mock()

        def load_path(params):
            self.assertEqual(['caller', 'key'], sorted(params.keys()))
            return config_parser
        load_path_mock.return_value = load_path
        config_parser.get_workflow_names.return_value = ['some_workflow']

        time.time.return_value = 10

        client = mock.Mock()
        client.query_and_own.return_value = QueryAndOwnResponse(tokens=[])

        output = command.execute(client, None)

        runnable_query = Query(
            namePrefix='/workflow/some_workflow/123/job/runnable/owned',
            maxTokens=1)
        runnable_request = QueryAndOwnRequest(owner='workflow_util',
                                              expirationTime=(10 + 5 * 60),
                                              query=runnable_query)

        waiting_query = Query(
            namePrefix='/workflow/some_workflow/123/job/waiting/owned',
            maxTokens=1)
        waiting_request = QueryAndOwnRequest(owner='workflow_util',
                                             expirationTime=(10 + 5 * 60),
                                             query=waiting_query)
        self.assertEqual([mock.call(runnable_request),
                          mock.call(waiting_request)],
                         client.query_and_own.call_args_list)

        self.assertEqual('job owned in workflow some_workflow instance 123 '
                         'either not found or already owned', output)

    @staticmethod
    def _check_modify_request(expected_request):
        """Verify a modify request.

        We cannot simply compare the exact content of the tokens passed in the
        modify request as pickling could have distorted it.

        Args:
            expected_request: The request to compare with.
        Returns:
            Function with the same interface as TokenMasterService.modify()
        """
        def check_request(request):
            assert not expected_request.deletes and not request.deletes
            assert expected_request.updates and not request.updates
            assert len(expected_request.updates) == len(request.updates)
            for i in range(0, len(expected_request.updates)):
                assert (str(expected_request.updates[i]) ==
                        str(request.updates[i]))
        return check_request

    @mock.patch('pinball.tools.workflow_util.time')
    @mock.patch('pinball.parser.utils.load_path')
    def test_reload_not_owned_job(self, load_path_mock, time):
        Options = collections.namedtuple(
            'args', 'workflow, instance, jobs, force')
        options = Options(workflow='some_workflow',
                          instance='123',
                          jobs='not_owned',
                          force=True)
        PinballConfig.PARSER_PARAMS = {'key': 'value'}
        command = Reload()
        command.prepare(options)

        config_parser = mock.Mock()

        def load_path(params):
            self.assertEqual(['caller', 'key'], sorted(params.keys()))
            return config_parser
        load_path_mock.return_value = load_path
        config_parser.get_workflow_names.return_value = ['some_workflow']

        event_token = Token(name=('/workflow/some_workflow/123/input/'
                                  'not_owned_job/__WORKFLOW_START__'),
                            data='some_data')
        config_parser.get_workflow_tokens.return_value = [
            self._not_owned_job_def_token,
            self._owned_job_def_token,
            event_token]

        time.time.return_value = 10

        client = mock.Mock()

        not_owned_job_token = copy.copy(self._not_owned_job_token)
        not_owned_job_token.owner = 'workflow_util'
        not_owned_job_token.expirationTime = 10 + 5 * 60
        client.query_and_own.return_value = QueryAndOwnResponse(
            tokens=[not_owned_job_token])

        client.modify.return_value = ModifyResponse(
            updates=[self._new_not_owned_job_token])

        output = command.execute(client, None)

        config_parser.get_workflow_tokens.assert_called_once_with(
            'some_workflow')

        runnable_query = Query(
            namePrefix='/workflow/some_workflow/123/job/runnable/not_owned',
            maxTokens=1)
        runnable_request = QueryAndOwnRequest(owner='workflow_util',
                                              expirationTime=(10 + 5 * 60),
                                              query=runnable_query)
        client.query_and_own.assert_called_once_with(runnable_request)

        new_not_owned_job_token = copy.copy(self._new_not_owned_job_token)
        new_not_owned_job_token.name = ('/workflow/some_workflow/123/job/'
                                        'runnable/not_owned')
        client.modify.side_effect = ReloadTestCase._check_modify_request(
            ModifyRequest(updates=[new_not_owned_job_token], deletes=None))

        self.assertEqual("reloaded jobs ['not_owned'] in workflow "
                         "some_workflow instance 123", output)

    @mock.patch('pinball.tools.workflow_util.time')
    @mock.patch('pinball.parser.utils.load_path')
    def test_reload_all_jobs(self, load_path_mock, time):
        Options = collections.namedtuple(
            'args', 'workflow, instance, jobs, force')
        options = Options(workflow='some_workflow',
                          instance='123',
                          jobs=None,
                          force=True)
        PinballConfig.PARSER_PARAMS = {'key': 'value'}
        command = Reload()
        command.prepare(options)

        config_parser = mock.Mock()

        def load_path(params):
            self.assertEqual(['caller', 'key'], sorted(params.keys()))
            return config_parser
        load_path_mock.return_value = load_path
        config_parser.get_workflow_names.return_value = ['some_workflow']

        time.time.return_value = 10

        client = mock.Mock()
        client.group.return_value = GroupResponse(counts={
            '/workflow/some_workflow/123/job/runnable': 1,
            '/workflow/some_workflow/123/job/waiting': 1})

        not_owned_job_token = copy.copy(self._not_owned_job_token)
        not_owned_job_token.owner = 'workflow_util'
        not_owned_job_token.expirationTime = 10 + 5 * 60
        client.query_and_own.return_value = QueryAndOwnResponse(
            tokens=[not_owned_job_token])

        client.modify.return_value = ModifyResponse(
            updates=[self._not_owned_job_token])

        output = command.execute(client, None)

        group_request = GroupRequest(
            namePrefix='/workflow/some_workflow/123/job/',
            groupSuffix='/')
        client.group.assert_called_once_with(group_request)

        query = Query(namePrefix='/workflow/some_workflow/123/job/')
        query_and_own_request = QueryAndOwnRequest(
            owner='workflow_util',
            expirationTime=(10 + 5 * 60),
            query=query)
        client.query_and_own.assert_called_once_with(query_and_own_request)

        modify_request = ModifyRequest(updates=[self._not_owned_job_token])
        client.modify.assert_called_once_with(modify_request)

        self.assertEqual('only 1 out of 2 job tokens in workflow '
                         'some_workflow instance 123 could be claimed', output)


class AlterTestCase(unittest.TestCase):
    def _init(self, command, altered_parent_disabled, altered_child_disabled):
        self._command = command
        self._altered_parent_disabled = altered_parent_disabled
        self._altered_child_disabled = altered_child_disabled

    def _alter_non_existent(self, expected_output):
        Options = collections.namedtuple(
            'args', 'workflow, instance, jobs, force')
        options = Options(workflow='does_not_exist', instance='123',
                          jobs='some_job', force=True)
        self._command.prepare(options)

        client = mock.Mock()
        client.query.return_value = QueryResponse(tokens=[[]])
        store = mock.Mock()
        output = self._command.execute(client, store)

        query = Query(namePrefix='/workflow/does_not_exist/123/job/')
        query_request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(query_request)
        self.assertEqual(expected_output, output)

    def _check_modify_request(self, request):
        self.assertEqual(2, len(request.updates))
        parent_token = request.updates[0]
        self.assertEqual('/workflow/some_workflow/123/job/waiting/parent',
                         parent_token.name)
        parent_job = pickle.loads(parent_token.data)
        self.assertEqual(self._altered_parent_disabled, parent_job.disabled)
        child_token = request.updates[1]
        self.assertEqual('/workflow/some_workflow/123/job/waiting/child',
                         child_token.name)
        child_job = pickle.loads(child_token.data)
        self.assertEqual(self._altered_child_disabled, child_job.disabled)

    def _alter_jobs(self, expected_output):
        Options = collections.namedtuple(
            'args', 'workflow, instance, jobs, force')
        options = Options(workflow='some_workflow', instance='123',
                          jobs='parent,child', force=True)
        self._command.prepare(options)

        parent_job = ShellJob(name='parent', inputs=['__WORKFLOW_START__'],
                              outputs=['child'])
        parent_job.disabled = True
        parent_job_token = Token(
            name='/workflow/some_workflow/123/job/waiting/parent',
            data=pickle.dumps(parent_job))

        altered_parent_job_token = copy.copy(parent_job_token)
        parent_job.disabled = self._altered_parent_disabled
        altered_parent_job_token.data = pickle.dumps(parent_job)

        child_job = ShellJob(name='child', inputs=['parent'])
        child_job_token = Token(
            name='/workflow/some_workflow/123/job/waiting/child',
            data=pickle.dumps(child_job))

        altered_child_job_token = copy.copy(child_job_token)
        child_job.disabled = self._altered_child_disabled
        altered_child_job_token.data = pickle.dumps(child_job)

        client = mock.Mock()
        client.query.return_value = QueryResponse(tokens=[[parent_job_token,
                                                           child_job_token]])
        client.modify.return_value = ModifyResponse(
            updates=[altered_parent_job_token, altered_child_job_token])
        client.modify.side_effect = self._check_modify_request

        store = mock.Mock()
        output = self._command.execute(client, store)

        query = Query(namePrefix='/workflow/some_workflow/123/job/')
        query_request = QueryRequest(queries=[query])
        client.query.assert_called_once_with(query_request)

        self.assertEqual(1, client.modify.call_count)

        self.assertEqual(expected_output, output)


class DisableTestCase(AlterTestCase):
    def setUp(self):
        self._init(Disable(), True, True)

    def test_alter_non_existent(self):
        self._alter_non_existent("job(s) ['some_job'] not found in the "
                                 "master.  Note that only jobs of a running "
                                 "workflow can be disabled")

    def test_alter_jobs(self):
        self._alter_jobs('disabled 2 job(s) in 1 tries\n')


class EnableTestCase(AlterTestCase):
    def setUp(self):
        self._init(Enable(), False, False)

    def test_alter_non_existent(self):
        self._alter_non_existent("job(s) ['some_job'] not found in the "
                                 "master.  Note that only jobs of a running "
                                 "workflow can be enabled")

    def test_alter_jobs(self):
        self._alter_jobs('enabled 2 job(s) in 1 tries\n')


class CleanupTestCase(unittest.TestCase):
    def setUp(self):
        # Set PinballConfig to enable s3 log saver
        PinballConfig.S3_LOGS_DIR_PREFIX = 's3n://pinball/tmp/'
        PinballConfig.S3_LOGS_DIR = \
            PinballConfig.S3_LOGS_DIR_PREFIX \
            + PinballConfig.JOB_LOG_PATH_PREFIX

    @mock.patch('pinball.tools.workflow_util.DataBuilder')
    def test_cleanup_non_existent(self, data_builder):
        Options = collections.namedtuple('args', 'age_days,force')
        options = Options(age_days=10, force=True)
        command = Cleanup()
        command.prepare(options)

        builder = mock.Mock()
        data_builder.return_value = builder
        builder.get_workflows.return_value = []

        store = mock.Mock()
        output = command.execute(None, store)

        builder.get_workflows.assert_called_once_with()
        self.assertEqual('no tokens need to be cleaned up\nremoved 0 token(s) '
                         'and 0 directory(ies)\n', output)

    @mock.patch('pinball.tools.workflow_util.delete_s3_directory')
    @mock.patch('pinball.tools.workflow_util.time')
    @mock.patch('pinball.tools.workflow_util.DataBuilder')
    @mock.patch('pinball.tools.workflow_util.shutil')
    def test_cleanup_instances(self, shutil, data_builder, time, delete_s3):
        Options = collections.namedtuple('args', 'age_days,force')
        options = Options(age_days=10, force=True)
        command = Cleanup()

        time.time.return_value = 1373181000  # 2013-07-07 07:10:00 UTC

        command.prepare(options)

        builder = mock.Mock()
        data_builder.return_value = builder

        old_workflow = WorkflowData('old_workflow')
        new_workflow = WorkflowData('new_workflow')
        builder.get_workflows.return_value = [old_workflow, new_workflow]

        old_instance = WorkflowInstanceData('old_workflow', '1234',
                                            status=Status.SUCCESS,
                                            start_time=10,
                                            end_time=20)

        new_instance = WorkflowInstanceData(
            'new_workflow', '12345',
            status=Status.SUCCESS,
            start_time=1373180880,  # 2013-07-07 07:08:00 UTC
            end_time=1373180940)  # 2013-07-07 07:09:00 UTC

        def side_effect(*args, **kwargs):
            if args[0] == old_workflow.workflow:
                return [old_instance]
            elif args[0] == new_workflow.workflow:
                return [new_instance]
            else:
                assert False, 'unknown workflow %s' % args[0]
        builder.get_instances.side_effect = side_effect

        store = mock.Mock()
        token = Token(name='some_token')
        store.read_archived_tokens.return_value = [token]

        output = command.execute(None, store)

        builder.get_workflows.assert_called_once_with()
        self.assertEqual(2, builder.get_instances.call_count)
        builder.get_instances.assert_any_call(old_workflow.workflow)
        builder.get_instances.assert_any_call(new_workflow.workflow)

        store.read_archived_tokens.assert_called_once_with(
            name_prefix='/workflow/old_workflow/1234/')
        store.delete_archived_tokens.assert_called_once_with([token])

        shutil.rmtree.assert_called_once_with(
            '/tmp/pinball_job_logs/old_workflow/1234', ignore_errors=True)

        delete_s3.assert_called_once_with(
            's3n://pinball/tmp/pinball_job_logs/old_workflow/1234')

        self.assertEqual('removed 1 token(s) and 2 directory(ies)\n', output)


class RebuildCacheTestCase(unittest.TestCase):
    @mock.patch('pinball.tools.workflow_util.DataBuilder')
    def test_rebuild_cache(self, data_builder):
        Options = collections.namedtuple('args', 'force')
        options = Options(force=True)
        command = RebuildCache()

        command.prepare(options)

        builder = mock.Mock()
        data_builder.return_value = builder
        store = mock.Mock()
        store.read_cached_data_names.return_value = [
            '/workflow/some_workflow/123',
            '/workflow/some_workflow/1234',
            '/workflow/some_workflow/12345']

        output = command.execute(None, store)

        self.assertEqual(2, store.read_cached_data_names.call_count)
        self.assertEqual(1, store.clear_cached_data.call_count)
        data_builder.assert_called_once_with(store, use_cache=True)
        self.assertEqual(1, builder.get_workflows.call_count)

        self.assertEqual('rebuilt data cache.  It now has 3 data items',
                         output)
