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

"""Populate the database with some tokens.  Useful for testing."""
import argparse
import os
import pickle
import sys
import time

from pinball.master.blessed_version import BlessedVersion
from pinball.master.master_handler import MasterHandler
from pinball.master.thrift_lib.ttypes import Token
from pinball.persistence.store import DbStore
from pinball.scheduler.schedule import WorkflowSchedule
from pinball.workflow.job import ShellJob
from pinball.workflow.job_executor import ExecutionRecord
from pinball.workflow.name import Name
from pinball.workflow.signaller import Signal


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


def _try_generate_blessed_version(store):
    blessed_version = BlessedVersion(MasterHandler._BLESSED_VERSION,
                                     MasterHandler._MASTER_OWNER)
    active_tokens = store.read_active_tokens()
    for token in active_tokens:
        if token.name == blessed_version.name:
            return
    store.commit_tokens([blessed_version])


def _generate_job_token(workflow, instance, job, executions, max_jobs):
    if job == 0:
        inputs = [Name.WORKFLOW_START_INPUT]
    else:
        inputs = ['job_%d' % (job - 1)]
    if job == max_jobs - 1:
        outputs = []
    else:
        outputs = ['job_%d' % (job + 1)]
    shell_job = ShellJob(name='job_%d' % job, inputs=inputs, outputs=outputs,
                         command='some command %d' % job)
    for e in range(0, executions):
        start_time = 1000000 * workflow + 10000 * instance + 100 * job + e + 1
        end_time = start_time + 10 * e + 1
        DIR = '/tmp/pinball/logs'
        if not os.path.exists(DIR):
            os.makedirs(DIR)
        LOG_PATTERN = '%s/%%s.%%d.%%s' % DIR
        info_log_file = LOG_PATTERN % (job, start_time, 'info')
        with open(info_log_file, 'w') as f:
            f.write('some info log of execution %d' % e)
        error_log_file = LOG_PATTERN % (job, start_time, 'error')
        with open(error_log_file, 'w') as f:
            f.write('some error log of execution %d' % e)
        record = ExecutionRecord(
            info='some_command %d some_args %d' % (e, e),
            instance='instance_%d' % instance,
            start_time=start_time,
            end_time=end_time,
            exit_code=(workflow + instance + e) % 2,
            logs={'info': info_log_file, 'error': error_log_file})
        shell_job.history.append(record)
    name = Name(workflow='workflow_%d' % workflow,
                instance='instance_%d' % instance,
                job_state=Name.WAITING_STATE,
                job='job_%d' % job)
    return Token(name=name.get_job_token_name(),
                 version=1000000 * workflow + 10000 * instance + 100 * job,
                 priority=job,
                 data=pickle.dumps(shell_job))


def _generate_workflows_tokens(offset, workflows, instances, jobs, executions):
    result = []
    for w in range(offset, offset + workflows):
        for i in range(0, instances):
            for j in range(0, jobs):
                result.append(_generate_job_token(w, i, j, executions, jobs))
    return result


def _generate_schedule_tokens(workflows):
    result = []
    for w in range(workflows):
        next_run_time = time.time() + (365 + w) * 24 * 60 * 60
        recurrence = min(365 * 24 * 60 * 60, 60 ** w)
        workflow = 'workflow_%d' % w
        schedule = WorkflowSchedule(next_run_time,
                                    recurrence_seconds=recurrence,
                                    overrun_policy=w % 4, workflow=workflow)
        name = Name(workflow=workflow)
        result.append(Token(name=name.get_workflow_schedule_token_name(),
                            version=100000000 * w,
                            owner='some_owner',
                            expirationTime=next_run_time,
                            data=pickle.dumps(schedule)))
    return result


def _generate_signal_tokens(workflows):
    result = []
    for w in range(0, workflows, 2):
        workflow = 'workflow_%d' % w
        signal = Signal(Signal.DRAIN)
        name = Name(workflow=workflow,
                    signal=Signal.action_to_string(signal.action))
        result.append(Token(name=name.get_signal_token_name(),
                            version=10000000000 * w,
                            data=pickle.dumps(signal)))
    return result


def generate_workflows(active_workflows, archived_workflows, instances, jobs,
                       executions, store):
    _try_generate_blessed_version(store)
    active_tokens = _generate_workflows_tokens(0,
                                               active_workflows,
                                               instances,
                                               jobs,
                                               executions)

    archived_tokens = _generate_workflows_tokens(active_workflows,
                                                 archived_workflows,
                                                 instances,
                                                 jobs,
                                                 executions)

    schedule_tokens = _generate_schedule_tokens(active_workflows +
                                                archived_workflows)

    signal_tokens = _generate_signal_tokens(active_workflows)

    store.commit_tokens(active_tokens + archived_tokens + schedule_tokens +
                        signal_tokens)
    store.archive_tokens(archived_tokens)


def main():
    parser = argparse.ArgumentParser(
        description='Start Pinball master server.')
    parser.add_argument('-w',
                        '--active_workflows',
                        dest='active_workflows',
                        type=int,
                        default=5,
                        help='number of active workflows to generate')
    parser.add_argument('-r',
                        '--archived_workflows',
                        dest='archived_workflows',
                        type=int,
                        default=5,
                        help='number of archived workflows to generate')
    parser.add_argument('-i',
                        '--instances',
                        dest='instances',
                        type=int,
                        default=5,
                        help='number of instances per workflow')
    parser.add_argument('-j',
                        '--jobs',
                        dest='jobs',
                        type=int,
                        default=5,
                        help='number of jobs per workflow')
    parser.add_argument('-e',
                        '--executions',
                        dest='executions',
                        type=int,
                        default=5,
                        help='number of executions per job')
    options = parser.parse_args(sys.argv[1:])

    generate_workflows(options.active_workflows,
                       options.archived_workflows,
                       options.instances,
                       options.jobs,
                       options.executions,
                       DbStore())


if __name__ == '__main__':
    main()
