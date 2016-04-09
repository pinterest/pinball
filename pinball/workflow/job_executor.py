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

"""Logic handling job execution.

In a typical case, each job class has its own executor.
"""
import abc
import atexit
import os
import select
import signal
import socket
import subprocess
import threading
import time
import traceback

from pinball.config.pinball_config import PinballConfig
from pinball.config.utils import get_log
from pinball.config.utils import timestamp_to_str
from pinball.persistence.token_data import TokenData
from pinball.workflow import log_saver
from pinball.workflow.job import ShellConditionJob
from pinball.workflow.job import ShellJob
from pinball.workflow.buffered_line_reader import BufferedLineReader
from pinball.workflow.utils import get_logs_dir


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.workflow.job_executor')


class ExecutionRecord(TokenData):
    """A data object holding information about a single job execution."""
    def __init__(self, info=None, instance=None, start_time=None,
                 end_time=None, exit_code=None, logs=None):
        self.info = info
        self.instance = instance
        self.start_time = start_time
        self.end_time = end_time
        self.exit_code = exit_code
        self.events = []
        # TODO(pawel): rename this to attributes for naming consistency.
        self.properties = {}
        self.cleanup_exit_code = None
        # Setting logs={} in the argument list is a bad idea.
        # See http://effbot.org/zone/default-values.htm for explanation.
        self.logs = logs if logs is not None else {}

    @property
    def _COMPATIBILITY_ATTRIBUTES(self):
        result = super(ExecutionRecord, self)._COMPATIBILITY_ATTRIBUTES
        result['properties'] = {}
        result['instance'] = None
        result['cleanup_exit_code'] = None
        result['events'] = []
        return result

    def get_event_attributes(self):
        ATTRIBUTE_PREFIX = 'EVENT_ATTR:'
        result = {}
        for key, value in self.properties.items():
            if key.startswith(ATTRIBUTE_PREFIX):
                result[key.split(':', 1)[1]] = value
        return result

    def __str__(self):
        if self.start_time:
            start_time = timestamp_to_str(self.start_time)
        else:
            start_time = str(self.start_time)
        if self.end_time:
            end_time = timestamp_to_str(self.end_time)
        else:
            end_time = str(self.end_time)
        return ('ExecutionRecord(info=%s, instance=%s, start_time=%s, '
                'end_time=%s, exit_code=%s, events=%s, properties=%s, '
                'logs=%s)' %
                (self.info, self.instance, start_time, end_time,
                 self.exit_code, self.events, self.properties, self.logs))

    def __repr__(self):
        return self.__str__()


class JobExecutor(object):
    """Interface of a client communicating with token master."""
    __metaclass__ = abc.ABCMeta

    def __init__(self, workflow, instance, job_name, job, data_builder,
                 emailer):
        self._workflow = workflow
        self._instance = instance
        self._job_name = job_name
        self.job = job
        self._data_builder = data_builder
        self._emailer = emailer
        # A map from log type to the log saver storing job output of this type.
        self._log_savers = {}

    _cleaners = set()

    @staticmethod
    @atexit.register
    def _call_cleaners():
        for cleaner in JobExecutor._cleaners:
            cleaner()

    @staticmethod
    def from_job(workflow, instance, job_name, job, data_builder, emailer):
        """Create an executor capable of running a given job."""
        if type(job) == ShellJob or type(job) == ShellConditionJob:
            return ShellJobExecutor(workflow, instance, job_name, job,
                                    data_builder, emailer)

    @abc.abstractmethod
    def prepare(self):
        return

    @abc.abstractmethod
    def execute(self):
        return

    @abc.abstractmethod
    def abort(self):
        return


class ShellJobExecutor(JobExecutor):
    def __init__(self, workflow, instance, job_name, job, data_builder,
                 emailer):
        super(ShellJobExecutor, self).__init__(workflow, instance, job_name,
                                               job, data_builder, emailer)
        # Indicates if job data has not been recorded in the master.
        self.job_dirty = False
        self._process = None
        self._aborted = False
        self._abort_timeout_reached = False
        self._warn_timeout_reached = False
        self._lock = threading.Lock()
        self._log_pipe_readers = {}

    def _get_logs_dir(self, log_directory):
        """Generate name of directory where job logs are stored.

        Returns:
            Name of the job logs directory.
        """
        return get_logs_dir(self._workflow, self._instance, log_directory)

    def _get_log_filename(self, log_type, timestamp):
        """Generate log file name.

        Args:
            log_type: Type of the log stored in the file.
            timestamp: The execution timestamp.
        """
        # TODO(pawel): the file name should contain more context, in particular
        # the workflow and instance names.
        filename = '%s.%d.%s' % (self.job.name, timestamp, log_type)
        log_directory = PinballConfig.S3_LOGS_DIR \
            if PinballConfig.S3_LOGS_DIR else PinballConfig.LOCAL_LOGS_DIR
        return os.path.join(self._get_logs_dir(log_directory), filename)

    def _get_last_execution_record(self):
        """Retrieve the most recent job execution record.

        Returns:
            The most recent job execution record.
        """
        assert self.job.history
        return self.job.history[-1]

    def _check_token_lost(self):
        """Check if the ownership of the job token has been lost.

        Returns:
            True iff the job token ownership has been lost.
        """
        if not self.job.history:
            return False
        execution_record = self.job.history[-1]
        assert execution_record.start_time
        if not execution_record.end_time:
            execution_record.end_time = time.time()
            execution_record.exit_code = 1
            message = 'executor failed to renew job ownership on time\n'
            self._append_to_pinlog(message)
            return True
        return False

    def prepare(self):
        """Prepare the execution.

        As a side effect, this method appends an execution record to the job
        history.

        Returns:
            True iff the preparations succeeded.  If False, the job should not
            be executed.
        """
        if self._check_token_lost():
            return False
        execution_record = ExecutionRecord(instance=self._instance,
                                           start_time=time.time())
        execution_record.properties['worker'] = socket.gethostname()
        self.job.history.append(execution_record)
        self.job.truncate_history()
        execution_record.events = self.job.events
        self.job.events = []
        if self.job.disabled:
            execution_record.info = 'DISABLED'
            return True
        else:
            execution_record.info = self.job.command
        try:
            logs_dir = self._get_logs_dir(PinballConfig.LOCAL_LOGS_DIR)
            if not os.path.exists(logs_dir):
                os.makedirs(logs_dir)
        except:
            LOG.exception('')
            execution_record.end_time = time.time()
            execution_record.exit_code = 1
            return False

        for log_type in ['stdout', 'stderr']:
            execution_record.logs[log_type] = self._get_log_filename(
                log_type, execution_record.start_time)
            self._log_savers[log_type] = \
                log_saver.FileLogSaver.from_path(
                    execution_record.logs[log_type])
            self._log_savers[log_type].open()
        return True

    def _set_log_pipe_reader(self, process):
        """ Sets up buffered line reader to read the log pipes

        Args:
            process: the process to set up the log piper reader for
        """
        self._log_pipe_readers = {
            process.stdout: BufferedLineReader(process.stdout),
            process.stderr: BufferedLineReader(process.stderr)}

    def _process_log_line(self, line):
        """Process a log line to extract properties.

        It will parse every log line that starts with "PINBALL_MAGIC" and the
        line format is expected to be:
            "PINBALL_MAGIC:prop_name=prop_value"

        prop_name and prop_value could be arbitrary text value (as long as it
        doesn't include the magic string), but the following prop_name is
        known to be working with Pinball UI:

        "kill_id"    - the prop_value will be passed down to cleanup command
            example: "PINBALL_MAGIC:kill_id=data_core/123456"

        "kv_job_url" - the prop_value is supposed to be "anchor_text|job_url"
                        pairs, where value is a full url to job page and key
                        is the anchor text for this link. '|' is used to
                        separate the anchor_text and url as it's an illegal url
                        char according to: http://tools.ietf.org/html/rfc3986#section-2
            example: "PINBALL_MAGIC:kv_job_url=job_123456|http://job_url_link"

        The extracted properties is a map in following format: {
            kill_id: ['executor_name1/cmd_id1', 'executor_name2/cmd_id2']
            kv_job_url: ['job_id1|job_url1', 'job_id2|job_url2'],
        }

        NOTE:
            1. for values we read for the same key, we will accumulate them into
        a list, even if there is only one value for that key.
            2. we will keep the value list unique. (no duplicated item)

        Args:
            line: The log line to process.
        """

        def _parse_key_value(kv_text, separator):
            k, v = None, None
            try:
                k, v = kv_text.split(separator, 1)
            except Exception:
                LOG.exception('')
                LOG.warn("Can't parse: %s using sep: %s", kv_text, separator)
            return k, v

        # A magic value marking log lines with key=value pairs.
        PINBALL_MAGIC = 'PINBALL:'
        if line.startswith(PINBALL_MAGIC):
            if not line.endswith('\n'):
                LOG.warn('PINBALL line is not properly terminated: %s', line)

            line = line.strip()
            line = line[len(PINBALL_MAGIC):]
            prop_key, prop_value = _parse_key_value(line, '=')

            if prop_key:
                execution_record = self._get_last_execution_record()
                if prop_key not in execution_record.properties.keys():
                    execution_record.properties[prop_key] = []

                # We might have duplicated pinball magic log lines.
                if prop_value not in execution_record.properties[prop_key]:
                    execution_record.properties[prop_key].append(prop_value)

                self.job_dirty = True
            else:
                LOG.warn("Empty key is found in pinball magic string: %s", line)

    def _consume_logs(self, process):
        """Process logs produced by the specified process.

        Args:
            fout: The file descriptor to write stdout to.
            ferr: The file descriptor to write stderr to.
            process: The process whose logs we want to consume.
        Returns:
            True iff any data was read.
        """
        TIMEOUT_SEC = 60.  # 1 minute
        streams = []
        if not process.stdout.closed:
            streams.append(process.stdout)
        if not process.stderr.closed:
            streams.append(process.stderr)
        if not streams:
            return False
        ready_to_read = select.select(streams,
                                      [],
                                      [],
                                      TIMEOUT_SEC)[0]
        if not ready_to_read:
            LOG.info('select timeout reached while reading output of command '
                     '%s', self.job.command)
        for source in ready_to_read:
            lines = self._log_pipe_readers[source].readlines()

            if self._log_pipe_readers[source].eof():
                source.close()

            for line in lines:
                if source == process.stdout:
                    self._log_savers['stdout'].write(line)
                else:
                    assert source == process.stderr
                    self._log_savers['stderr'].write(line)
                self._process_log_line(line)

        return ready_to_read != []

    def _write_separator_to_logs(self, flag):
        msg = '\n<<<<<<<<<<%s of cleanup code logs>>>>>>>>>>\n' % flag
        for log_type in ['stdout', 'stderr']:
            if log_type in self._log_savers:
                self._log_savers[log_type].write(msg)

    def _execute_cleanup(self):
        """Cleanup given execution of the job."""
        execution_record = self._get_last_execution_record()
        if not self.job.cleanup_template:
            self._append_to_pinlog('cleanup template not found.')
            return None
        if not execution_record.properties.get('kill_id', None):
            self._append_to_pinlog('kill_id not found.')

        try:
            # we assume the only thing needed by the template is kill_ids
            kill_id_text = ','.join(execution_record.properties['kill_id'])
            cleanup_command = self.job.cleanup_template % {
                'kill_id': kill_id_text
            }
        except KeyError:
            message = ('Could not customize cleanup command %s with '
                       'properties %s' % (self.job.cleanup_template,
                                          execution_record.properties))
            self._append_to_pinlog(message)
            return 1
        env = os.environ.copy()
        env.pop('DJANGO_SETTINGS_MODULE', None)
        cleanup_process = subprocess.Popen(
            cleanup_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            env=env,
            preexec_fn=os.setsid)
        self._set_log_pipe_reader(cleanup_process)

        self._write_separator_to_logs('Start')
        while cleanup_process.poll() is None:
            self._consume_logs(cleanup_process)
        # Check again to catch anything after the process
        # exits.
        while self._consume_logs(cleanup_process):
            pass
        self._write_separator_to_logs('End')
        return cleanup_process.wait()

    def _append_to_pinlog(self, message):
        """Append message to pinlog.

        PINLOG means pinball log which contains log lines pinball itself
        generates for a particular job.

        Args:
            message: The log message to append.
        """
        execution_record = self._get_last_execution_record()
        pinlog = execution_record.logs.get('pinlog')
        try:
            if not pinlog:
                #TODO(Mao): Move the logic to create local dirs to log saver
                logs_dir = self._get_logs_dir(PinballConfig.LOCAL_LOGS_DIR)
                if not os.path.exists(logs_dir):
                    os.makedirs(logs_dir)
                pinlog = self._get_log_filename('pinlog',
                                                execution_record.start_time)
                self._log_savers['pinlog'] = log_saver.FileLogSaver.from_path(
                    pinlog)
                self._log_savers['pinlog'].open()
                execution_record.logs['pinlog'] = pinlog

            self._log_savers['pinlog'].write(message)
        except:
            LOG.exception('')

    def _get_emails(self):
        """Get notification emails for the currently running job.

        Returns:
            List of job's notification email addresses.
        """
        emails = set(self.job.emails)
        schedule_data = self._data_builder.get_schedule(self._workflow)
        if schedule_data:
            emails.update(schedule_data.emails)
        return list(emails)

    def _check_timeouts(self):
        """Check if timeouts have been reached."""
        if self._abort_timeout_reached:
            return
        execution_record = self._get_last_execution_record()
        start_time = execution_record.start_time
        now = time.time()
        if (self.job.abort_timeout_sec and
                start_time + self.job.abort_timeout_sec < now):
            self._abort_timeout_reached = True
            self.abort()
            return
        if (not self._warn_timeout_reached and self.job.warn_timeout_sec and
                start_time + self.job.warn_timeout_sec < now):
            self._warn_timeout_reached = True
            emails = self._get_emails()
            if not emails:
                return
            execution = len(self.job.history) - 1
            job_execution_data = self._data_builder.get_execution(
                self._workflow, self._instance, self._job_name, execution)
            self._emailer.send_job_timeout_warning_message(emails,
                                                           job_execution_data)

    def execute(self):
        """Execute the job.

        It is assumed that the prepare method has been called and returned
        True.

        Returns:
            True iff the execution succeeded.
        """
        execution_record = self._get_last_execution_record()
        assert not execution_record.end_time
        if self.job.disabled:
            execution_record.end_time = execution_record.start_time
            execution_record.exit_code = 0
            return True
        try:
            assert not self._process
            with self._lock:
                # We need the lock to prevent a situation where the job
                # executes even though it got aborted (by a different
                # thread).
                aborted = self._aborted
                if not aborted:
                    command = self.job.customize_command()
                    LOG.info('executing command: %s', command)
                    env = os.environ.copy()
                    # Pinball sets Django module path which may interfere
                    # with the command being executed.
                    env.pop('DJANGO_SETTINGS_MODULE', None)
                    # The os.setsid() is passed in the argument preexec_fn
                    # so it's run after the fork() and before  exec() to
                    # run the shell.  It attaches a session id of the child
                    # process to the parent process which is a shell in our
                    # case.  This will make it the group leader.  So when a
                    # signal is sent to the process group leader, it's
                    # transmitted to all of the child processes.
                    self._process = subprocess.Popen(
                        command,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        shell=True,
                        env=env,
                        preexec_fn=os.setsid)
                    self._set_log_pipe_reader(self._process)
                    JobExecutor._cleaners.add(self.abort)

            if aborted:
                # TODO(pawel): we should have an explicit indicator that
                # the job was aborted.
                execution_record.exit_code = 1
            else:
                while self._process.poll() is None:
                    self._consume_logs(self._process)
                    self._check_timeouts()
                # Check again to catch anything after the process
                # exits.
                while self._consume_logs(self._process):
                    pass
                execution_record.exit_code = self._process.wait()
                JobExecutor._cleaners.remove(self.abort)
            with self._lock:
                self._process = None

            if execution_record.exit_code != 0:
                execution_record.cleanup_exit_code = \
                    self._execute_cleanup()
        except:
            LOG.exception('')
            execution_record.exit_code = 1
            self._append_to_pinlog(traceback.format_exc())
        finally:
            execution_record.end_time = time.time()
            # Make sure we've saved all the logs
            try:
                for log_type in self._log_savers:
                    self._log_savers[log_type].close()
            except:
                LOG.exception('')

        return execution_record.exit_code == 0

    def abort(self):
        """Abort the currently running job."""
        with self._lock:
            self._aborted = True
            if self._process:
                os.killpg(self._process.pid, signal.SIGKILL)
