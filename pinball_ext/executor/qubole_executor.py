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

"""An executor for running Hadoop/Hive jobs in Qubole cluster.

For more information about Qubole and its API, please check:
    - https://api.qubole.com
"""

import getpass
import logging
import os
import re
import subprocess
import sys
import StringIO
import time

from qds_sdk.commands import Command
from qds_sdk.commands import HiveCommand
from qds_sdk.commands import ShellCommand
from qds_sdk.qubole import Qubole
from pinball_ext.common import output_filter
from pinball_ext.common import s3_utils
from pinball_ext.common import utils
from pinball_ext.common.decorators import retry
from pinball_ext.executor.cluster_executor import ClusterExecutor
from pinball_ext.executor.common import Platform


__author__ = 'Zach Drach, Changshu Liu, Jie Li'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class QuboleExecutor(ClusterExecutor):
    """Implements the executor on the Qubole platform.

    The implementation is based on the Qubole qds_sdk Python version. For
    detailed information, please check: https://github.com/qubole/qds-sdk-py.

    The following configuration paths are supposed to be absolute paths on s3:
        - self.config.USER_LIBJAR_DIRS
        - self.config.USER_APPJAR_PATH
        - self.config.USER_ARCHIVE_PATH

    This executor will generate a shell command line, which downloads all
    resource files to a node inside Qubole cluster and then submits the actual
    Hadoop job to the cluster from that node.

    TODO(csliu):
     - make executor work when jars are stored on HDFS.
     - make Hadoop streaming work for this executor.
    """

    class Config(ClusterExecutor.Config):
        # Qubole API token required to use Qubole's API, check
        # https://api.qubole.com/users/edit for detail.
        if getpass.getuser() == 'prod':
            # Should be configured in executor ctor later.
            API_TOKEN = ''
        else:
            # Make it flexible if developer runs it manually.
            API_TOKEN = os.environ.get('QUBOLE_API_TOKEN')

        API_URL = 'https://api.qubole.com/api/'
        API_VERSION = 'v1.2'
        POLL_INTERVAL_SEC = 20

        SCHEDULER_PARAM = 'mapred.fairscheduler.pool'
        PLATFORM = Platform.QUBOLE

        # These jars are already provided in Qubole's Hadoop distribution, so we need
        # to remove these jars from the Hadoop classpath, and also not ship these jars
        # over to the task nodes.
        QUBOLE_JARS_BLACKLIST = [
            'slf4j-api-1.7.2.jar',
            'slf4j-jdk14-1.6.4.jar',
            'slf4j-log4j12-1.7.2.jar',
            'log4j-1.2.15.jar',
            'log4j-over-slf4j-1.7.2.jar',
            'hadoop-core-1.0.3.jar',
            'hadoop-lzo-0.4.16.jar',
            'hadoop-test-1.0.3.jar',
            'hadoop-tools-1.0.3.jar',
            'jcl-over-slf4j-1.7.2.jar',
        ]

        # Number of retries for pulling status info from Quoble.
        NUM_RETRIES = 6

        # Initial delay for the retry of pulling status info from Quoble.
        INITIAL_DELAY = 60

    def __init__(self, executor_config=None):
        qubole_blacklist_jars = None
        for key, value in executor_config.items():
            if key == 'QUBOLE_JARS_BLACKLIST':
                qubole_blacklist_jars = value.split(',')
        if qubole_blacklist_jars:
            executor_config['QUBOLE_JARS_BLACKLIST'] = qubole_blacklist_jars

        super(QuboleExecutor, self).__init__(executor_config=executor_config)

    def run_hive_query(self, query_str, upload_archive=False):
        # tmp is a keyword in Qubole
        regex = r'([\s\(\),;]|^)tmp([\s\(\),;]|$)'
        query_str = re.sub(regex, r'\g<1>`tmp`\g<2>', query_str)

        full_query_string = self._generate_hive_query_header(
            upload_archive=upload_archive)
        full_query_string += self._get_scheduler_hive_setting()
        full_query_string += query_str

        self.log.info('Running query %s' % full_query_string)

        kwargs = dict(query=full_query_string)
        hc, output, stderr, job_ids = self._run_qubole_command_with_stderr(
            HiveCommand, self._hive_query_log_line_processor, kwargs)

        return output, stderr, job_ids

    def kill_command(self, qubole_jid):
        """Kills a qubole job with the given job_id."""
        self._configure_qubole()
        qubole_jid = int(qubole_jid)
        Command.cancel_id(qubole_jid)

    def get_job_result(self, qubole_jid):
        """Finds and retrieves results for existing Qubole job.

        Args:
            id: qubole job id.

        Returns:
            Job stdout output.
        """
        self._configure_qubole()
        qubole_jid = str(qubole_jid)
        return self._get_qubole_command_output(HiveCommand.find(qubole_jid))

    def run_hadoop_job(self,
                       class_name,
                       jobconf_args=None,
                       extra_args=None,
                       extra_jars=None):
        """Run a Hadoop job in Qubole cluster.

        We assume extra_jars are stored on s3 and the path looks like:
            s3://pinball/%{USER}/some_jar_dir/

        We fail the entire command if pulling the JARs down from s3 fails,
        so we use "&&" to connect shell commands.
        """
        jobconf_args = jobconf_args if jobconf_args else {}
        extra_args = extra_args if extra_args else []
        extra_jars = extra_jars if extra_jars else []

        # The place where all jars are stored in s3.
        s3_jar_dirs = self.config.USER_LIBJAR_DIRS + extra_jars
        # The place where all jars will be copied to locally.
        local_jar_dir = '/tmp/hadoop_users/%s/%s' % \
                        (self.config.USER, utils.get_random_string())
        download_jar_cmds = ['hadoop fs -get %s %s' % (s3_dir, local_jar_dir)
                             for s3_dir in s3_jar_dirs]
        download_jar_cmd = ' && '.join(download_jar_cmds)
        appjar_name = s3_utils.extract_file_name_from_s3_path(
            self.config.USER_APPJAR_PATH)
        download_jar_cmd += ' && hadoop fs -get %s %s/%s' % (
            self.config.USER_APPJAR_PATH,
            local_jar_dir,
            appjar_name
        )

        # Set default JobConf args.
        jobconf_args = {} if jobconf_args is None else jobconf_args.copy()
        if self.config.SCHEDULER_QUEUE:
            jobconf_args[self.config.SCHEDULER_PARAM] = \
                self.config.SCHEDULER_QUEUE
        jobconf_args['mapred.job.name'] = self.job_name

        # Create arguments.
        arguments = ' '.join('-D%s=%s' % (k, v) for k, v in jobconf_args.iteritems())
        arguments += ' '
        arguments += ' '.join(extra_args)

        libjars = self._get_libjars_local_paths(s3_jar_dirs, local_jar_dir)
        hadoop_classpath = '%s/*' % local_jar_dir

        cmd = 'mkdir -p %(local_jar_dir)s && %(download_jar_cmd)s'

        files_to_be_deleted = []
        for qubole_jar in self.config.QUBOLE_JARS_BLACKLIST:
            files_to_be_deleted.append('%s/%s' % (local_jar_dir, qubole_jar))
        if files_to_be_deleted:
            cmd += ' && rm -f %s' % (' && rm -f '.join(files_to_be_deleted))

        # Generate command.
        var_dict = {
            'class_name': class_name,
            'arguments': arguments,
            'appjar_name': appjar_name,
            'download_jar_cmd': download_jar_cmd,
            'local_jar_dir': local_jar_dir,
            'hadoop_classpath': hadoop_classpath,
            'libjars': libjars,
        }
        cmd += (' && export HADOOP_CLASSPATH=%(hadoop_classpath)s'
                ' && hadoop jar %(local_jar_dir)s/%(appjar_name)s'
                ' %(class_name)s'
                ' -libjars %(libjars)s'
                ' %(arguments)s')
        cmd += ';\nEXIT_CODE=$?; \nrm -rf %(local_jar_dir)s; \nexit $EXIT_CODE;'
        cmd = cmd % var_dict

        # Log command messages.
        self.log.info('Full command: %s' % cmd)

        # Run command.
        hc, output, stderr, job_ids = self.run_shell_command(cmd)
        return output, stderr, job_ids

    def run_shell_command(self, cmd):
        kwargs = {'inline': cmd}
        hc, output, stderr, job_ids = self._run_qubole_command_with_stderr(
            ShellCommand,
            self._shell_command_log_line_processor,
            kwargs)
        return hc, output, stderr, job_ids

    def _retry_wrapper(self, fn, tries=4):
        return retry(Exception, tries=tries, logger=self.log)(fn)()

    def _get_libjars_local_paths(self, s3_jar_dirs, local_jar_dir):
        """Returns a list of local jar paths downloaded from s3.

        Args:
            s3_jar_dirs: S3 path from which we pull down JARs from.
            local_jar_dir: local path on every machine in the Qubole cluster
                to which jars are pulled down

        Returns:
            List of local file paths as a string, with each file name delimited
            comma (,), if the supplied s3_jar_dirs is valid. Otherwise, returns
            empty string.
        """
        file_paths = []
        for s3_jar_dir in s3_jar_dirs:
            file_paths += s3_utils.list_s3_directory(s3_jar_dir)

        jar_names = [
            s3_utils.extract_file_name_from_s3_path(file_path)
            for file_path in file_paths if str(file_path).endswith('jar')]
        filtered_jar_names = [
            jar_name
            for jar_name in jar_names if jar_name not in self.config.QUBOLE_JARS_BLACKLIST]

        # dedup jar lists.
        filtered_jar_names = list(set(filtered_jar_names))

        final_jar_paths = [
            '%s/%s' % (local_jar_dir, jar_name)
            for jar_name in filtered_jar_names]
        return ','.join(final_jar_paths)

    ############################################################################
    # Private helper functions for Qubole functions
    ############################################################################

    def _configure_qubole(self):
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger('qds_connection')
        logger.propagate = False
        qdslog = logging.getLogger('qds')
        if not self.config.API_TOKEN:
            raise Exception("You didn't specify your QUBOLE_API_TOKEN in "
                            "your environment before running commands on "
                            "Qubole!\n. It can be found at http://api.qubole"
                            ".com/users/edit")
        Qubole.configure(api_token=self.config.API_TOKEN,
                         api_url=self.config.API_URL,
                         version=self.config.API_VERSION,
                         poll_interval=self.config.POLL_INTERVAL_SEC)
        return qdslog

    def _get_qubole_command_output(self, q_cmd):
        """Return the stdout output from a Qubole command object.

        Args:
            hc: the qubole command object

        Returns:
            Query stdout output.
        """
        raw_result_str = ''
        file_handler = StringIO.StringIO()
        if q_cmd.status == 'done':
            self._retry_wrapper(lambda: q_cmd.get_results(file_handler), tries=10)
            raw_result_str = file_handler.getvalue()
        file_handler.close()

        rows = []
        # Qubole writes very large outputs to S3,
        # with each field in this file delimited by ^A
        raw_result_str = raw_result_str.replace('\x01', '\t')
        lines = raw_result_str.strip().split('\n')

        for line in lines:
            if line == '[Empty]':
                continue
            if line.strip():
                rows.append(line.strip().split('\t'))

        return rows

    def _run_qubole_command_with_stderr(self, cls, log_line_processor, kwargs):
        """Run the Qubole command and print the log to sys.stderr in real time.

        Args:
            cls: the qds_sdk.command.Command subclass
            kwargs: for the constructor of the qubole command class

        Returns:
            The tuple (hc, output, stderr)

            hc: the qubole command object pointer
            output: the command output, same as run_raw_hive_query
            stderr: the command stderr, same as run_raw_hive_query
        """
        self._configure_qubole()

        # TODO(mao): set proper number for the tries param.
        hc = self._retry_wrapper(lambda: cls.create(**kwargs), tries=10)
        sys.stderr.write("PINBALL:kill_id=%s/%s\n" % (self.config.PLATFORM,
                                                      hc.id))
        sys.stderr.flush()

        f = None
        if log_line_processor:
            f = output_filter.OutputFilter(log_line_processor,
                                           output=sys.stderr)
        stderr_list = []
        self._job_ids = []

        retry_exception = self.config.NUM_RETRIES
        retry_delay = self.config.INITIAL_DELAY
        while retry_exception > 0:
            try:
                if cls.is_done(hc.status):
                    break
                self.log.info("Sleeping for %s seconds and polling."
                              % Qubole.poll_interval)
                time.sleep(Qubole.poll_interval)
                hc = cls.find(hc.id)

                # TODO(csliu): polling entire error log file is very inefficient
                stderr = self._retry_wrapper(lambda: hc.get_log())
                stderr = stderr.strip().split('\n')
                for i in range(len(stderr_list), len(stderr)):
                    line = stderr[i] + "\n"
                    stderr_list.append([line])
                    if f:
                        f.process_and_output([line])

                # Get a successful status pulling from quoble, reset the retry
                # exception number
                retry_exception = self.config.NUM_RETRIES
            except Exception as e:
                retry_exception -= 1
                self.log.error("Got error %s when checking Qubole status."
                               " Going to retry %d more times." %
                               (e.message, retry_exception))
                time.sleep(retry_delay)
                retry_delay *= 2

        query_id = str(hc.id)
        self.log.info('Completed Query, Id: %s, Status: %s' %
                      (query_id, hc.status))

        if hc.status == 'error' or hc.status == 'cancelled':
            error_message = "Failed on query: %s" % query_id
            raise subprocess.CalledProcessError(1, error_message)
        elif hc.status == "running":
            error_message = "The job is still running, but got too many " \
                            "qubole exceptions: %s" % query_id
            raise subprocess.CalledProcessError(1, error_message)

        self.log.info("Now receiving the query output.")
        output = self._get_qubole_command_output(hc)
        self.log.info("Received the query output.")
        self.log.info("Output has %d rows. First 10 rows:\n\t%s"
                      % (len(output),
                         '\n\t'.join([str(o) for o in output[:9]])))

        return hc, output, stderr_list, self._job_ids

    ############################################################################
    # Private log line processors.
    ############################################################################

    def _hive_query_log_line_processor(self, line):
        """ A callback function that gets executed for every line of
        stderr coming from the running job. Returns a dict of pinball
        metadata.
        """
        job_regex = r"Starting Job = (?P<job_id>.+?), " \
                    r"Tracking URL = <a href='(?P<job_url>.+?)'"
        m = re.search(job_regex, line)
        if m:
            job_id = m.group('job_id')
            job_url = m.group('job_url')
            if job_id and job_url:
                self._job_ids.append(job_id)
                return {'job_id': job_id, 'job_url': job_url}
        return {}

    def _shell_command_log_line_processor(self, line):
        """ A callback function that gets executed for every line of
        stderr coming from the running job. Returns a dict of pinball
        metadata.
        """
        job_regex = r"Submitted job: (?P<job_id>.+)"
        m = re.search(job_regex, line)
        if m:
            job_id = m.group('job_id')
            if job_id not in self._job_ids:
                self._job_ids.append(job_id)
            return {'job_id': job_id}

        # Checking "Running job" besides of "Submitted job" makes
        # job_id extraction more reliable even we miss some logs.
        job_regex = r"Running job: (?P<job_id>.+)"
        m = re.search(job_regex, line)
        if m:
            job_id = m.group('job_id')
            if job_id not in self._job_ids:
                self._job_ids.append(job_id)
            return {'job_id': job_id}

        url_regex = r"Tracking URL: <a href='(?P<job_url>.+?)'"
        m = re.search(url_regex, line)
        if m:
            job_url = m.group('job_url')
            return {'job_url': job_url}
        return {}
