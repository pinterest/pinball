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

import getpass
import os
import resource
import subprocess
import time

from pinball_ext.common import hadoop_utils
from pinball_ext.common import utils
from pinball_ext.executor.common import make_executor
from pinball_ext.executor.common import Platform


__author__ = 'Mao Ye, Mohammad Shahangian, Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


_SUCCESS_FILE = '_SUCCESS'
JOB_SLEEP_TIME_SEC = 60
LOG = utils.get_logger('pinball_ext.job.basic_jobs')


class JobBase(object):
    """Base class for all actual Jobs.

    All actual Jobs will take a (possibly empty) dictionary of parameters that
    it will parse and apply to its calculation.

    args:
        params - key value pairs for the params that the job will run using

    To run a Job we invoke the runjob() method which decides how to call
    the following methods:
        - _setup(): Responsible for constructing the arguments for the command
            line tool being invoked and doing any other prep work such as
            compilation.

        _set_output_dirs(): Set the attribute self.job_output_dirs with list of
            local or s3 directories for output files. These directories will be
            deleted when the job fails if _CLEANUP_AFTER_FAILURE is set to True.

        _execute(): Responsible for running the executable with the parameters
            that were setup(). Some jobs will have more complex execute methods,
            e.g. running the executable once for each day.

            This method should raise an exception if the job could not execute
            successfully.

            If this job produced any output that needs to be persisted or
            post-processed, the _execute function is also responsible for
            assigning that data to the "self._job_output" variable. The output
            is expected to be in list-of-lists format, which can roughly be
            translated into row-column format:
            self._job_output = [
                ['cell1a', 'cell1b'], # row 1
                ['cell2a', 'cell2b'], # row 2
            ]

            The _execute function is also responsible for assigning the
            "self._job_stderr" variable which takes the same format of data as
            _job_output.

        _complete(): Responsible for persisting any of the query results.
            For example, the _REPORTER is invoked in the _complete method.

        _cleanup(): Cleans up any temporary local files, or temporary S3/HDFS
            files. This will be called even if the other methods raise
            exceptions.
    """
    # If set to True, output dynamically generated during a failed job will
    # be deleted. Subclass jobs extending JobBase should override this to True
    # if needed. Do not change this default value.
    _CLEANUP_AFTER_FAILURE = False

    # If set to true, skip the job if the output dir has the _SUCCESS file.
    _SKIP_IF_SUCCEEDED = False

    def __init__(self, params=None, settings=None):
        """
        Args:
            params - a dict config that might be used by job logic.
            settings - a dict config that's used to execute job.
        """
        self.params = params if params else {}
        self.settings = settings if settings else {}

        self.user = getpass.getuser()
        job_name = self.params.get('job_name', None)
        self.job_name = job_name if job_name is not None else self.__class__.__name__
        self.delay_execution = 0

        # This is set in _set_output_dirs function
        self.job_output_dirs = []
        # These fields are set by the _execute function
        self._job_output = None
        self._job_stderr = None

    def _skip_execution(self):
        if self._SKIP_IF_SUCCEEDED and hasattr(self, 'output'):
            success_file = os.path.join(self.output, _SUCCESS_FILE)
            if hadoop_utils.hdfs_exists(success_file):
                LOG.info('Found {0}. Skip this job.'.format(success_file))
                return True
        return False

    def runjob(self, dry_run=False):
        """The runjob method is responsible for calling the setup, execute
        and complete for the dates that the report will be run.
        """
        try:
            self._setup()
            self._set_output_dirs()
            if dry_run:
                print self
                return
            if not self._skip_execution():
                self._execute()
            self._complete()
        except Exception:
            LOG.exception("Failed to run job")
            if self._CLEANUP_AFTER_FAILURE:
                self._cleanup_output_dirs(self.job_output_dirs)
            raise
        finally:
            self._cleanup()
        return

    def _setup(self):
        pass

    def _execute(self):
        pass

    def _complete(self):
        LOG.info('Maximum Resident set was %d MB' % (
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
        LOG.info('Successfully ran %s' % self.job_name)

    def _cleanup(self):
        pass

    def _cleanup_output_dirs(self, directories):
        LOG.info("Job failed: Output generated during job will get deleted.")
        for directory in directories:
            if self._is_local_directory(directory):
                self._delete_local_directory(directory)
            else:
                hadoop_utils.hdfs_rmr(directory)

    @staticmethod
    def _is_local_directory(directory):
        """Args:
            directory: A string representation of the directory path
        """
        return os.path.isdir(directory)

    @staticmethod
    def _delete_local_directory(directory):
        if directory[-1] != '/':
            directory += '/'
        proc = subprocess.Popen(
            'rm -rf %s' % directory,
            shell=True, stdout=subprocess.PIPE)
        assert proc.communicate()[0] is ''

    def _get_input_output_resources(self):
        # Override in child job classes.
        pass

    def _set_output_dirs(self):
        # Override in child job classes.
        pass

    def __str__(self):
        return '(%s): (%s)' % (self.job_name, self.params)


class PythonJob(JobBase):
    """PythonJob execute python code.

    The code should exist in the same codebase that this class lives. Each job
    is responsible for importing the dependencies in its execute() method.
    """
    def _setup(self):
        LOG.info('Warning: This PythonJob doesn\'t have any setup step.')

    def _execute(self):
        NotImplementedError('PythonJob does not have an execute method')


class CommandLineJob(JobBase):
    """CommandLineJob run a command line command.

    The user will usually need to override the setup method to format the
    arguments in the format that its command expects.
    """
    def _setup(self):
        self.arguments = ''
        for k, v in self.params.iteritems():
            self.arguments += '--%s=%s ' % (k, v)

    def _execute(self):
        command = '%s %s' % (self._get_command(), self.arguments)
        LOG.info('Running command: %s' % command)
        proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)

        # Get command process stdout.
        self._job_output = proc.communicate()[0]
        proc.wait()
        if proc.returncode != 0:
            raise Exception('Command %s failed' % command)

    def _get_command(self):
        raise Exception('This job does not have a command associated with it')


class ClusterJob(JobBase):
    def __init__(self, params, settings=None):
        super(ClusterJob, self).__init__(params, settings=settings)

        self._counters = {}
        self._job_ids = []

        if 'executor' not in self.settings:
            self.settings['executor'] = Platform.EMR
            LOG.warning(
                "%s is missing an 'executor' parameter in the job_settings. "
                "Using the default executor: %s.",
                self.job_name,
                self.settings['executor'])
        executor_name = self.settings['executor']
        executor_config = self.settings.get('executor_config', {})
        executor_config['NAME'] = self.job_name
        executor_config['PLATFORM'] = executor_name
        self.executor = make_executor(executor_name, executor_config)

    @property
    def hadoop_job_name(self):
        """The name of the job as it appears on the Hadoop Job Tracker."""
        return self.executor.job_name

    # on the mutable variable self._job_ids. rename to get_counters()
    @property
    def counters(self):
        """Return the job counters for the jobs in this Cluster job.

        Returns dict from job ID to a map of counter names to values for each
        job created by the Cluster job.
        """
        if not self._counters:
            for jid in self._job_ids:
                self._counters[jid] = self.executor.get_hadoop_counters(jid)
        return self._counters

    def _is_prod_job(self):
        return self.executor.config.USER == 'prod'

    def _delay(self):
        """Sleep for a while.

        This method is invoked by operations that depend on s3 files which may
        not be available due to s3 eventual consistency issues.
        """
        if self._is_prod_job():
            LOG.info('waiting for %d seconds to give s3 time to replicate',
                     JOB_SLEEP_TIME_SEC)
            time.sleep(JOB_SLEEP_TIME_SEC)
