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

"""An executor to run Hadoop job in Amazon Elastic MapReduce cluster.

For more information about EMR, check: http://aws.amazon.com/elasticmapreduce/
"""

import datetime
import os
import re
import subprocess

from pinball_ext.common import hadoop_utils
from pinball_ext.common import utils
from pinball_ext.common.decorators import retry
from pinball_ext.executor.cluster_executor import ClusterExecutor
from pinball_ext.executor.common import Platform


__author__ = 'Changshu Liu, Zach Drach'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class EMRExecutor(ClusterExecutor):
    """Implements the executor on the EMR platform.

    The config.USER_JAR_DIRS and config.USER_APP_JAR are local paths relative to
    self.config.HADOOP_HOST_HOME/self.config.USER on machine:
    self.config.HADOOP_HOST_NAME (usually, it's the EMR master node).

    So you need to upload everything to that folder before executing any Hadoop
    job using this executor. Usually, those jars are built and uploaded to
    corresponding folder automatically as part of build system.

    TODO(csliu): make run_hadoop_streaming_job() work.
    """

    class Config(ClusterExecutor.Config):
        # Directory that hive query files are put into on hadoop master.
        # The hive query itself as well as its output will be in a custom
        # directory under this folder.
        HIVE_QUERIES_DIR = '/mnt/tmp'

        HIVE_FAILED_PREFIX = 'FAILED:'
        HIVE_HADOOP_FAILURE = ['java.lang.RuntimeException',  # Java error
                               'Processes ended with exit',  # Java error
                               'Program will exit.',  # Java error
                               'Exception in thread',  # Java java error
                               'does not exist']  # Hive add jar error

        SCHEDULER_PARAM = 'mapred.job.queue.name'

        PLATFORM = Platform.EMR

    def run_hive_query(self, query_str, upload_archive=False):
        full_query_string = \
            self._generate_hive_query_header(upload_archive=upload_archive)
        full_query_string += self._get_scheduler_hive_setting()
        full_query_string += query_str

        self.log.info('Running query:\n %s' % full_query_string)

        query_dir = self._mk_tmp_dir()
        hadoop_utils.put_string_to_hadoop(
            self.hadoop_host_config,
            full_query_string,
            '%s/query.ql' % query_dir)

        hadoop_utils.run_and_check_command_in_hadoop(
            self.hadoop_host_config,
            'set -o pipefail; /home/hadoop/hive/bin/hive -f %s/query.ql 2>&1 '
            '> %s/out.csv | tee %s/out.err'
            % (query_dir, query_dir, query_dir),
            log_line_processor=self._hive_query_log_line_processor)

        q_stdout = self._get_raw_query_result('%s/out.csv' % query_dir)
        q_stderr = self._get_raw_query_result('%s/out.err' % query_dir)

        self._check_for_hive_failure_message(q_stderr)

        self.log.info("Output has %d rows. First 10 rows:\n\t%s"
                      % (len(q_stdout),
                         '\n\t'.join([str(o) for o in q_stdout[:9]])))

        return q_stdout, q_stderr, self.job_ids

    def _check_for_hive_failure_message(self, q_stderr):
        """Catch when hive fails without returning a non-zero error code.

        q_stderr: a list of lines from q_stderr.
        """
        failed = any(' '.join(line).startswith(self.config.HIVE_FAILED_PREFIX)
                     for line in q_stderr)
        failed = failed or any(
            any(f in line for f in self.config.HIVE_HADOOP_FAILURE)
            in line for line in q_stderr)
        if failed:
            # hive didn't actually returns a non-zero error code.
            raise subprocess.CalledProcessError(
                1,
                '\n'.join('\t'.join(l) for l in q_stderr))

    def _hive_query_log_line_processor(self, line):
        """ A callback function that gets executed for every line of stderr
        coming from the running job.

        Returns a dict of pinball metadata.
        """
        job_regex = \
            r'Starting Job = (?P<job_id>\w+), Tracking URL = (?P<job_url>.+)'
        m = re.search(job_regex, line)
        if m:
            job_id = m.group('job_id')
            job_url = m.group('job_url')
            if job_id and job_url:
                self.job_ids.append(job_id)
                return {'job_id': job_id,
                        'job_url': job_url,
                        'kill_id': '%s/%s' % (self.config.PLATFORM, job_id)}
        return {}

    @retry(subprocess.CalledProcessError)
    def _mk_tmp_dir(self):
        query_timestamp =\
            datetime.datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
        query_dir = os.path.join(
            self.config.HIVE_QUERIES_DIR,
            self.config.USER,
            '%s_%s_%s' % (query_timestamp,
                          utils.get_random_string(),
                          self.config.NAME))
        hadoop_utils.run_and_check_command_in_hadoop(
            self.hadoop_host_config,
            command='mkdir -p %s' % query_dir)
        return query_dir

    @retry(subprocess.CalledProcessError)
    def _get_raw_query_result(self, output_file):
        """Return the raw query output from a query output file.

        Args:
            output_file: Location on the Hadoop local master of query output.

        Returns:
            A list of parsed rows. Each row is a list of strings.
        """
        rows = []
        with hadoop_utils.run_command_in_hadoop(
                self.hadoop_host_config,
                command='cat %s' % output_file) as hive_out:
            for line in hive_out:
                if line.strip():
                    rows.append(line.strip().split('\t'))
        return rows

    def kill_job(self, job_id):
        """Kills a EMR job with the given job_id."""
        cmd = 'hadoop job -kill %s' % job_id
        hadoop_utils.run_and_check_command_in_hadoop(
            self.hadoop_host_config,
            cmd)

    def run_hadoop_job(self,
                       class_name,
                       jobconf_args=None,
                       extra_args=None,
                       extra_jars=None):
        jobconf_args = jobconf_args if jobconf_args else {}
        extra_args = extra_args if extra_args else []
        extra_jars = extra_jars if extra_jars else []

        # Set default jobconf args
        jobconf_args = jobconf_args.copy()
        if self.config.SCHEDULER_QUEUE:
            jobconf_args[self.config.SCHEDULER_PARAM] = \
                self.config.SCHEDULER_QUEUE
        jobconf_args['mapred.job.name'] = self.job_name

        # create arguments string
        arguments = \
            ' '.join('-D%s=%s' % (k, v) for k, v in jobconf_args.iteritems())
        arguments += ' '
        arguments += ' '.join(extra_args)

        base_dir = self.get_job_resource_dir(self.config.USER)
        libjars_glob = ' '.join(
            ['%s/%s/*.jar' % (base_dir, d) for d in self.config.USER_LIBJAR_DIRS])
        libjars = '`echo %s | tr \' \' \',\'`' % libjars_glob

        user_jar_dirs = ['%s/%s/*' % (base_dir, d) for d in self.config.USER_LIBJAR_DIRS]
        hadoop_classpath = ':'.join(user_jar_dirs)

        app_jar_path = '%s/%s' % (base_dir, self.config.USER_APPJAR_PATH)

        # temp dir for holding stdout and stderr
        query_dir = self._mk_tmp_dir()

        # generate command
        var_dict = {
            'class_name': class_name,
            'arguments': arguments,
            'query_dir': query_dir,
            'app_jar': app_jar_path,
            'libjars': libjars,
            'hadoop_classpath': hadoop_classpath,
        }
        cmd = ('set -o pipefail; HADOOP_CLASSPATH=%(hadoop_classpath)s'
               ' hadoop jar %(app_jar)s'
               ' %(class_name)s'
               ' -libjars %(libjars)s'
               ' %(arguments)s'
               ' 2>&1 > %(query_dir)s/out.csv | tee %(query_dir)s/out.err') % \
            var_dict

        self.log.info('Running class:%s with arguments:%s' % (class_name, arguments))
        self.log.info('Full command: %s' % cmd)

        # run command
        hadoop_utils.run_and_check_command_in_hadoop(
            self.hadoop_host_config,
            cmd,
            log_line_processor=self._hadoop_job_log_line_processor)

        rows = self._get_raw_query_result('%s/out.csv' % query_dir)
        stderr = self._get_raw_query_result('%s/out.err' % query_dir)

        return rows, stderr, self.job_ids

    def _hadoop_job_log_line_processor(self, line):
        """ A callback function that gets executed for every line of
        stderr coming from the running job. Returns a dict of pinball
        metadata.
        """
        job_regex = r"submitted hadoop job: (?P<job_id>.+)"
        m = re.search(job_regex, line)
        if m:
            job_id = m.group('job_id')
            self.job_ids.append(job_id)
            return {'job_id': job_id,
                    'kill_id': '%s/%s' % (self.config.PLATFORM, job_id)}
        return {}

    def get_job_resource_dir(self, run_as_user):
        """Path to location where job resources (jars, archives) are stored.

        For EMR it would be a local dir on the master node."""
        return os.path.join(self.config.HADOOP_HOST_HOME,
                            run_as_user)
