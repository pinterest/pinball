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

from pinball_ext.common import utils
from pinball_ext.job.basic_jobs import ClusterJob


__author__ = 'Changshu Liu, Mohammad Shahangian, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = utils.get_logger('pinball_ext.job.hadoop_jobs')


class HadoopJob(ClusterJob):
    """Base class for actual Hadoop jobs.

    App jar and lib jars are configured in executor, please see
    Executor.run_hadoop_job() for detailed info.

    Derived class should at least override _get_class_name() to specify what's
    the main Java class to execute. It can also optionally override _setup() to
    config the follow parameters to further tune the job config:
    - self.jobconf_args
    - self.extra_jars
    - self.extra_arguments

    self.params derived from JobBase will also be passed as job's extra
    arguments (together with self.extra_arguments).
    """
    def __init__(self, params, settings=None):
        super(HadoopJob, self).__init__(params, settings)
        self.jobconf_args = {}
        self.extra_arguments = []
        self.extra_jars = []

    def _get_class_name(self):
        raise NotImplementedError('No class name specified for this Hadoop Job')

    def _execute(self):
        param_args = ['-%s %s' % (k, v) for k, v in self.params.iteritems()]

        self._job_output, self._job_stderr, self._job_ids = \
            self.executor.run_hadoop_job(
                self._get_class_name(),
                jobconf_args=self.jobconf_args,
                extra_args=param_args + self.extra_arguments,
                extra_jars=self.extra_jars)

        LOG.info('Dump job output ...')
        for line in self._job_output:
            LOG.info('\t'.join(line))

    def __str__(self):
        return '(%s): (%s) - (%s)' % (self.job_name,
                                      self.params,
                                      self.jobconf_args)
