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

"""An executor that executes Hadoop job locally."""

import os

from pinball_ext.executor.cluster_executor import ClusterExecutor
from pinball_ext.executor.common import Platform


__author__ = 'Changshu Liu, Rui Jiang'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class LocalExecutor(ClusterExecutor):
    """Runs Hadoop jobs on local machine.

    The Config.USER_LIBJAR_DIRS, Config.USER_APP_JAR_PATH should be local path.
    """
    def __init__(self, executor_config=None):
        super(LocalExecutor, self).__init__(executor_config)
        self.config.PLATFORM = Platform.LOCAL

    def run_hadoop_job(self,
                       class_name,
                       jobconf_args=None,
                       extra_args=None,
                       extra_jars=None):
        jobconf_args = jobconf_args if jobconf_args else {}
        extra_args = extra_args if extra_args else []

        # set default jobconf args
        jobconf_args = jobconf_args.copy()
        jobconf_args['mapred.job.name'] = self.job_name

        # create arguments string
        arguments = ' '.join('-D%s=%s' % (k, v) for k, v in jobconf_args.iteritems())
        arguments += ' '
        arguments += ' '.join(extra_args)

        libjars_glob = ' '.join(
            ['%s/*.jar' % d for d in self.config.USER_LIBJAR_DIRS])
        libjars = '`echo %s | tr \' \' \',\'`' % libjars_glob

        user_jar_dirs = ['%s/*' % d for d in self.config.USER_LIBJAR_DIRS]
        hadoop_classpath = ':'.join(user_jar_dirs)

        # generate command
        var_dict = {
            'user': self.config.USER,
            'class_name': class_name,
            'arguments': arguments,
            'hadoop_classpath': hadoop_classpath,
            'libjars': libjars,
            'app_jar': self.config.USER_APPJAR_PATH,
        }

        cmd = ('export HADOOP_CLASSPATH=%(hadoop_classpath)s;'
               ' hadoop jar %(app_jar)s'
               ' %(class_name)s'
               ' -libjars %(libjars)s'
               ' %(arguments)s') % var_dict

        # log command message
        self.log.info('Running class:%s with arguments:%s' %
                      (class_name, arguments))
        self.log.info('Full command: %s' % cmd)

        # run command
        os.system(cmd)

        return [], [], self.job_ids
