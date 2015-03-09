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

import unittest

import mock
from pinball_ext.common.hadoop_utils import HadoopHostConfig
from pinball_ext.executor import emr_executor
from pinball_ext.executor.common import Platform


__author__ = 'Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


hadoop_node = HadoopHostConfig()
hadoop_node.USER_NAME = 'test_user'
hadoop_node.HOST_NAME = 'test_host_name'
hadoop_node.SSH_PORT = 22
hadoop_node.SSH_KEY_FILE = '~/test.key'
hadoop_node.HOST_NAME = '/usr/hadoop/'


class EmrExecutorTest(unittest.TestCase):
    @mock.patch('pinball_ext.common.hadoop_utils.run_and_check_command_in_hadoop')
    @mock.patch('pinball_ext.executor.emr_executor.EMRExecutor._get_raw_query_result',
                mock.Mock(return_value=[]))
    @mock.patch('pinball_ext.executor.emr_executor.EMRExecutor._mk_tmp_dir',
                mock.Mock(return_value='test_job_dir'))
    def test_hadoop_job(self, run_command):
        user_jar_dirs = ['/dir1/jar1', '/dir2/jar2']
        user_app_jar = '/dir/jar.jar'
        user_archive = '/dir/file.archive'

        executor = emr_executor.EMRExecutor(
            executor_config={
                'USER_LIBJAR_DIRS': ','.join(user_jar_dirs),
                'USER_APPJAR_PATH': user_app_jar,
                'USER_ARCHIVE_PATH': user_archive,
                'USER': 'test_user',

                'HADOOP_HOST_USER': hadoop_node.USER_NAME,
                'HADOOP_HOST_NAME': hadoop_node.HOST_NAME,
                'HADOOP_HOST_SSH_PORT': hadoop_node.SSH_PORT,
                'HADOOP_HOST_SSH_KEY_FILE': hadoop_node.SSH_KEY_FILE,
                'HADOOP_HOST_HOME': hadoop_node.REMOTE_HADOOP_HOME,
            }
        )
        self.assertEqual(executor.config.PLATFORM, Platform.EMR)

        executor.run_hadoop_job(
            'test_job_class',
            jobconf_args={
                'pinball.key1': 'value1',
                'pinball.key2': 'value2',
            },
            extra_args=[
                '-Dpinball.key3=value3'
            ])

        self.assertEqual(run_command.call_count, 1)
        self.assertEquals(len(run_command.call_args_list[0][0]), 2)
        self.assertEquals(run_command.call_args_list[0][0][0], hadoop_node)
        self.assertEquals(run_command.call_args_list[0][0][1],
                          "set -o pipefail; "
                          "HADOOP_CLASSPATH=/home/hadoop/test_user//dir1/jar1/*:/home/hadoop/test_user//dir2/jar2/* "
                          "hadoop jar /home/hadoop/test_user//dir/jar.jar test_job_class "
                          "-libjars `echo /home/hadoop/test_user//dir1/jar1/*.jar /home/hadoop/test_user//dir2/jar2/*.jar | tr ' ' ','` "
                          "-Dmapred.job.name=test_user:AdHocCommand "
                          "-Dpinball.key2=value2 "
                          "-Dpinball.key1=value1 "
                          "-Dpinball.key3=value3 "
                          "2>&1 > test_job_dir/out.csv | tee test_job_dir/out.err")
        self.assertEquals(run_command.call_args_list[0][1].keys(), ['log_line_processor'])


    @mock.patch('pinball_ext.common.hadoop_utils.put_string_to_hadoop')
    @mock.patch('pinball_ext.common.hadoop_utils.run_and_check_command_in_hadoop')
    @mock.patch('pinball_ext.executor.emr_executor.EMRExecutor._get_raw_query_result',
                mock.Mock(return_value=[]))
    @mock.patch('pinball_ext.executor.emr_executor.EMRExecutor._mk_tmp_dir',
                mock.Mock(return_value='test_query_dir'))
    def test_hive_job(self, run_command, put_string):
        executor = emr_executor.EMRExecutor(
            executor_config={
                'USER': 'test_user',
                'HADOOP_HOST_USER': hadoop_node.USER_NAME,
                'HADOOP_HOST_NAME': hadoop_node.HOST_NAME,
                'HADOOP_HOST_SSH_PORT': hadoop_node.SSH_PORT,
                'HADOOP_HOST_SSH_KEY_FILE': hadoop_node.SSH_KEY_FILE,
                'HADOOP_HOST_HOME': hadoop_node.REMOTE_HADOOP_HOME,
            }
        )

        q_str = 'SHOW TABLES;'
        executor.run_hive_query(q_str)

        self.assertEquals(put_string.call_count, 1)
        self.assertEquals(len(put_string.call_args_list[0][0]), 3)
        self.assertEquals(put_string.call_args_list[0][0][0], hadoop_node)
        self.assertEquals(put_string.call_args_list[0][0][1], 'set mapred.job.name=test_user:AdHocCommand;\nSHOW TABLES;')
        self.assertEquals(put_string.call_args_list[0][0][2], 'test_query_dir/query.ql')

        self.assertEquals(run_command.call_count, 1)
        self.assertEquals(len(run_command.call_args_list[0][0]), 2)
        self.assertEquals(run_command.call_args_list[0][0][0], hadoop_node)
        self.assertEquals(run_command.call_args_list[0][0][1],
                          'set -o pipefail; '
                          '/home/hadoop/hive/bin/hive -f test_query_dir/query.ql 2>&1 > test_query_dir/out.csv '
                          '| tee test_query_dir/out.err')
        self.assertEquals(run_command.call_args_list[0][1].keys(), ['log_line_processor'])
