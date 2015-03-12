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
from pinball_ext.executor import qubole_executor
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


def _mock_run_qubole_cmd(qb_cmd, log_line, kwargs):
    return None, 'stdout', 'stderr', []


def _mock_list_s3_directory(s3_dir):
    print(s3_dir)
    if s3_dir[-1] == '1':
        return ['%s/jar10.jar' % s3_dir, '%s/jar11.jar' % s3_dir]
    elif s3_dir[-1] == '2':
        return ['%s/jar20.jar' % s3_dir, '%s/jar21.jar' % s3_dir]
    return []


def _mock_run_shell_command(qb_cmd):
    return None, 'stdout', 'stderr', []


class QuboleExecutorTest(unittest.TestCase):
    @mock.patch('pinball_ext.common.utils.get_random_string',
                mock.Mock(return_value='random_str'))
    @mock.patch('pinball_ext.common.s3_utils.list_s3_directory',
                side_effect=_mock_list_s3_directory)
    @mock.patch('pinball_ext.executor.qubole_executor.QuboleExecutor.run_shell_command',
                side_effect=_mock_run_shell_command)
    def test_hadoop_job(self, run_command, s3_list):
        user_jar_dirs = ['s3://b1/dir1', 's3://b2/dir2']
        user_app_jar = 's3:/b/dir/jar.jar'
        user_archive = 's3://b3/dir3/file.archive'

        executor = qubole_executor.QuboleExecutor(
            executor_config={
                'QUBOLE_JARS_BLACKLIST': 'hadoop.jar,hive.jar,jar10.jar',
                'USER_LIBJAR_DIRS': ','.join(user_jar_dirs),
                'USER_APPJAR_PATH': user_app_jar,
                'USER_ARCHIVE_PATH': user_archive,
                'USER': 'test_user',
            }
        )
        self.assertEqual(executor.config.PLATFORM, Platform.QUBOLE)

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
        self.assertEquals(len(run_command.call_args_list[0][0]), 1)
        self.assertEquals(run_command.call_args_list[0][0][0],
                          "mkdir -p /tmp/hadoop_users/test_user/random_str && "
                          "hadoop fs -get s3://b1/dir1 /tmp/hadoop_users/test_user/random_str && "
                          "hadoop fs -get s3://b2/dir2 /tmp/hadoop_users/test_user/random_str && "
                          "hadoop fs -get s3:/b/dir/jar.jar /tmp/hadoop_users/test_user/random_str/jar.jar && "
                          "rm -f /tmp/hadoop_users/test_user/random_str/hadoop.jar && "
                          "rm -f /tmp/hadoop_users/test_user/random_str/hive.jar && "
                          "rm -f /tmp/hadoop_users/test_user/random_str/jar10.jar && "
                          "export HADOOP_CLASSPATH=/tmp/hadoop_users/test_user/random_str/* && "
                          "hadoop jar /tmp/hadoop_users/test_user/random_str/jar.jar test_job_class "
                          "-libjars /tmp/hadoop_users/test_user/random_str/jar21.jar"
                          ",/tmp/hadoop_users/test_user/random_str/jar11.jar"
                          ",/tmp/hadoop_users/test_user/random_str/jar20.jar "
                          "-Dmapred.job.name=test_user:AdHocCommand "
                          "-Dpinball.key2=value2 "
                          "-Dpinball.key1=value1 "
                          "-Dpinball.key3=value3;\n"
                          "EXIT_CODE=$?; \n"
                          "rm -rf /tmp/hadoop_users/test_user/random_str; \n"
                          "exit $EXIT_CODE;")

    @mock.patch('pinball_ext.executor.qubole_executor.QuboleExecutor._run_qubole_command_with_stderr',
                side_effect=_mock_run_qubole_cmd)
    def test_hive_job(self, run_qubole_cmd):
        executor = qubole_executor.QuboleExecutor(
            executor_config={
                'USER': 'test_user',
            }
        )

        q_str = 'SHOW TABLES;'
        executor.run_hive_query(q_str)

        self.assertEquals(run_qubole_cmd.call_count, 1)
        self.assertEquals(len(run_qubole_cmd.call_args_list[0][0]), 3)
        self.assertEquals(run_qubole_cmd.call_args_list[0][0][2], {
            'query': 'set mapred.job.name=test_user:AdHocCommand;\nSHOW TABLES;'})
