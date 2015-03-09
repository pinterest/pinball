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

import mock
import unittest

from pinball_ext.executor import local_executor


__author__ = 'Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class LocalExecutorTest(unittest.TestCase):
    @mock.patch('os.system')
    def test_hadoop_job(self, f1):
        user_jar_dirs = ['/dir1/jar1', '/dir2/jar2']
        user_app_jar = '/dir/jar.jar'
        user_archive = '/dir/file.archive'

        executor = local_executor.LocalExecutor(
            executor_config={
                'USER_LIBJAR_DIRS': ','.join(user_jar_dirs),
                'USER_APPJAR_PATH': user_app_jar,
                'USER_ARCHIVE_PATH': user_archive,
                'USER': 'test_user'
            }
        )
        executor.run_hadoop_job(
            'test_job_class',
            jobconf_args={
                'pinball.key1': 'value1',
                'pinball.key2': 'value2',
            },
            extra_args=[
                '-Dpinball.key3=value3'
            ])

        self.assertEqual(executor.config.USER_LIBJAR_DIRS, user_jar_dirs)
        self.assertEqual(executor.config.USER_APPJAR_PATH, user_app_jar)
        self.assertEqual(executor.config.USER_ARCHIVE_PATH, user_archive)
        # self.assertEqual(executor.config.PLATFORM, Platform.LOCAL)
        f1.assert_called_once_with(
            "export HADOOP_CLASSPATH=/dir1/jar1/*:/dir2/jar2/*; "
            "hadoop jar /dir/jar.jar test_job_class "
            "-libjars `echo /dir1/jar1/*.jar /dir2/jar2/*.jar | tr ' ' ','` "
            "-Dmapred.job.name=test_user:AdHocCommand "
            "-Dpinball.key2=value2 "
            "-Dpinball.key1=value1 "
            "-Dpinball.key3=value3")
