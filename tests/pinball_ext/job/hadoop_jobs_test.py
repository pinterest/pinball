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
from pinball_ext.executor.common import Platform
from pinball_ext.job import hadoop_jobs


__author__ = 'Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'

_HADOOP_TEST_JOB_CLASS_NAME = '_NON_EXISTS_CLASS_NAME_'

_PARAMS = {
    'start_date': '2015-01-01',
    'end_date': '2015-01-07',
}

_SETTINGS = {
    'executor': Platform.QUBOLE,
    'executor_config': {
        'USER_APPJAR_PATH': 's3n:/my_bucket/my_dir/my_jar.jar',
    }
}

_JOB_STDOUT = 'stdout'
_JOB_STDERR = 'stderr'
_JOB_IDS = ['9527']

_JOBCONF_ARGS = {
   'job_conf_arg1': 'value1',
   'job_conf_arg2': 'value2',
}

_EXTRA_JARS = ['s3n://my_bucket/my_dir/myjar1.jar',
               's3n://my_bucket/my_dir/myjar2.jar']


class HadoopTestJob(hadoop_jobs.HadoopJob):
    def _setup(self):
        self.jobconf_args = _JOBCONF_ARGS
        self.extra_jars = _EXTRA_JARS
        self.extra_arguments = ['-arg value']

    def _get_class_name(self):
        return _HADOOP_TEST_JOB_CLASS_NAME


def _mock_qubole_run_hadoop_job(class_name,
                                jobconf_args,
                                extra_args,
                                extra_jars):
    return _JOB_STDOUT, _JOB_STDERR, _JOB_IDS


class HadoopJobTestCase(unittest.TestCase):
    @mock.patch('pinball_ext.executor.qubole_executor.QuboleExecutor.__init__',
                mock.Mock(return_value=None))
    @mock.patch('pinball_ext.executor.qubole_executor.QuboleExecutor.run_hadoop_job',
                side_effect=_mock_qubole_run_hadoop_job)
    def test_hadoop_job(self, f1):
        hjob = HadoopTestJob(params=_PARAMS, settings=_SETTINGS)
        hjob.runjob()
        self.assertEquals(hjob._job_output, _JOB_STDOUT)
        self.assertEquals(hjob._job_stderr, _JOB_STDERR)
        self.assertEquals(hjob._job_ids, _JOB_IDS)
        f1.assert_called_once_with(
            _HADOOP_TEST_JOB_CLASS_NAME,
            jobconf_args=_JOBCONF_ARGS,
            extra_args=['-start_date 2015-01-01', '-end_date 2015-01-07', '-arg value'],
            extra_jars=_EXTRA_JARS)
