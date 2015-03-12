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
import sys
import unittest

from pinball_ext.job import hive_jobs
from pinball_ext.job import job_runner


__author__ = 'Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'

JOB_IMPORT_DIR = [
    'tests/pinball_ext/job'
]

_RUNNER_ARGS = [
    '--job_class_name', 'JobRunnerTestHiveJob',
    '--job_params', 'start_date=2015-01-01,end_date=2015-01-07',
    '--executor', 'emr',
    '--executor_config', 'USER_APPJAR_PATH=s3n:/my_bucket/my_dir/my_jar.jar',
    '--job_import_dirs_config', 'tests.pinball_ext.job.job_runner_test.JOB_IMPORT_DIR',
]

_Q_TEMPLATE = "SELECT * FROM unknown_db.unknown_tb WHERE dt >= '%(start_date)s' AND dt <= '%(end_date)s'"

_PARAMS = {
    'start_date': '2015-01-01',
    'end_date': '2015-01-07',
}


class JobRunnerTestHiveJob(hive_jobs.HiveJob):
    _QUERY_TEMPLATE = _Q_TEMPLATE


def _mock_emr_run_hive_query_job(q_str, upload_archive):
    print(q_str)
    return None, None, []


class JobRunnterTestCase(unittest.TestCase):
    @mock.patch('pinball_ext.job.basic_jobs.ClusterJob._is_prod_job',
                mock.Mock(return_value=False))
    @mock.patch('pinball_ext.executor.emr_executor.EMRExecutor.__init__',
                mock.Mock(return_value=None))
    @mock.patch('pinball_ext.executor.emr_executor.EMRExecutor.run_hive_query',
                side_effect=_mock_emr_run_hive_query_job)
    def test_job_runner_run_job(self, f1):
        sys.argv = [sys.argv[0]] + _RUNNER_ARGS
        job_runner.main()
        f1.assert_called_once_with(_Q_TEMPLATE % _PARAMS, upload_archive=False)
