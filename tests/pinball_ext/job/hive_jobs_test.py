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
from pinball_ext.job import hive_jobs


__author__ = 'Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'

_Q_TEMPLATE = """SELECT * FROM unknown_db.unknown_tb WHERE dt >= '%(start_date)s' AND dt <= '%(end_date)s'"""

_PARAMS = {
    'start_date': '2015-01-01',
    'end_date': '2015-01-07',
}

_SETTINGS = {
    'executor': Platform.EMR,
    'executor_config': {
        'USER_APPJAR_PATH': 's3n:/my_bucket/my_dir/my_jar.jar',
    }
}

_JOB_STDOUT = 'stdout'
_JOB_STDERR = 'stderr'
_JOB_IDS = ['9527']


class HiveTestJob(hive_jobs.HiveJob):
    _QUERY_TEMPLATE = _Q_TEMPLATE


class HiveFileTestJob(hive_jobs.HiveFileJob):
    # Check HiveFileJob to see where the dir starts.
    _QUERY_TEMPLATE_FILE = '../../tests/pinball_ext/job/hive_test_query.ql'


def _mock_emr_run_hive_query_job(q_str, upload_archive):
    return _JOB_STDOUT, _JOB_STDERR, _JOB_IDS


class HiveJobsTestCase(unittest.TestCase):
    @mock.patch('pinball_ext.job.basic_jobs.ClusterJob._is_prod_job',
                mock.Mock(return_value=False))
    @mock.patch('pinball_ext.executor.emr_executor.EMRExecutor.__init__',
                mock.Mock(return_value=None))
    @mock.patch('pinball_ext.executor.emr_executor.EMRExecutor.run_hive_query',
                side_effect=_mock_emr_run_hive_query_job)
    def test_hive_query_job(self, f1):

        hive_job = HiveTestJob(params=_PARAMS, settings=_SETTINGS)
        hive_job.runjob()
        self.assertEquals(hive_job._job_output, _JOB_STDOUT)
        self.assertEquals(hive_job._job_stderr, _JOB_STDERR)
        self.assertEquals(hive_job._job_ids, _JOB_IDS)
        f1.assert_called_once_with((_Q_TEMPLATE % _PARAMS),
                                   upload_archive=False)

    @mock.patch('pinball_ext.job.basic_jobs.ClusterJob._is_prod_job',
                mock.Mock(return_value=False))
    @mock.patch('pinball_ext.executor.emr_executor.EMRExecutor.__init__',
                mock.Mock(return_value=None))
    @mock.patch('pinball_ext.executor.emr_executor.EMRExecutor.run_hive_query',
                side_effect=_mock_emr_run_hive_query_job)
    def test_hive_file_job(self, f1):
        hive_job = HiveFileTestJob(params=_PARAMS, settings=_SETTINGS)
        hive_job.runjob()
        self.assertEquals(hive_job._job_output, _JOB_STDOUT)
        self.assertEquals(hive_job._job_stderr, _JOB_STDERR)
        self.assertEquals(hive_job._job_ids, _JOB_IDS)
        f1.assert_called_once_with((_Q_TEMPLATE % _PARAMS),
                                   upload_archive=False)
