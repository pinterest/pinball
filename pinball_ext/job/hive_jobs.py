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

import os

from pinball_ext.common import utils
from pinball_ext.job.basic_jobs import ClusterJob


__author__ = 'Changshu Liu, Mao Ye, Mohammad Shahangian'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = utils.get_logger('pinball_ext.job.hive_jobs')


class HiveJobBase(ClusterJob):
    """Base class for jobs that run Hive query."""

    # If set to true, upload archive; otherwise don't upload archive
    _UPLOAD_ARCHIVE = False

    def _get_query_template(self):
        """Get the hive query template as a string.

        The returned template may contain some place holder parameters that will
        be replaced with self.params.
        """
        raise NotImplementedError("No query template available in HiveJobBase")

    def _setup(self):
        super(HiveJobBase, self)._setup()
        self._delay()

    def _execute(self):
        super(HiveJobBase, self)._execute()
        self._job_output, self._job_stderr, self._job_ids =\
            self.executor.run_hive_query(
                self._get_query_template() % self.params,
                upload_archive=self._UPLOAD_ARCHIVE)


class HiveJob(HiveJobBase):
    """A job to run Hive query whose template defined as job attribute.

    It retrieves job attribute: _QUERY_TEMPLATE as query template.
    """

    _QUERY_TEMPLATE = None

    def _get_query_template(self):
        if not self._QUERY_TEMPLATE:
            raise Exception('_QUERY_TEMPLATE is empty')

        return self._QUERY_TEMPLATE


class HiveFileJob(HiveJobBase):
    """A job to run Hive query whose template is stored in a file.

    The file path is specified in job attribute: _QUERY_TEMPLATE_FILE. The path
    is relative to job's root_dir attributed which is the dir where HiveFileJob
    class is defined by default. Derived job class can override _setup() method
    to override this attribute.
    """

    _QUERY_TEMPLATE_FILE = None

    def _setup(self):
        super(HiveFileJob, self)._setup()
        self.root_dir = os.path.dirname(__file__)

    def _get_query_template(self):
        if not self._QUERY_TEMPLATE_FILE:
            raise NotImplementedError('_QUERY_FILE is empty')

        query_file_path = os.path.join(self.root_dir, self._QUERY_TEMPLATE_FILE)
        LOG.info('reading hive query template from: %s ...', query_file_path)
        with open(query_file_path, 'r') as f:
            query_template = f.read()
            return query_template
