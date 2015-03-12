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

from pinball_ext.job.hive_jobs import HiveJob


class ShowTableHiveJob(HiveJob):
    _QUERY_TEMPLATE = """
    SHOW TABLES;
    """


class RandomUsersHiveJob(HiveJob):
    _QUERY_TEMPLATE = """
    SELECT *
    FROM pop_names
    WHERE dt < '%(end_date)s'
    """

    def _complete(self):
        super(RandomUsersHiveJob, self)._complete()
        print "job stdout:\n"
        print self._job_output
