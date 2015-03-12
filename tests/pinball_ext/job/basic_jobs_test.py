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

from pinball_ext.job import basic_jobs


__author__ = 'Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class PythonTestJob(basic_jobs.PythonJob):
    def _execute(self):
        out_stdout = "%s/%s" % ('output', 'stdout')
        out_stderr = "%s/%s" % ('output', 'stderr')
        self._job_output = out_stdout
        self._job_stderr = out_stderr
        return


class CommandLineTestJob(basic_jobs.CommandLineJob):
    def _get_command(self):
        return 'echo "hello_world"'

    def _execute(self):
        super(CommandLineTestJob, self)._execute()
        self._job_stderr = 'stderr'
        return


class BasicJobTestCase(unittest.TestCase):
    def test_python_job_dryrun(self):
        job_py = PythonTestJob()
        job_py.runjob(dry_run=True)
        self.assertEquals(job_py._job_output, None)
        self.assertEquals(job_py._job_stderr, None)

    def test_python_job_normal(self):
        job_py = PythonTestJob()
        job_py.runjob()
        self.assertEquals(job_py._job_output, 'output/stdout')
        self.assertEquals(job_py._job_stderr, 'output/stderr')

    def test_command_line_dryrun(self):
        cl_job = CommandLineTestJob()
        cl_job.runjob(dry_run=True)
        self.assertEquals(cl_job._job_output, None)
        self.assertEquals(cl_job._job_stderr, None)

    def test_command_line_normal(self):
        cl_job = CommandLineTestJob()
        cl_job.runjob()
        self.assertEquals(cl_job._job_output, 'hello_world\n')
        self.assertEquals(cl_job._job_stderr, 'stderr')
