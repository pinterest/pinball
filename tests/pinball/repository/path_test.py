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

"""Validation tests for operations on repository paths."""
import unittest

from pinball.repository.path import Path


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class PathTestCase(unittest.TestCase):
    def test_workflow_prefix(self):
        path = Path(workflow='some_workflow')
        self.assertEqual('/workflow/some_workflow/',
                         path.get_workflow_prefix())

    def test_job_prefix(self):
        path = Path(workflow='some_workflow', job='some_job')
        self.assertEqual('/workflow/some_workflow/job/', path.get_job_prefix())

    def test_schedule_path(self):
        path = Path(workflow='some_workflow')
        self.assertEqual('/workflow/some_workflow/schedule',
                         path.get_schedule_path())

    def test_job_path(self):
        path = Path(workflow='some_workflow', job='some_job')
        self.assertEqual('/workflow/some_workflow/job/some_job',
                         path.get_job_path())
