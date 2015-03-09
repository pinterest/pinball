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

"""Validation tests for blessed version."""
# We don't use 'testing' module since the project will likely move to its own
# repository.
import unittest

from pinball.master.blessed_version import BlessedVersion


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class BlessedVersionTestCase(unittest.TestCase):
    def test_advance_version(self):
        blessed_version = BlessedVersion('some_name', 'some_owner')
        self.assertEqual('some_name', blessed_version.name)
        self.assertEqual('some_owner', blessed_version.owner)
        versions = []
        for _ in range(1, 1000):
            versions.append(blessed_version.advance_version())
        sorted_versions = list(versions)
        sorted_versions.sort()
        self.assertEqual(sorted_versions, versions)
