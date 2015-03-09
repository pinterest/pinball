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

"""Utilities to construct and manipulate config paths.

Config paths hierarchical.  We use '/' as the level separator.

Schedule config is stored in
/workflow/<workflow_name>/schedule

Job config is stored in
/workflow/<workflow_name>/job/<job>
"""


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class Path(object):
    def __init__(self, workflow=None, job=None):
        """Create a path. """
        self.workflow = workflow
        self.job = job

    DELIMITER = '/'
    WORKFLOW_PREFIX = '/workflow/'

    # TODO(pawel): add checks to verify that component names contain
    # alphanumerics and underscores only.

    def get_workflow_prefix(self):
        if self.workflow:
            return '/workflow/%(workflow)s/' % {'workflow': self.workflow}
        return ''

    def get_job_prefix(self):
        if self.workflow:
            return '/workflow/%(workflow)s/job/' % {'workflow': self.workflow}
        return ''

    def get_schedule_path(self):
        if self.workflow:
            return ('/workflow/%(workflow)s/schedule' %
                    {'workflow': self.workflow})
        return ''

    def get_job_path(self):
        if self.workflow and self.job:
            return ('/workflow/%(workflow)s/job/%(job)s' %
                    {'workflow': self.workflow, 'job': self.job})
        return ''
