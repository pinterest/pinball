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

"""Validation tests for our workflow config."""
import unittest

from pinball_ext.examples.workflows import WORKFLOWS


__author__ = 'Pawel Garbacki, Mao Ye, Jooseong Kim'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class WorkflowConfigTestCase(unittest.TestCase):
    def test_pinball_config_parses(self):
        from pinball_ext.workflow.parser import PyWorkflowParser
        params =\
            {'workflows_config': 'pinball_ext.examples.workflows.WORKFLOWS'}
        py_workflow_parser = PyWorkflowParser(params)
        py_workflow_parser.parse_workflows()

    def test_duplicate_job_definition(self):
        jobs = {}
        for workflow_name, workflow in WORKFLOWS.items():
            for job_name, job_config in workflow.jobs.items():
                if not getattr(job_config.template, '_REUSABLE', False):
                    if jobs.get(job_name):
                        raise Exception(
                            "%s is declared in both workflow %s and %s." %
                            (job_name, workflow_name, jobs[job_name]))
                    else:
                        jobs[job_name] = workflow_name
