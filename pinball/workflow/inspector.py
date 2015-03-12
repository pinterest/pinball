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

"""Inspector supports traversal of the token tree hierarchy.

In particular, inspector comes handy when we work with tokens hierarchies that
contain name components unknown to the client.  Inspector understands the
hierarchical names of workflow tokens and exposes an interface to traverse
those names level-by-level.  E.g., we may use the inspector to find all
workflow instance ids or all waiting jobs in a given workflow isntance.
"""


from pinball.master.thrift_lib.ttypes import GroupRequest
from pinball.workflow.name import Name


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class Inspector(object):
    def __init__(self, client):
        self._client = client

    def get_workflow_names(self):
        """Return list of workflow names."""
        request = GroupRequest()
        request.namePrefix = Name.WORKFLOW_PREFIX
        request.groupSuffix = Name.DELIMITER
        response = self._client.group(request)
        workflow_names = []
        if response.counts:
            for prefix in response.counts.keys():
                name = Name.from_workflow_prefix(prefix)
                if name.workflow:
                    workflow_names.append(name.workflow)
        return workflow_names

    def get_workflow_instances(self, workflow_name):
        """Return list of instances of a given workflow."""
        request = GroupRequest()
        name = Name()
        name.workflow = workflow_name
        request.namePrefix = name.get_workflow_prefix()
        request.groupSuffix = Name.DELIMITER
        response = self._client.group(request)
        instance_names = []
        if response.counts:
            for prefix in response.counts.keys():
                name = Name.from_instance_prefix(prefix)
                if name.instance:
                    instance_names.append(name.instance)
        return instance_names

    def _get_job_names(self, workflow_name, instance, state):
        """Return list of job names in a given workflow instance and state.

        E.g., assume the following tokens are stored in the master:
            /workflow/some_workflow/12345/waiting/some_waiting_job
            /workflow/some_workflow/12345/waiting/some_other_waiting_job
            /workflow/some_workflow/12345/runnable/some_runnable_job

        the method called with workflow_name=some_workflow, instance=12345,
        state=waiting will return [some_waiting_job, some_other_waiting_job].
        """
        request = GroupRequest()
        name = Name()
        name.workflow = workflow_name
        name.instance = instance
        name.job_state = state
        request.namePrefix = name.get_job_state_prefix()
        request.groupSuffix = Name.DELIMITER
        response = self._client.group(request)
        job_names = []
        if response.counts:
            for job_name in response.counts.keys():
                name = Name.from_job_token_name(job_name)
                job_names.append(name.job)
        return job_names

    def get_runnable_job_names(self, workflow_name, instance):
        """Return names of runnable jobs in a given workflow instance."""
        return self._get_job_names(workflow_name,
                                   instance,
                                   Name.RUNNABLE_STATE)

    def get_waiting_job_names(self, workflow_name, instance):
        """Return names of waiting jobs in a given workflow instance."""
        return self._get_job_names(workflow_name,
                                   instance,
                                   Name.WAITING_STATE)

    def get_event_names(self, workflow_name, instance, job, input_name):
        """Return names of events under a workflow instance, job, and input."""
        request = GroupRequest()
        name = Name()
        name.workflow = workflow_name
        name.instance = instance
        name.job = job
        name.input = input_name
        request.namePrefix = name.get_input_prefix()
        request.groupSuffix = Name.DELIMITER
        response = self._client.group(request)
        events = []
        if response.counts:
            for event in response.counts.keys():
                name = Name.from_event_token_name(event)
                events.append(name.event)
        return events
