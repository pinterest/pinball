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

"""Generator of SVG diagram representing a given workflow instance."""
# pydot invokes 'dot' binary from the graphviz library.  This binary has to be
# accessible through $PATH.
import pydot

from pinball.ui.data import Status
from pinball.ui.utils import get_workflow_jobs_from_parser


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class WorkflowGraph(object):
    def __init__(self, jobs_data, instance_data):
        self._jobs_data = jobs_data
        self._instance_data = instance_data
        self._graph = pydot.Dot(graph_type='graph')
        # Graph id should be in sync with the value defined in
        # templates/jobs.html
        self._graph.set_id('workflow_graph')
        self._graph.set_orientation('portrait')
        self._build()

    @staticmethod
    def from_config(config, workflow):
        data_jobs = get_workflow_jobs_from_parser(workflow, config)
        return WorkflowGraph(data_jobs, None)

    @staticmethod
    def from_parser(workflow, config=None):
        data_jobs = get_workflow_jobs_from_parser(workflow, config)
        return WorkflowGraph(data_jobs, None)

    def _build(self):
        for job in self._jobs_data:
            node = pydot.Node(job.job)
            self._graph.add_node(node)
            node.set_id(job.job)
            node.set_fillcolor(Status.COLORS[job.status])
            node.add_style('filled')
            if job.is_condition:
                node.set_shape('octagon')
                if (job.status == Status.FAILURE and
                        self._instance_data and
                        self._instance_data.status == Status.RUNNING):
                    node.set_fillcolor(Status.COLORS[Status.CONDITION_PENDING])
            for output in job.outputs:
                edge = pydot.Edge(job.job, output)
                self._graph.add_edge(edge)
                edge.set_id('%s/%s' % (job.job, output))

    def get_svg(self):
        return self._graph.create_svg()
