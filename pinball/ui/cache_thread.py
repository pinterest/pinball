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

import json
import time

from threading import Thread

from pinball.config.utils import get_log
from pinball.ui.data_builder import DataBuilder

LOG = get_log('pinball.ui.cache_thread')


__author__ = 'Julia Oh, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = ['Julia Oh', 'Mao Ye']
__license__ = 'Apache'
__version__ = '2.0'


def start_cache_thread(dbstore):
    """Creates and starts a daemon thread for workflow data computation.

    This method is called when pinball ui server starts.

    Args:
        dbstore: The store to retrieve runs status.
    Returns:
        cache_thread
    """
    thread = Thread(target=_compute_workflow, args=[dbstore])
    thread.daemon = True
    thread.start()
    return thread


def _compute_workflow(dbstore):
    """Cache thread's target callable that computes the workflow.

    This runnable is called my thread's run() method when thread
    starts. It will compute workflows data, serialize it, and store it
    in _WORKFLOW_JSON. This computation will infinitely
    repeat itself, constantly updating the _WORKFLOW_JSON until pinball_ui
    server stops.

    Args:
        dbstore: The store to retrieve runs status.
    """
    global _WORKFLOWS_JSON
    data_builder = DataBuilder(dbstore, use_cache=True)
    while True:
        try:
            LOG.info("Workflow data computation starting.")
            workflows_data = data_builder.get_workflows()
            schedules_data = data_builder.get_schedules()
            _WORKFLOWS_JSON = _serialize(workflows_data, schedules_data)
            LOG.info("Workflow data computation complete.")
            # TODO(mao): Tune this parameter depending on future
            # pinball user experience.
            # TODO(mao): Make this computation run at scheduled time intervals
            # and cancel the next execution if the previous job hasn't
            # finished.
            time.sleep(60 * 20)
        except Exception as e:
            LOG.exception(e)


def _serialize(workflows_data, schedules_data):
    workflow_emails = {}
    workflows_info = []
    for schedule in schedules_data:
        workflow_emails[schedule.workflow] = schedule.emails
    for workflow in workflows_data:
        workflow_data = workflow.format()
        if workflow.workflow in workflow_emails:
            workflow_data['owners'] = ','.join(email.split('@')[0]
                                               for email in workflow_emails[workflow.workflow])
        else:
            workflow_data['owners'] = 'N/A'
        workflows_info.append(workflow_data)
    return json.dumps({'aaData': workflows_info})


def get_workflows_json():
    return _WORKFLOWS_JSON

_WORKFLOWS_JSON = _serialize([], [])

