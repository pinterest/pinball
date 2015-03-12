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

"""Generic workflow-related utilities."""
import importlib
import os
import time

from pinball.config.pinball_config import PinballConfig


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


def get_unique_workflow_instance():
    """Generate a unique workflow instance name.

    Returns:
        A timestamp-based workflow instance name.
    """
    # We don't expect instance names to be generated for the same workflow
    # at sub-millisecond intervals so timestamp should be good enough.
    return str(int(time.time() * 1000))


def load_path(path):
    """Load an object identified by a fully qualified python path.

    Args:
        path: The path of the object to load.
    Returns:
        The object identified by the path.
    """
    index = path.rfind('.')
    if index == -1:
        return None
    module_name = path[:index]
    attribute_name = path[(index + 1):]
    module = importlib.import_module(module_name)
    return getattr(reload(module), attribute_name)


def get_logs_dir(workflow, instance,
                 parent_log_directory=PinballConfig.LOCAL_LOGS_DIR):
    """Get name of the directory where workflow instance logs are stored.

    Args:
        workflow: Name of the workflow whose logs are stored in the returned
            directory.
        instance: Name of the workflow instance whose logs are stored in the
            returned directory.
        parent_log_directory: Name of the parent directory where workflow instance
            logs are stored.
    Returns:
        Name of the directory where workflow instance logs are stored.
    """
    return os.path.join(parent_log_directory, workflow, instance)
