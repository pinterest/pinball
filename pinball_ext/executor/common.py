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

"""Constants and functions to be shared among all the executors."""


__author__ = 'Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class Platform(object):
    """Enum that defines the available platforms."""
    EMR = 'emr'
    LOCAL = 'local'
    QUBOLE = 'qubole'


def make_local_executor(executor_config):
    # NOTE: import here to avoid cyclic import
    from pinball_ext.executor import local_executor
    return local_executor.LocalExecutor(executor_config)


def make_emr_executor(executor_config):
    # NOTE: import here to avoid cyclic import
    from pinball_ext.executor import emr_executor
    return emr_executor.EMRExecutor(executor_config)


def make_qubole_executor(executor_config):
    # NOTE: import here to avoid cyclic import
    from pinball_ext.executor import qubole_executor
    return qubole_executor.QuboleExecutor(executor_config)


EXECUTOR_FACTORY_REGISTRY = {
    Platform.EMR: make_emr_executor,
    Platform.LOCAL: make_local_executor,
    Platform.QUBOLE: make_qubole_executor,
}


def make_executor(executor_name, executor_config):
    if executor_name not in EXECUTOR_FACTORY_REGISTRY:
        raise KeyError("Executor with name: %s does not exist, available "
                       "executors are: %s. " % (
                       executor_name,
                       ','.join(EXECUTOR_FACTORY_REGISTRY.keys())))
    executor_factory = EXECUTOR_FACTORY_REGISTRY[executor_name]
    executor_config = executor_config.copy()
    return executor_factory(executor_config)
