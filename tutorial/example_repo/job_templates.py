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

from pinball_ext import job_templates
from pinball_ext.executor.common import Platform


class ExampleQuboleJobTemplate(job_templates.JobTemplate):
    """A JobTemplate to show how to use a specific Qubole cluster to run Jobs.

    Basically, we just need to override get_pinball_job() and fill Qubole
    executor related config k/v into params and then
    JobTemplate.get_pinball_job() will generate correct command line
    accordingly.

    For what kind of information should be configured for Qubole executor,
    please check pinball_ext.executor.QuboleExecutor.
    """
    def __init__(self, name, max_attempts=1, emails=None, priority=None,
                 warn_timeout_sec=None, abort_timeout_sec=None):
        executor_config = {
            # Token for Example Qubole cluster.
            'API_TOKEN': 'your_own_qubole_api_token',
        }

        super(ExampleQuboleJobTemplate, self).__init__(
            name=name,
            executor=Platform.QUBOLE,
            executor_config=executor_config,
            max_attempts=max_attempts,
            emails=emails,
            priority=priority,
            warn_timeout_sec=warn_timeout_sec,
            abort_timeout_sec=abort_timeout_sec)


class ExampleEMRJobTemplate(job_templates.JobTemplate):
    def __init__(self, name, max_attempts=1, emails=None, priority=None,
                 warn_timeout_sec=None, abort_timeout_sec=None):
        executor_config = {
            # Modify these configs according to your EMR settings.

            # The host to ssh to run query/job. Usually, it's the EMR master.
            'HADOOP_HOST_NAME': 'your_own_host_name',
            # The user used to log in the host.
            'HADOOP_HOST_USER': 'your_user_name',
            'HADOOP_HOST_SSH_KEY_FILE': '~/.pinball_oss_emr.pem',
            # The home dir on the host in Hadoop cluster.
            'HADOOP_HOST_HOME': '/home/hadoop_users',
            # Local dirs that contain lib jars for Hadoop jobs.
            # It's relative path under 'HADOOP_HOST_HOME'/'USER'.
            'USER_LIBJAR_DIRS': 'wordcount',
            # Local path for the jar contains the job main class.
            # It's also relative path under 'HADOOP_HOST_HOME'/'USER'.
            'USER_APPJAR_PATH': 'wordcount/wordcount.jar',
        }

        super(ExampleEMRJobTemplate, self).__init__(
            name=name,
            executor=Platform.EMR,
            executor_config=executor_config,
            max_attempts=max_attempts,
            emails=emails,
            priority=priority,
            warn_timeout_sec=warn_timeout_sec,
            abort_timeout_sec=abort_timeout_sec)
