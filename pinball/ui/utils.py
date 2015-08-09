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

"""Generic ui-related utilities."""
import pickle

from pinball.config.pinball_config import PinballConfig
from pinball.ui.data import JobData
from pinball.ui.data import Status
from pinball.parser.config_parser import ParserCaller
from pinball.parser.utils import load_parser_with_caller
from pinball.workflow.name import Name


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


def get_workflow_jobs_from_parser(workflow):
    config_parser = load_parser_with_caller(PinballConfig.PARSER,
                                            PinballConfig.PARSER_PARAMS,
                                            ParserCaller.UI)
    tokens = config_parser.get_workflow_tokens(workflow)
    jobs_data = []
    for token in tokens:
        name = Name.from_job_token_name(token.name)
        if name.job:
            assert name.workflow == workflow
            job = pickle.loads(token.data)
            jobs_data.append(JobData(workflow=workflow,
                                     instance=None,
                                     job=name.job,
                                     job_type=job.__class__.__name__,
                                     is_condition=job.IS_CONDITION,
                                     info=job.info(),
                                     inputs=job.inputs,
                                     outputs=job.outputs,
                                     emails=job.emails,
                                     max_attempts=job.max_attempts,
                                     retry_delay_sec=job.retry_delay_sec,
                                     warn_timeout_sec=job.warn_timeout_sec,
                                     abort_timeout_sec=job.abort_timeout_sec,
                                     priority=token.priority,
                                     status=Status.NEVER_RUN))
    return jobs_data
