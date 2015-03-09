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

from pinball_ext.job import basic_jobs
from pinball_ext.common import import_utils


__author__ = 'Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


_JOB_MODULES = None


def get_job_modules(import_dirs):
    global _JOB_MODULES
    if not _JOB_MODULES:
        _JOB_MODULES = import_utils.ModuleImport(import_dirs,
                                                 basic_jobs.JobBase)
        _JOB_MODULES.import_all_modules()
    return _JOB_MODULES


def get_py_class_by_name(job_import_dirs, job_class_name):
    job_modules = get_job_modules(job_import_dirs)
    return job_modules.get_class_by_name(job_class_name)


def get_sorted_job_names(job_import_dirs):
    job_modules = get_job_modules(job_import_dirs)
    job_names = job_modules.get_all_class_names()
    return sorted(job_names)
