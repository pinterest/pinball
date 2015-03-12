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

""" The job_runner.py command is responsible for running any *single* Job.

    It supports Bash scripts, Python scripts, Hive scripts and Hadoop jobs.

    It will either be triggered by Pinball or manually via command line and it's
    responsible for parsing and passing along job specific parameters.

    Parameters:
    See _get_cmd_line_options() for supported parameters.

    The required parameters are:
        - job_import_dirs_config: python var name which stores the dirs where
            all job classes are defined.
        - job_class_name: name of the job class we are going to run.

    Users can pass arguments to their Jobs using the --job_params parameter.

    E.g.
    python pinball_ext/job/job_runner.py \
    --job_class_name=SampleReportHiveJob \
    --job_params="start_date=2013-07-27,end_date=2013-07-27" \

    It is up to individual jobs to support date ranges and produce the expected
    output.
"""

import fcntl
import optparse
import traceback

from pinball_ext.common import utils
from pinball_ext.job import job_module


__author__ = 'Changshu Liu, Mohammad Shahangian, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = utils.get_logger('pinball_ext.job.job_runner')


def _acquire_exclusive_lock(write_lock_name):
    """Acquires a lock with a given name.

    Underneath we create a lock file and return a descriptor of that file. The
    lock will be held as long as this file descriptor is open.
    """
    lock_filename = '/var/lock/%s.lock' % write_lock_name
    LOG.info('Acquiring lock %s ...', lock_filename)

    lock_file = open(lock_filename, 'w')
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
    LOG.info('Acquired lock %s.', lock_filename)
    return lock_file


def _get_job_import_dirs(job_import_dirs_config):
    """Get the list of dirs where job classes are defined.

    Args:
        job_import_dirs_config: the variable name that stores a list of dirs
    where job classes are defined.

    NOTE:
        If what user specified is a full qualified variable name, we need to
        import the module first, otherwise, Python won't be able to interpret
        the variable nae correctly.
    """
    if "." in job_import_dirs_config:
        module_name = '.'.join(job_import_dirs_config.split('.')[:-1])
        exec('import %s' % module_name)
    return eval(job_import_dirs_config)


def _get_cmd_line_options():
    parser = optparse.OptionParser()
    parser.add_option("--job_class_name",
                      dest="job_class_name",
                      help="The class name of the job to run.")
    parser.add_option("--dry_run",
                      dest="dry_run",
                      action="store_true",
                      help="If true, don't execute or complete the jobs.")
    parser.add_option("--job_params",
                      dest="job_params",
                      default="",
                      help="These parameters are passed as 'params=' to the job class, and then "
                           "passed along all the way through the job hierarchy. Any arguments "
                           "that user logic might need to access should be put here. Format are "
                           "comma delimited and each have the format '{key}={value}'.")
    parser.add_option("--write_lock_name",
                      dest="write_lock_name",
                      default="",
                      help="Will require an exclusive lock with a given name before running the job. "
                           "Locks are enforced only among jobs running on the same machine. "
                           "The corresponding lock file is named /var/lock/<write_lock_name>.lock")
    parser.add_option("--executor",
                      dest="executor",
                      default="",
                      help="The name of the main executor for the job. The list of settings is in "
                           "pinball_ext/executor/common.py.")
    parser.add_option("--executor_config",
                      dest="executor_config",
                      default="",
                      help="These settings are passed along to all of the Executor constructors. "
                           "They are for overriding the default Executor configuration settings. "
                           "Settings are comma delimited and each have the format '{key}={value}'.")
    parser.add_option("--job_import_dirs_config",
                      dest="job_import_dirs_config",
                      default="",
                      help="Full qualified Python variable name which stores the list of dirs that "
                           "contains job definition files.")

    (options, args) = parser.parse_args()
    return parser, options, args


def main():
    parser, opts, args = _get_cmd_line_options()

    # Load job Python class
    assert opts.job_import_dirs_config, '--job_import_dirs_config not set.'
    job_import_dirs = _get_job_import_dirs(opts.job_import_dirs_config)
    LOG.info('loading job classes from: %s', '\n\t'.join(job_import_dirs))

    job_class_name = opts.job_class_name
    job_py_class = job_module.get_py_class_by_name(job_import_dirs,
                                                   job_class_name)
    if not job_py_class:
        all_class_names = '\n\t' + '\n\t'.join(
            job_module.get_sorted_job_names(job_import_dirs))
        LOG.error('job_class_name:%s not found, it should be one of:%s',
                  job_class_name,
                  all_class_names)
        raise Exception('Python class with name: %s not found' % job_class_name)

    # Executor related settings.
    settings = {}
    if opts.executor:
        settings['executor'] = opts.executor
    executor_config = utils.parse_arguments(opts.executor_config)
    settings['executor_config'] = executor_config

    # Config job parameters
    params = utils.parse_arguments(opts.job_params)

    # Run the job
    lock_file = None
    if opts.write_lock_name:
        lock_file = _acquire_exclusive_lock(opts.write_lock_name)

    try:
        LOG.info("starts to run job: %s on executor: %s ...",
                 job_class_name,
                 opts.executor)

        # Create the job object
        job_object = job_py_class(params=params, settings=settings)
        job_object.runjob(dry_run=opts.dry_run)
    except Exception:
        LOG.warning("Error: %s", traceback.format_exc())
        LOG.error("job failed.")
        raise
    finally:
        if lock_file:
            lock_file.close()

    LOG.info("job succeeded.")


if __name__ == "__main__":
    main()
