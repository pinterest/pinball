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

"""Utilities for executing commands in a Hadoop cluster.

Commands are executed by SSHing to the Hadoop cluster and performing
the desired command.

The user of scripts which use these functions should either have their SSH key
on the Hadoop cluster's authorized users list or have the hadoop_identity
SSH key pair available to them.
"""

import logging
import os
import re
import sys
import tempfile

from pinball_ext.common import output_filter
from pinball_ext.common import shell_utils


__author__ = 'Dmitry Chechik, Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = logging.getLogger('common.hadoop_utils')
LOG.setLevel(logging.INFO)

_HADOOP_SSH_OPTS = [
    '-o', 'UserKnownHostsFile=/dev/null',
    '-o', 'StrictHostKeyChecking=no',
    '-o', 'LogLevel=error',
    # Prevent timeout disconnects.
    '-o', 'ServerAliveInterval=60',
]


# Default parameters to connect to a node in a Hadoop cluster.
class HadoopHostConfig(object):
    # User used in SSH.
    USER_NAME = 'hadoop'
    # Host to SSH to.
    HOST_NAME = None
    # Port which we can SSH to.
    SSH_PORT = 22
    # Path for the local ssh identity file.
    SSH_KEY_FILE = '%s/.ssh/hadoop_identity' % \
                   os.environ.get('HOME', '/home/prod')
    # Home path on Hadoop host.
    REMOTE_HADOOP_HOME = '/home/hadoop'

    def __eq__(self, other):
        if type(self) != type(other):
            raise ValueError('comparing apples and carrots (%s)' % type(other))
        return (self.USER_NAME == other.USER_NAME and
                self.HOST_NAME == other.HOST_NAME and
                self.SSH_PORT == other.SSH_PORT and
                self.SSH_KEY_FILE == other.SSH_KEY_FILE and
                self.REMOTE_HADOOP_HOME == other.REMOTE_HADOOP_HOME)


def run_command_in_hadoop(hadoop_node_config,
                          command,
                          cmd_work_dir=None,
                          check_output=False):
    """Run the given command in the Hadoop cluster.

    The caller has to finish reading the output to make sure
    the command completes. This method returns as soon as the command is
    invoked.

    Usage:
        with run_hadoop_command('ls -l') as f:
            result = f.read()

    Args:
        hadoop_node_config - Where to SSH and run this command.
        command - Command to run.
        cmd_work_dir - Path on hadoop cluster to run from.

    Returns:
        A file-like object to the stdout of the command.
    """
    if not cmd_work_dir:
        cmd_work_dir = hadoop_node_config.REMOTE_HADOOP_HOME

    key_file_path = os.path.expanduser(hadoop_node_config.SSH_KEY_FILE)
    if not os.path.exists(key_file_path):
        LOG.warn('SSH key %s does not exist.' % hadoop_node_config.SSH_KEY_FILE)
        key_file_path = None

    return shell_utils.run_ssh_command(
        user_name=hadoop_node_config.USER_NAME,
        host_name=hadoop_node_config.HOST_NAME,
        command=command,
        cmd_work_dir=cmd_work_dir,
        ssh_port=hadoop_node_config.SSH_PORT,
        ssh_options=_HADOOP_SSH_OPTS,
        ssh_key_file=key_file_path,
        check_output=check_output)


def run_and_check_command_in_hadoop(hadoop_node_config,
                                    command,
                                    cmd_work_dir=None,
                                    log_line_processor=None):
    """Run the given command on the Hadoop cluster.

    Args:
        hadoop_node_config - where to SSH to and run this command.
        command - Command to run.
        cmd_work_dir - Path on hadoop cluster to run from.
        log_line_processor - Processor to process the command stdoutput.

    Raises:
        CalledProcessError if it fails.
    """
    if not log_line_processor:
        run_command_in_hadoop(hadoop_node_config, command, cmd_work_dir, check_output=True)
    else:
        command_output = run_command_in_hadoop(hadoop_node_config, command, cmd_work_dir)
        out_filter = output_filter.OutputFilter(log_line_processor,
                                                output=sys.stderr)
        out_filter.read_and_output(command_output)


def hdfs_exists(hadoop_node_config, file_pattern):
    """Return true iff file_pattern exists on HDFS or S3.

    Args:
        hadoop_node_config - where to SSH to and run this command.
        file_pattern - string file name or glob-like pattern.
    """
    cmd = 'hadoop fs -ls %s 2>&1 > /dev/null; echo $?' % file_pattern
    with run_command_in_hadoop(hadoop_node_config, cmd) as f:
        output = f.read()
    return output.strip() == '0'


def hdfs_rmr(file_pattern, safety_check=None, date_partitioned_check=False):
    """Remove recursively a file or file pattern from HDFS or S3.

    Args:
        file_pattern - File to remove.
        safety_check - If present, will check for existence
                       of the string in file_pattern to
                       double-check that we're not deleting
                       an unexpected path.
        date_partitioned_check - If set to true, exception is raised
                       if the directory doesn't end with 'YYYY-MM-DD'
    Raises:
        CalledProcessError on error.
    """
    DATE_PARTITION_REGEX = r'(.*\d{4}-\d{2}-\d{2}/*)'
    if date_partitioned_check:
        if not re.match(DATE_PARTITION_REGEX, file_pattern):
            raise Exception('Attempted to delete non date partitioned dir %s'
                            % file_pattern)

    if safety_check:
        assert(safety_check in file_pattern)
    cmd = 'hadoop fs -rmr %s' % file_pattern
    run_and_check_command_in_hadoop(cmd)


def put_string_to_hadoop(hadoop_node_config, input_string, remote_path):
    """Takes the passed-in string and writes it to a file on a Hadoop node.

    NOTE: we are not putting to HDFS.

    Args:
        input_string - String to write.
        remote_path - Absolute path on Hadoop node's local disk to write to.

    Raises:
        CalledProcessError on error.
    """
    tmp_file = tempfile.NamedTemporaryFile()
    try:
        tmp_file.write(input_string)
        tmp_file.flush()
        shell_utils.scp_file_to_host(
            hadoop_node_config.USER_NAME,
            hadoop_node_config.HOST_NAME,
            tmp_file.name,
            remote_path,
            ssh_options=_HADOOP_SSH_OPTS,
            ssh_port=hadoop_node_config.SSH_PORT,
            ssh_key_file=hadoop_node_config.SSH_KEY_FILE)
    finally:
        tmp_file.close()
