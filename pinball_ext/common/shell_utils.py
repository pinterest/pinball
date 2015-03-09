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

"""Utilities for executing shell commands.

Anything related to running subprocesses, or interacting with the shell or
prompt should go here.
"""

import subprocess

from pinball_ext.common.decorators import retry
from pinball_ext.common.utils import get_logger


__author__ = 'Dmitry Chechik, Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_logger("pinball_ext.common.shell_utils")

_DEFAULT_SSH_OPTS = [
    '-o', 'UserKnownHostsFile=/dev/null',
    '-o', 'StrictHostKeyChecking=no',
    # Suppress warnings while SSHing.
    '-o', 'LogLevel=quiet',
]

_DEFAULT_RSYNC_OPTS = [
    '-u',  # Skip files that are newer on the receiver.
    '-L',  # Transform symlinks to real files.
    '-v',
]


@retry(subprocess.CalledProcessError, tries=3, delay=1)
def scp_file_to_host(user_name,
                     remote_host,
                     local_path,
                     remote_path,
                     ssh_options=_DEFAULT_SSH_OPTS,
                     ssh_port=22,
                     ssh_key_file=None):
    """SCP the local_path file to the remote host.

    Args:
        ``user_name`` - username on remote machine.
        ``host`` - remote host.
        ``local_path`` - path on local machine.
        ``remote_path`` - path on remote machine.
        ``options`` - any additional options.
        ``port`` - port to connect to.
        ``key`` - key file to use for connection, if any.
    Raises:
        CalledProcessError if scp fails.
    """
    scp_command = ['scp'] + ssh_options + ['-P', str(ssh_port)]
    if ssh_key_file:
        scp_command += ['-i', ssh_key_file]
    scp_command += [local_path]
    scp_command += ['%s@%s:%s' % (user_name, remote_host, remote_path)]
    scp_command = ' '.join(scp_command)

    LOG.info('Running command: %s', scp_command)

    subprocess.check_call(scp_command, shell=True)


@retry(subprocess.CalledProcessError, tries=3, delay=1)
def rsync_over_ssh(user_name,
                   remote_host,
                   local_path,
                   remote_path,
                   rsync_options=_DEFAULT_RSYNC_OPTS,
                   ssh_options=_DEFAULT_SSH_OPTS,
                   ssh_port=22,
                   ssh_key_file=None):
    """rsync the local_path to remote host over SSH.

    Args:
        username - username on remote machine.
        host - remote host.
        local_path - path on local machine.
        remote_path - path on remote machine.
        rsync_options - rsync options.
        ssh_options - extra ssh options.
        ssh_port - port to connect to.
        ssh_key_file - key file to use for connection, if any.

    Raises:
        CalledProcessError if scp fails.
    """
    ssh_command = ['ssh'] + ssh_options + ['-p', str(ssh_port)]
    if ssh_key_file:
        ssh_command += ['-i', ssh_key_file]

    command = ['rsync'] + rsync_options + ['-e ' + '\"%s\"' % ' '.join(ssh_command)]
    command += [local_path]
    command += ['%s@%s:%s' % (user_name, remote_host, remote_path)]
    command = ' '.join(command)

    LOG.info('Running command:%s' % command)

    subprocess.check_call(command, shell=True)


def run_ssh_command(user_name,
                    host_name,
                    command,
                    cmd_work_dir,
                    ssh_options=_DEFAULT_SSH_OPTS,
                    ssh_port=22,
                    ssh_key_file=None,
                    check_output=False,
                    forward_agent=False):
    """Run the given SSH command and returns the stdout response.

    Args:
        host - host to SSH to.
        command - The command to run.
        cmd_work_dir - Path on remote host to cd to before running the command.
        username - username to SSH as.
        ssh_options - list of additional options to pass to the SSH command.
        ssh_port - port to SSH to.
        ssh_key_file - Path to a key file to use to SSH.
        check_output - Check that the command ran successfully?
                       If so, this will return nothing and raise a
                       CalledProcessError on error.
                       If not, it will return a file-like object with the
                       command stdout to read.

    Returns:
        A file pointer to the stdout output of the process.
    """
    ssh_command = ['ssh'] + ssh_options + ['-p', str(ssh_port)]
    if ssh_key_file:
        ssh_command += ['-i', ssh_key_file]

    if forward_agent:
        ssh_command += ['-A']

    ssh_command += ['%s@%s' % (user_name, host_name)]
    ssh_command += ["cd %s && %s" % (cmd_work_dir, command)]

    LOG.info('Running command:%s' % ssh_command)

    if check_output:
        return subprocess.check_call(ssh_command)
    else:
        ssh = subprocess.Popen(ssh_command, stdout=subprocess.PIPE)

        return StdoutProcessWrapper(ssh_command, ssh)


class StdoutProcessWrapper(object):
    """A file-like object that wraps around the subprocess.POpen call.

    It acts as a file that will read the data from the popen's stdout,
    and will verify and raise a CalledProcessError if the underlying
    process errors out at any point.

    We can't just return POpen.stdout from these commands, because if
    we do, it won't check if there process exists with a bad return code
    when we read from it.
    """
    def __init__(self, cmd, popen_obj):
        self.popen_obj = popen_obj
        self.cmd = cmd

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # If an exception happened in the body of the with we want to
        # return False to let the exception propagate.
        if exc_type:
            return False

        # Otherwise check that SSH completed succesfully.
        self._verify_success()
        return False

    def _verify_success(self):
        returncode = self.popen_obj.wait()
        if returncode:
            raise subprocess.CalledProcessError(returncode, self.cmd)

    def close(self):
        self._verify_success()

    def next(self):
        try:
            return self.popen_obj.stdout.next()
        except StopIteration as e:
            self.popen_obj.wait()
            self._verify_success()
            raise e

    def __iter__(self):
        while True:
            yield self.next()

    def read(self, bytes_to_read=None):
        if bytes_to_read:
            # Only read bytes_to_read read bytes.
            result = self.popen_obj.stdout.read(bytes_to_read)
            return result
        else:
            # Block until we read everything.
            result = self.popen_obj.stdout.read()
            self._verify_success()
            return result

    def readline(self):
        result = self.popen_obj.stdout.readline()
        if not result:
            self._verify_success()
        return result
