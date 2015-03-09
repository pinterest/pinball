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

"""Base class for executors running Hadoop/Hive jobs."""

import datetime
import getpass
import os
import re
import subprocess

from pinball_ext.common import hadoop_utils
from pinball_ext.common.decorators import retry
from pinball_ext.common.utils import get_logger


__author__ = 'Zach Drach, Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class ClusterExecutor(object):
    """ A class that implements Hadoop functionality across multiple
    Hadoop cluster types.

    Subclasses should implement the following functions on their platforms:
        - run_hive_query()
        - run_hadoop_job()
        - run_hadoop_streaming_job()
        - kill_job()
        - get_job_result()
        - get_job_resource_dir() [optional]

    The other public functions on the executor class make use of the
    above three functions, making it easy to implement a set of common
    Hadoop/Hive utility functions on multiple platforms.

    All Executor settings are stored in a Config object, described below.
    """
    class Config(object):
        """ An object for defining configuration parameters for this executor.

        All configuration parameters can be overridden in the constructor of the
        executor. These values are just the defaults.
        """
        # The user that is running the commands
        USER = getpass.getuser()

        # TODO(csliu): job name should be a job level property.
        # A label for all hadoop jobs started by the executor
        NAME = "AdHocCommand"

        # All jars under these dirs will be added as libjars. It could be local
        # path or s3/hdfs path, depending on how concrete executor interpret it.
        USER_LIBJAR_DIRS = []

        # This jar contains the main class of Hadoop app.
        USER_APPJAR_PATH = None

        # Archive path for Hadoop application.
        USER_ARCHIVE_PATH = None

        # Scheduling queue.
        SCHEDULER_QUEUE = 'prod_pool' if USER == 'prod' else None
        SCHEDULER_PARAM = None  # platform-specific

        # Name of the cluster platform.
        # pinball_ext.common.PLATFORM contains all platforms.
        PLATFORM = None

        # Configs about a node (usually the master node) inside a Hadoop cluster
        HADOOP_HOST_USER = hadoop_utils.HadoopHostConfig.USER_NAME
        HADOOP_HOST_NAME = hadoop_utils.HadoopHostConfig.HOST_NAME
        HADOOP_HOST_SSH_PORT = hadoop_utils.HadoopHostConfig.SSH_PORT
        HADOOP_HOST_SSH_KEY_FILE = hadoop_utils.HadoopHostConfig.SSH_KEY_FILE
        HADOOP_HOST_HOME = hadoop_utils.HadoopHostConfig.REMOTE_HADOOP_HOME

    def __init__(self, executor_config=None):
        """ Initialize parameters for the data job execution layer.

        Args:
            executor_config: a dictionary of configuration params that override
            the defaults. The key names should be the same as the field names of
            corresponding xxxExecutor.Config class.
        """
        executor_config = executor_config if executor_config else {}

        self.log = get_logger(self.__class__.__name__)

        # Override self.config using executor_config dict.
        self.config = self.Config()
        for key, value in executor_config.items():
            if key == 'USER_LIBJAR_DIRS':
                self.config.USER_LIBJAR_DIRS = value.split(',')
            else:
                setattr(self.config, key, value)

        # Construct HadoopHostConfig object according to overridden config.
        self.hadoop_host_config = self._contruct_hadoop_host_config()

        self.job_ids = []

    def _contruct_hadoop_host_config(self):
        hh_config = hadoop_utils.HadoopHostConfig()
        hh_config.USER_NAME = self.config.HADOOP_HOST_USER
        hh_config.HOST_NAME = self.config.HADOOP_HOST_NAME
        hh_config.SSH_PORT = self.config.HADOOP_HOST_SSH_PORT
        hh_config.SSH_KEY_FILE = self.config.HADOOP_HOST_SSH_KEY_FILE
        hh_config.REMOTE_HADOOP_HOME = self.config.HADOOP_HOST_HOME
        return hh_config

    @property
    def job_name(self):
        return "%s:%s" % (self.config.USER, self.config.NAME)

    def run_hive_query(self, query_str, upload_archive=True):
        """Run a hive query and return the raw results.

        Args:
            query_str: A hive query string.
            upload_archive: If true, we will upload the archive with git code
                before running this command. This is unnecessary for hive
                queries with no dependencies on our code base.

        Return:
            The tuple (output, stderr, job_ids)

            output: a list of rows of the query output. Each row is a
                list of strings.
            stderr: the job stderr in the same format
            job_ids: a list of hadoop job ids as python strings
        """
        raise NotImplementedError(
            "run_hive_query is not supported by this executor.")

    def run_hadoop_streaming_job(self,
                                 mapper,
                                 reducer,
                                 input_path,
                                 output_dir,
                                 partitioner=None,
                                 input_format='TextInputFormat',
                                 output_format='TextInputFormat',
                                 extra_args=None,
                                 extra_jars=None):
        """Run a Hadoop Streaming job on the cluster.

        See http://hadoop.apache.org/common/docs/r0.20.1/streaming.html.

        Args:
            mapper: The mapper command to execute.
            reducer: The reducer command to execute.
            input_path: The input file or directory on HDFS or S3.
            output_dir: Where to write the output on HDFS or S3.
            partitioner: Optional partitioner class name.
            input_format: The inputformat to use.
            output_format: The outputformat to use.
            extra_arguments: List of additional optional arguments to pass into
                the job. (see http://hadoop.apache.org/docs/mapreduce/r0.22.0/
                streaming.html#Specifying+Communication+Formats+in+Detail)
            extra_jars: List of additional jars to pass to hadoop streaming's
                libjars param.

        Returns:
            Same as run_hive_query

        Raises:
            CalledProcessError if job fails.
        """
        raise NotImplementedError(
            "run_hadoop_streaming_job is not supported by this executor.")

    def run_hadoop_job(self,
                       class_name,
                       jobconf_args=None,
                       extra_args=None,
                       extra_jars=None):
        """Runs a hadoop mapreduce job on the cluster.

        Returns stdout and stderr in list-of-lists format

        Args:
            class_name: Java class name for this Hadoop app. This must be in a
                jar defined in self.config.USER_APP_JAR.
            jobconf_args: a dictionary of -D<key>=<value> settings
            extra_args: list of additional args.
            extra_jars: List of extra jars to pass to Hadoop's libjars param.

        Returns:
            Same as run_hive_query

        Raises:
            CalledProcessError if the command fails.
        """
        raise NotImplementedError(
            "run_hadoop_job is not supported by this executor.")

    def kill_job(self, job_id):
        """Kills the Hadoop job with the given id."""
        raise NotImplementedError("kill_job is not supported by this executor.")

    def get_job_result(self, job_id):
        """Retrieves results for previously executed job."""
        raise NotImplementedError("get_job_result is not supported by this executor.")

    def get_job_resource_dir(self, run_as_user):
        """Path to location where job resources (jars, archives) are stored."""
        raise NotImplementedError("get_job_resource_dir is not supported by this executor.")

    def __str__(self):
        return self.__class__.__name__

    def __repr__(self):
        return str(self)

    @retry(subprocess.CalledProcessError, tries=3, delay=1, backoff=2)
    def get_table_description(self, table, database='default'):
        """Return table description string from Hive."""
        rows, stderr, job_ids = self.run_hive_query(
            "USE %s; DESCRIBE EXTENDED %s;" % (database, table),
            upload_archive=False)
        output = '\n'.join([' '.join(row) for row in rows])
        result = re.match(r'.*Detailed Table Information (.*)$',
                          output,
                          re.DOTALL)
        return result.groups()[0]

    @retry(subprocess.CalledProcessError, tries=3, delay=1, backoff=2)
    def get_table_location(self, table, database='default'):
        """Return the location of a table."""
        table_info = self.get_table_description(table, database=database)
        return re.match(r'.*location:([^,]*).*', table_info).groups()[0]

    @retry(subprocess.CalledProcessError, tries=3, delay=1, backoff=2)
    def get_table_locations(self, table, database='default', partition='dt'):
        """Return the locations for each partition of a table."""
        location = self.get_table_location(table, database=database)
        partitions = self.get_partitions(table, database=database)
        return [os.path.join(location, partition, '=', part) for part in partitions]

    @retry(subprocess.CalledProcessError, tries=3, delay=1, backoff=2)
    def drop_partition(self, table, database, partition_name, partition):
        """Drop a partition in given table in Hive."""
        self.run_hive_query(
            "USE %s; ALTER TABLE %s DROP PARTITION(%s='%s');" %
            (database, table, partition_name, partition),
            upload_archive=False)

    @retry(subprocess.CalledProcessError, tries=3, delay=1, backoff=2)
    def drop_partitions_condition(self, table, database, condition):
        """Drop partitions satisfying the given condition."""
        self.run_hive_query(
            "USE %s; ALTER TABLE %s DROP PARTITION(%s);" %
            (database, table, condition),
            upload_archive=False)

    @retry(subprocess.CalledProcessError, tries=3, delay=1, backoff=2)
    def recover_partitions(self, table, database='default'):
        """Recover partitions in given table in Hive."""
        self.run_hive_query(
            "USE %s; ALTER TABLE %s RECOVER PARTITIONS;" %
            (database, table),
            upload_archive=False)

    @retry(subprocess.CalledProcessError, tries=3, delay=1, backoff=2)
    def add_partition(self, table, database, partition_names, partition_values):
        """Add a partition in given table in Hive.

        Args:
            partition_names: an individual partition name or a list of partition
                names if the table has multiple partitions.
            partitions_values: the actual partition(s) corresponding to
                <partition_names>.

        Examples:
            add_partition(table, database, 'epoch', '2013-07-12-00-00')
            add_partition(table,
                          database,
                          ['epoch', 'action'], ['2013-07-12-00-00', 'a'])
        """
        if not isinstance(partition_names, (list, tuple)):
            partition_names = [partition_names]
        if not isinstance(partition_values, (list, tuple)):
            partition_values = [partition_values]
        if len(partition_names) != len(partition_values):
            raise ValueError('Unmatched partition param: %s vs %s' %
                             (partition_names, partition_values))

        partition_str = \
            ','.join(["%s='%s'" % t for t in zip(partition_names, partition_values)])
        self.run_hive_query(
            "USE %s; ALTER TABLE %s ADD IF NOT EXISTS PARTITION(%s);" %
            (database, table, partition_str),
            upload_archive=False)

    @retry(subprocess.CalledProcessError, tries=3, delay=1, backoff=2)
    def get_partitions(self, table, database='default'):
        """Return a list of epoch strings that Hive knows about for this table.

        This will return only the top partition for a multi-partitioned table.
        """
        rows, stderr, job_ids = self.run_hive_query(
            "USE %s; SHOW PARTITIONS %s;" % (database, table),
            upload_archive=False)
        return sorted(set([r[0].split('/')[0].split('=')[1] for r in rows]))

    @retry(subprocess.CalledProcessError, tries=3, delay=1, backoff=2)
    def get_full_partitions(self, table, database='default'):
        """Return a list of partitions for a table.

        Returns:
            a list of (<partition_names>, <partition_values>) tuples, the same
        format as accepted by the add_partition() function.
        """
        rows, stderr, job_ids = self.run_hive_query(
            "USE %s; SHOW PARTITIONS %s;" % (database, table),
            upload_archive=False)
        return [tuple(zip(*[pair.split('=') for pair in row[0].split('/')]))
                for row in rows]

    def get_available_dates(self, table, database='default'):
        """Return a list of datetime.dates that Hive knows for this table."""
        partitions = self.get_partitions(table, database=database)
        # Each row is of the form utc_date=<date>, so parse out the date part.
        dates = [datetime.datetime.strptime(r, "%Y-%m-%d").date()
                 for r in partitions if not r.startswith('_distcp')]
        return dates

    @retry(subprocess.CalledProcessError, tries=3, delay=1, backoff=2)
    def does_table_exist(self, table, database='default'):
        """Return True if the table exists in the database, else False."""
        rows, stderr, job_ids = self.run_hive_query(
            "USE %s; DESCRIBE EXTENDED %s;" % (database, table),
            upload_archive=False)
        for row in rows[0]:
            if row.startswith('Table %s does not exist' % table):
                return False
        return True

    def get_table_latest_date_partition(self, table, database='default'):
        """Get the latest date partition for the given table."""
        if not self.does_table_exist(table, database=database):
            return False
        existing_parts = self.get_available_dates(table, database=database)
        if not len(existing_parts):
            return None
        return existing_parts[-1]

    @retry(subprocess.CalledProcessError, tries=3, delay=1, backoff=2)
    def get_partition_description(self, table, partition, database='default'):
        """Return partition description string from Hive."""
        rows, stderr, job_ids = self.run_hive_query(
            "USE %s; DESCRIBE EXTENDED %s PARTITION( %s );" %
            (database, table, partition),
            upload_archive=False)
        return rows[-1][1]

    @retry(subprocess.CalledProcessError, tries=3, delay=1, backoff=2)
    def get_partition_location(self, table, partition, database='default'):
        """Return the location of table."""
        part_info = self.get_partition_description(table,
                                                   partition,
                                                   database=database)
        return re.match(r'.*location:([^,]*).*', part_info).groups()[0]

    @retry(subprocess.CalledProcessError, tries=3, delay=1, backoff=2)
    def alter_table_set_location(self, table, location, database='default'):
        """Set the table location in Hive.

        Args:
            table: table to set.
            location: path on S3 or HDFS.
        """
        self.run_hive_query("USE %s; ALTER TABLE %s SET LOCATION '%s';" %
                            (database, table, location),
                            upload_archive=False)

    @retry(subprocess.CalledProcessError, tries=3, delay=1, backoff=2)
    def alter_table_rename(self, table, new_name, database):
        """Rename the table in Hive.

        Args:
            table: table to rename.
            new_name: new name for the table.
        """
        self.run_hive_query("USE %s; ALTER TABLE %s RENAME TO %s;" %
                            (database, table, new_name),
                            upload_archive=False)

    ###########################################
    # Private helper functions
    ###########################################

    def _generate_hive_query_header(self, upload_archive=False):
        """Generates a string to set Hive environment variables.

        It requires that self._upload_archive() has been implemented by the
        subclass.

        Args:
            upload_archive: a boolean indicating whether the archive should be
                uploaded and added to the query header.
        """
        full_query_string = ''
        if upload_archive and self.config.USER_ARCHIVE_PATH:
            uploaded_archive_path = self._upload_archive()
            full_query_string += "add archive %s;\n" % uploaded_archive_path

        full_query_string += 'set mapred.job.name=%s;\n' % self.job_name

        return full_query_string

    def _upload_archive(self):
        """Uploads self.config.USER_ARCHIVE_PATH to the cluster and returns
        the path to the uploaded archive.

        The returned path may be an S3 path or a path on the Hadoop master
        node depending on the platform.

        file to upload: self.config.USER_ARCHIVE_PATH

        Returns:
            The path where self.config.USER_ARCHIVE_PATH is uploaded to.
        """
        raise NotImplementedError("Subclasses should implement this method")

    def _get_scheduler_job_setting(self):
        if not self.config.SCHEDULER_QUEUE:
            return ''
        else:
            return ' -D%s=%s ' % (self.config.SCHEDULER_PARAM,
                                  self.config.SCHEDULER_QUEUE)

    def _get_scheduler_hive_setting(self):
        if not self.config.SCHEDULER_QUEUE:
            return ''
        else:
            # Can't have a trailing space after the new line since the following
            # comment lines can't have a leading space.
            return ' SET %s=%s;\n' % (self.config.SCHEDULER_PARAM,
                                      self.config.SCHEDULER_QUEUE)

    def _get_scheduler_mrjob_setting(self):
        if not self.config.SCHEDULER_QUEUE:
            return ''
        else:
            return ' --jobconf="%s=%s" ' % (self.config.SCHEDULER_PARAM,
                                            self.config.SCHEDULER_QUEUE)
