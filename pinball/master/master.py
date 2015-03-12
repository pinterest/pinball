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

"""Command line tool to start token master."""

import argparse
import sys

from pinball.config.pinball_config import PinballConfig
from pinball.master.factory import Factory


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = ['Pawel Garbacki', 'Mao Ye']
__license__ = 'Apache'
__version__ = '2.0'


def main():
    parser = argparse.ArgumentParser(
        description='Start Pinball master server.')
    parser.add_argument(
        '-c',
        '--config_file',
        dest='config_file',
        required=True,
        help='full path to the pinball setting configure file')
    parser.add_argument(
        '-p',
        '--port',
        dest='port',
        type=int,
        default=PinballConfig.MASTER_PORT,
        help='port to run on')
    options = parser.parse_args(sys.argv[1:])

    PinballConfig.parse(options.config_file)
    master_port = options.port if options.port else PinballConfig.MASTER_PORT
    factory = Factory(master_port=master_port)

    # The reason why these imports are not at the top level is that some of the
    # imported code (db models initializing table names) depends on parameters
    # passed on the command line (master name).  Those imports need to be delayed
    # until after command line parameter parsing.
    from pinball.persistence.store import DbStore
    factory.create_master(DbStore())
    factory.run_master_server()

if __name__ == '__main__':
    main()
