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

"""Various utilities related to Hive."""

import re


__author__ = 'Zach Drach, Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


def extract_target_tables(hive_query):
    """Extracts the names of the tables that were updated.

    Returns:
        A list of (database,table) names that were updated.
    """
    flags = re.MULTILINE | re.IGNORECASE | re.DOTALL
    pattern = r"\b(?:insert\s+overwrite\s+table\s+'?(?:(\w+)\.)?" \
              r"(\w+)'?\s+partition)|(?:use\s+'?(\w+)'?\s*;)"
    current_database = 'default'
    output = []
    for match in re.finditer(pattern,
                             strip_hive_comments(hive_query),
                             flags):
        groups = match.groups()
        if groups[2] is not None:
            current_database = groups[2]
        else:
            database = groups[0] if groups[0] else current_database
            table = groups[1]
            output.append((database, table))
    return output


def strip_hive_comments(hive_query):
    """Strip the comments in a Hive query."""
    regex = r'--.*'
    flags = re.MULTILINE | re.IGNORECASE
    return re.sub(regex, '', hive_query, flags)
