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

"""Definitions of various configuration objects stored in the repository."""
import abc
import copy
import json

from pinball.config.utils import PinballException


__author__ = 'Pawel Garbacki, Tongbo Huang'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = ['Pawel Garbacki', 'Tongbo Huang']
__license__ = 'Apache'
__version__ = '2.0'


class Config(object):
    """Parent class for all configurations."""
    __metaclass__ = abc.ABCMeta

    # List of attributes that must exist in the configuration object.
    _REQUIRED_ATTRIBUTES = []

    # List of attributes that may be set in the configuration object but are
    # not required.
    _OPTIONAL_ATTRIBUTES = []

    @classmethod
    def from_dict(cls, query_dict):
        """Construct configuration object from a QueryDict.

        Args:
            query_dict: The QueryDict describing the query_dict.
        Returns:
            Configuration object described by the json string.
        """
        for attribute in cls._REQUIRED_ATTRIBUTES:
            if attribute not in query_dict:
                raise PinballException('attribute %s not found in query_dict '
                                       '%s' % (attribute, query_dict))
        for attribute in query_dict.keys():
            if ((attribute not in cls._REQUIRED_ATTRIBUTES) and
                    (attribute not in cls._OPTIONAL_ATTRIBUTES)):
                raise PinballException('unrecognized attribute %s found in '
                                       'query_dict %s' % (attribute,
                                                          query_dict))
        result = cls()
        for key, value in query_dict.items():
            setattr(result, key, value)
        for attribute in cls._OPTIONAL_ATTRIBUTES:
            if attribute not in query_dict:
                setattr(result, attribute, None)
        result._validate()
        return result

    @classmethod
    def from_json(cls, json_config):
        """Construct configuration object from a json string.

        Args:
            json_config: The json string describing the config.
        Returns:
            Configuration object described by the json string.
        """
        config = json.loads(json_config)
        if type(config) is not dict:
            raise PinballException('json %s is not a dictionary', json_config)
        return cls.from_dict(config)

    def format(self):
        """Extract configuration options describing the object.

        Returns:
            Dictionary with key-values describing the object.
        """
        for attribute in self._REQUIRED_ATTRIBUTES:
            if not hasattr(self, attribute):
                raise PinballException('attribute %s not found in config %s' %
                                       (attribute, self))
        return copy.copy(self.__dict__)

    def _validate(self):
        return


class JobConfig(Config):
    _REQUIRED_ATTRIBUTES = [
        'workflow',
        'job',
        'is_condition',
        'template',
        'template_params',
        'parents',
        'emails',
        'max_attempts',
        'retry_delay_sec',
        'priority']

    _OPTIONAL_ATTRIBUTES = [
        'warn_timeout_sec',
        'abort_timeout_sec']

    def _validate(self):
        super(JobConfig, self)._validate()
        assert not self.is_condition or not self.parents


class WorkflowScheduleConfig(Config):
    _REQUIRED_ATTRIBUTES = [
        'workflow',
        'start_date',
        'time',
        'recurrence',
        'overrun_policy',
        'emails']
