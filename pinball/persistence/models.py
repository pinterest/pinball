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

"""Data models describing tokens.
"""
from django.db import models

from pinball.config.utils import master_name
from pinball.master.thrift_lib.ttypes import Token


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class TokenModel(models.Model):
    """Token model stored as a row in database table."""
    version = models.BigIntegerField()
    # TODO(pawel): 255 chars may not be sufficient to represent longer names
    # but this is a limit for unique char fields:
    # https://docs.djangoproject.com/en/dev/ref/databases/#character-fields
    # Consider alternatives.
    name = models.CharField(max_length=512, primary_key=True)
    owner = models.CharField(max_length=255, blank=True, null=True)
    expirationTime = models.BigIntegerField(blank=True, null=True)
    priority = models.FloatField(blank=True, null=True)
    data = models.TextField(blank=True, null=True)

    def __unicode__(self):
        # Modifying the return value will break the data retrieval logic
        # in the implementation of the DbStore.
        return self.name

    def to_token(self):
        return Token(version=self.version,
                     name=self.name,
                     owner=self.owner,
                     expirationTime=self.expirationTime,
                     priority=self.priority,
                     data=self.data)

    class Meta:
        abstract = True


class ActiveTokenModel(TokenModel):
    """Active token represents a token stored in a pinball master."""
    @staticmethod
    def from_token(token):
        token_model = ActiveTokenModel(version=token.version,
                                       name=token.name,
                                       owner=token.owner,
                                       expirationTime=token.expirationTime,
                                       priority=token.priority,
                                       data=token.data)
        return token_model

    # TODO(mao): refactor code to remove the duplicated table name creation.
    @staticmethod
    def get_table_name():
        return 'active_tokens_%s' % master_name()

    class Meta(TokenModel.Meta):
        # Table name is suffixed with master name.  This way multiple masters
        # can store their state in the same database.
        db_table = 'active_tokens_%s' % master_name()


class ArchivedTokenModel(TokenModel):
    """Archived token is not stored in pinball master any more."""
    @staticmethod
    def from_token(token):
        return ArchivedTokenModel(version=token.version,
                                  name=token.name,
                                  owner=token.owner,
                                  expirationTime=token.expirationTime,
                                  priority=token.priority,
                                  data=token.data)

    @staticmethod
    def get_table_name():
        return 'archived_tokens_%s' % master_name()

    class Meta(TokenModel.Meta):
        db_table = 'archived_tokens_%s' % master_name()


class CachedDataModel(models.Model):
    """Cache data model stored as a row in database table."""
    name = models.CharField(max_length=255, primary_key=True)
    data = models.TextField(blank=True, null=True)

    def __unicode__(self):
        return self.name

    @staticmethod
    def from_data(name, data):
        return CachedDataModel(name=name, data=data)

    @staticmethod
    def get_table_name():
        return 'cached_data_%s' % master_name()

    class Meta:
        db_table = 'cached_data_%s' % master_name()
