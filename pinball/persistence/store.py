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

"""A token container supporting persistence."""
import abc

from pinball.config.utils import set_django_environment
set_django_environment()

from django import db
from django.core import management
from django.db import transaction

from pinball.config.utils import get_log
from pinball.persistence.models import ActiveTokenModel
from pinball.persistence.models import ArchivedTokenModel
from pinball.persistence.models import CachedDataModel


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = ['Pawel Garbacki', 'Mao Ye']
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.persistence.store')


class Store(object):
    """An interface for persistent token containers."""
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.initialize()

    @abc.abstractmethod
    def initialize(self):
        """Initialize the token store."""
        return

    @abc.abstractmethod
    def commit_tokens(self, updates=None, deletes=None):
        """Update or remove active tokens.

        Args:
            updates: The list of active token updates to persist.  Some tokens
            on this list may not exist in the store in which case they get
                inserted.
            deletes: The list of active tokens to remove from the store.
                Tokens on this list are required to exist in the store.
        """
        return

    def delete_archived_tokens(self, deletes):
        """Remove archived tokens.

        Args:
            deletes: The list of archived tokens to remove from the store.
                Tokens on this list are required to exist in the store.
        """
        return

    @abc.abstractmethod
    def read_active_tokens(self, name_prefix='', name_infix='',
                           name_suffix=''):
        """Read active tokens with names matching the provided filters."""
        return

    @abc.abstractmethod
    def read_archived_tokens(self, name_prefix='',
                             name_infix='',
                             name_suffix=''):
        """Read archived tokens with names matching the provided filters."""
        return

    @abc.abstractmethod
    def archive_tokens(self, tokens):
        """Move active tokens to archived store."""
        return

    @abc.abstractmethod
    def get_cached_data(self, name):
        """Get data with a given name from the cache."""
        return

    @abc.abstractmethod
    def set_cached_data(self, name, data):
        """Set data with a given name in the cache."""
        return

    @abc.abstractmethod
    def read_tokens(self, name_prefix='', name_infix='', name_suffix=''):
        """Read tokens with names matching the provided filters."""
        return

    @abc.abstractmethod
    def read_token_names(self, name_prefix='', name_infix='', name_suffix=''):
        """Read token names matching the provided filters."""
        return

    @abc.abstractmethod
    def read_archived_token_names(self, name_prefix='', name_infix='',
                                  name_suffix=''):
        """Read archived token names matching the provided filters."""
        return

    @abc.abstractmethod
    def read_cached_data_names(self, name_prefix='', name_infix='',
                               name_suffix=''):
        """Read cached data names matching the provided filters."""
        return


class DbStore(Store):
    """Implementation of token store on top of a database."""
    def initialize(self):
        """Create db tables if they don't exist."""
        management.call_command('syncdb', interactive=False)

    @transaction.commit_on_success
    def commit_tokens(self, updates=None, deletes=None):
        updates = updates if updates is not None else []
        deletes = deletes if deletes is not None else []
        for token in updates:
            token_model = ActiveTokenModel.from_token(token)
            token_model.save()
        for token in deletes:
            token_model = ActiveTokenModel.from_token(token)
            token_model.delete()

    @transaction.commit_on_success
    def delete_archived_tokens(self, deletes):
        for token in deletes:
            token_model = ArchivedTokenModel.from_token(token)
            token_model.delete()

    def read_active_tokens(self, name_prefix='', name_infix='',
                           name_suffix=''):
        # Refresh the connection to make sure we read the most recent snapshot.
        db.close_connection()
        return self._read_active_tokens(name_prefix, name_infix, name_suffix)

    @transaction.commit_on_success
    def _read_active_tokens(self, name_prefix='', name_infix='',
                            name_suffix=''):
        result = []
        token_models = self._read_data(ActiveTokenModel, name_prefix,
                                       name_infix, name_suffix)
        for token_model in token_models:
            result.append(token_model.to_token())
        return result

    def read_archived_tokens(self, name_prefix='', name_infix='',
                             name_suffix=''):
        db.close_connection()
        return self._read_archived_tokens(name_prefix, name_infix, name_suffix)

    @transaction.commit_on_success
    def _read_archived_tokens(self, name_prefix='', name_infix='',
                              name_suffix=''):
        result = []
        token_models = self._read_data(ArchivedTokenModel, name_prefix,
                                       name_infix, name_suffix)
        for token_model in token_models:
            result.append(token_model.to_token())
        return result

    def get_cached_data(self, name):
        db.close_connection()
        return self._get_cached_data(name)

    @transaction.commit_on_success
    def _get_cached_data(self, name):
        try:
            return CachedDataModel.objects.get(name=name).data
        except CachedDataModel.DoesNotExist:
            return None

    @transaction.commit_on_success
    def set_cached_data(self, name, data):
        cached_data = CachedDataModel.from_data(name, data)
        cached_data.save()

    @transaction.commit_on_success
    def archive_tokens(self, tokens):
        for token in tokens:
            active_token_model = ActiveTokenModel.from_token(token)
            active_token_model.delete()
            archived_token_model = ArchivedTokenModel.from_token(token)
            archived_token_model.save()

    def read_tokens(self, name_prefix='', name_infix='', name_suffix=''):
        db.close_connection()
        return self._read_tokens(name_prefix, name_infix, name_suffix)

    @transaction.commit_on_success
    def _read_tokens(self, name_prefix='', name_infix='', name_suffix=''):
        result = []
        active_tokens = self._read_data(ActiveTokenModel, name_prefix,
                                        name_infix, name_suffix)
        for token in active_tokens:
            result.append(token)
        archived_tokens = self._read_data(ArchivedTokenModel, name_prefix,
                                          name_infix, name_suffix)
        for token in archived_tokens:
            result.append(token)
        return result

    def read_token_names(self, name_prefix='', name_infix='', name_suffix=''):
        db.close_connection()
        return self._read_token_names(name_prefix, name_infix, name_suffix)

    def _read_token_names(self, name_prefix='', name_infix='', name_suffix=''):
        result = []
        active_token_names = self._read_data(ActiveTokenModel, name_prefix,
                                             name_infix, name_suffix, True)
        result.extend(active_token_names)
        archived_token_names = self._read_data(ArchivedTokenModel, name_prefix,
                                               name_infix, name_suffix, True)
        result.extend(archived_token_names)
        return result

    def read_archived_token_names(self, name_prefix='', name_infix='',
                                  name_suffix=''):
        db.close_connection()
        return self._read_archived_token_names(name_prefix, name_infix,
                                               name_suffix)

    def _read_archived_token_names(self, name_prefix='', name_infix='',
                                   name_suffix=''):
        result = []
        archived_token_names = self._read_data(ArchivedTokenModel, name_prefix,
                                               name_infix, name_suffix, True)
        result.extend(archived_token_names)
        return result

    def read_cached_data_names(self, name_prefix='', name_infix='',
                               name_suffix=''):
        db.close_connection()
        return self._read_cached_data_names(name_prefix, name_infix,
                                            name_suffix)

    def _read_cached_data_names(self, name_prefix='', name_infix='',
                                name_suffix=''):
        result = []
        cached_token_names = self._read_data(CachedDataModel, name_prefix,
                                             name_infix, name_suffix, True)
        result.extend(cached_token_names)
        return result

    @transaction.commit_on_success
    def clear_cached_data(self):
        CachedDataModel.objects.all().delete()

    def _customized_name_filter_query(self, model, name_prefix='', name_infix='',
                                      name_suffix='', select_name_only=False):
        """ Customize query based on given name filtering criteria.

        Instead of django.db filter operator, we use customized query is
        due to the following issue. django.db always translates string
        filter query by using "LIKE BINARY". Unfortunately, LIKE BINARY
        does not respect the index for the field with varchar data type.

        Args:
            model: django db model that contains table's information
            name_prefix: name starts with this string
            name_infix: name contains this string
            name_suffix: name ends with this string
            select_name_only: only return name in the query result

        Returns:
            Customized query
        """
        query_template = 'SELECT %s FROM %s %s;'
        selector = 'name' if select_name_only else '*'
        where_condition = ''
        conditions = []
        if name_prefix != '':
            conditions.append('name LIKE "%s%%%%"' % name_prefix.replace('_', '\_'))
        if name_infix != '':
            conditions.append('name LIKE "%%%%%s%%%%"' % name_infix.replace('_', '\_'))
        if name_suffix != '':
            conditions.append('name LIKE "%%%%%s"' % name_suffix.replace('_', '\_'))
        if conditions:
            where_condition = 'WHERE ' + ' AND '.join(conditions)

        query = query_template % (selector, model.get_table_name(), where_condition)
        return query

    @transaction.commit_on_success
    def _read_data(self, model, name_prefix='', name_infix='',
                   name_suffix='', select_name_only=False):
        raw_query = self._customized_name_filter_query(model, name_prefix,
                                                       name_infix, name_suffix,
                                                       select_name_only)
        results = model.objects.raw(raw_query)
        if select_name_only:
            # Unicode conversion extracts the value of the 'name' field
            # only from the model object.
            results = [unicode(result) for result in results]
        return results
