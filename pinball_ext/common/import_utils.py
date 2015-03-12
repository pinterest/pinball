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

import inspect
import pkgutil
import sys

from collections import defaultdict
from pinball_ext.common import utils


__author__ = 'Mao Ye, Changshu Liu'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = utils.get_logger('pinball_ext.common.import_utils')


class ModuleImport(object):
    """ModuleImport supports:
    1) import classes/modules for given import _import_directories
    2) retrieve class for given class_name
    3) retrieve all subclass names for a given parent class name
    """

    def __init__(self, import_directories,
                 base_class=object, map_class_module=False):
        if not import_directories:
            raise Exception("import directories are missing")
        self._import_directories = import_directories
        if base_class:
            self._base_class = base_class
        self._name_to_class_map = {}
        self._parent_child_class_name_map = defaultdict(list)
        self.name = self.__class__.__name__

        if map_class_module:
            self.class_name_to_module_name_map = {}
        else:
            self.class_name_to_module_name_map = None

    def get_class_by_name(self, class_name):
        """ Get class for a given name.

        Args:
            class_name: This is a string name of the class_name.

        Returns:
            The corresponding class.
        """
        return self._name_to_class_map.get(class_name, None)

    def get_subclass_name_list(self, base_class_name):
        """Get the subclass name list for a given base class name.

        Args:
            base_class_name: This is a string name of the base class.

        Returns:
            A list of subclass names.
        """
        return self._parent_child_class_name_map.get(base_class_name, None)

    def import_all_modules(self):
        """Import all modules for the given list of directories.

        Raises:
            ImportError: An error occurred when importing failed.
        """
        for dir_name in self._import_directories:
            try:
                self._import_modules_from_dir(dir_name, '')
            except ImportError:
                LOG.error('Error importing %s', dir_name)
                raise

    def _import_modules_from_dir(self, root_dir, file_suffix):
        """Import all modules from a given directory.

        Args:
            root_dir: This is directory that modules are imported from.
            file_suffix: This is a string representation of file suffix.

        Returns:
            There is no explicit return. Instead, the classes under the
            specified directory will be registered in _name_to_class_map.
        """

        def _import_all(module_dir):
            for importer, pkg_name, is_package in pkgutil.iter_modules([module_dir]):
                real_module_dir = module_dir.split('dist-packages/')[1] \
                    if 'dist-packages/' in module_dir else module_dir
                if is_package or pkg_name.endswith(file_suffix):
                    module_name = real_module_dir.replace('/', '.') + '.' + pkg_name
                    __import__(module_name)
                    self.register_module(module_name)
                if is_package:
                    _import_all(module_dir + '/' + pkg_name)
        # TODO(mao): To find a better way to handle import modules for dist-packages
        real_root_dir = root_dir.split('dist-packages/')[1] \
            if 'dist-packages/' in root_dir else root_dir
        root_module_name = real_root_dir.replace('/', '.')
        __import__(root_module_name)
        self.register_module(root_module_name)
        _import_all(root_dir)

    def register_module(self, module_name):
        """Register a particular module into _name_to_class_map and _parent_child_class_name_map.

           Args:
               module_name: string name of the module to be registered.

           Returns:
               There is no explict return.

           Raises:
               Exception: An exception occurred when the module's name is not
               unique.
        """

        for class_name, clazz in inspect.getmembers(sys.modules[module_name],
                                                    inspect.isclass):
            if not issubclass(clazz, self._base_class):
                continue
            existing_component = self._name_to_class_map.get(clazz.__name__)
            if existing_component and existing_component != clazz:
                raise Exception('Modules must have unique names. Duplicate: %s vs %s'
                                % (existing_component, clazz))

            self._name_to_class_map[clazz.__name__] = clazz

            if self.class_name_to_module_name_map is not None:
                self.class_name_to_module_name_map[clazz.__name__] = module_name

            # get the parent-child relationship
            parent_class_list = inspect.getmro(clazz)
            for p_class in parent_class_list:
                if p_class.__name__ is not clazz.__name__:
                    self._parent_child_class_name_map[p_class.__name__].append(clazz.__name__)

    def add_module(self, class_name, clazz):
        """Put class_name and corresponding class into _name_to_class_map.
        This is used in data_job.py.

           Args:
               class_name: This is a string name of the class.
               clazz: This is the class.

           Returns:
               There is no explicit return.
        """
        self._name_to_class_map[class_name] = clazz

    def module_exists(self, class_name):
        """Check if the give class_name is registered.

           Args:
               class_name: This is the string name of the class.

           Return:
               If the class is registered in _name_to_class_map, then True;
               otherwise, False.
        """
        return class_name in self._name_to_class_map

    def get_all_class_names(self):
        """Get a list of class names contained in _name_to_class_map."""
        return self._name_to_class_map.keys()

    def get_all_classes(self):
        """Get a list of classes contained in _name_to_class_map."""
        return self._name_to_class_map.values()
