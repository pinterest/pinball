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

"""Pinball configuration object"""

import socket
import yaml


__author__ = 'Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class PinballConfig(object):
    import django

    # GENERATION should be incremented with each change affecting any of the
    # pinball servers (master, workers, etc.)
    GENERATION = 1

    # Maximum number of running instances allowed for a workflow
    DEFAULT_MAX_WORKFLOW_RUNNING_INSTANCES = 3

    # Master and client configuration.
    MASTER_HOST = socket.gethostname()
    MASTER_PORT = 9090
    CLIENT_CONNECT_ATTEMPTS = 10
    CLIENT_TIMEOUT_SEC = 3 * 60

    # Number of workers
    WORKERS = 50
    # A delay between starting individual workers.  We space starting new
    # workers to prevent overwhelming the master.
    WORKER_CREATION_SLEEP_INTERVAL_SEC = 10 * 60
    WORKER_POLL_TIME_SEC = 5 * 60

    # Maximum backoff time for client reconnect to master
    MAX_BACKOFF_CLIENT_RECONNECT_SEC = 20 * 60

    # Application configuration for Pinball persistence layer.
    INSTALLED_APPS = (
        'pinball.authentication',
        'pinball.persistence',
        'django.contrib.admin',
        'django.contrib.auth',
        'django.contrib.contenttypes',
        'django.contrib.sessions',
        'django.contrib.sites',
        'django.contrib.messages',
        'django.contrib.staticfiles',
    )

    SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')

    DEFAULT_MIDDLEWARE_CLASSES = (
        'django.contrib.sessions.middleware.SessionMiddleware',
        'django.contrib.messages.middleware.MessageMiddleware',
        'django.middleware.common.CommonMiddleware',
        'django.middleware.csrf.CsrfViewMiddleware',
        # we use django 1.5.4, so we need this for http -> https redirects.
        # with django >= 1.8, you can use SECURE_SSL_REDIRECT
        'sslify.middleware.SSLifyMiddleware',
    )

    MIDDLEWARE_CLASSES = DEFAULT_MIDDLEWARE_CLASSES

    # The UI_HOST is a VPN-friendly link for use to link Pinball logs in email.
    UI_HOST = None
    UI_PORT = 8080

    # UI server configuration.
    import os
    import pinball.ui as project_module
    ROOT_URLCONF = 'pinball.ui.urls'
    PROJECT_ROOT = os.path.dirname(os.path.realpath(project_module.__file__))
    STATICFILES_DIRS = (
        os.path.join(PROJECT_ROOT, 'assets'),
    )
    STATIC_ROOT = os.path.join(PROJECT_ROOT, 'static')
    ADMINS = (
    )
    MANAGERS = ADMINS
    STATIC_URL = '/static/'

    if django.VERSION < (1, 8):
        from django.conf.global_settings import TEMPLATE_CONTEXT_PROCESSORS as TCP
        TEMPLATE_CONTEXT_PROCESSORS = list(TCP) + [
            'django.core.context_processors.request',
        ]
        TEMPLATE_DIRS = (
            os.path.join(PROJECT_ROOT, 'templates'),
        )
    else:
        TEMPLATES = [
            {
                'BACKEND': 'django.template.backends.django.DjangoTemplates',
                'DIRS': [
                    os.path.join(PROJECT_ROOT, 'templates'),
                ],
                'APP_DIRS': True,
                'OPTIONS': {
                    'context_processors': [
                        'django.contrib.auth.context_processors.auth',
                        'django.template.context_processors.debug',
                        'django.template.context_processors.i18n',
                        'django.template.context_processors.media',
                        'django.template.context_processors.static',
                        'django.template.context_processors.request',
                        'django.template.context_processors.tz',
                    ],
                },
            },
        ]

    # Configuration for storing job log files in a remote storage.
    # By default, we disable logging to the remote storage.
    # If pinball ui and workers are run on different machines,
    # this parameter should be set.
    JOB_LOG_PATH_PREFIX = 'pinball_job_logs/'
    LOCAL_LOGS_DIR_PREFIX = '/tmp/'
    LOCAL_LOGS_DIR = LOCAL_LOGS_DIR_PREFIX + JOB_LOG_PATH_PREFIX
    S3_LOGS_DIR_PREFIX = None
    S3_LOGS_DIR = None

    # Use the following two settings when DEFAULT_S3_LOGS_DIR is set.
    AWS_ACCESS_KEY_ID = None
    AWS_SECRET_ACCESS_KEY = None

    # Pinball db settings
    DATABASES = {
        # Django requires a default database.
        'default': {
            'ENGINE': 'django.db.backends.mysql',
            'NAME': 'pinball',
            'USER': 'flipper',
            'PASSWORD': 'flipper123',
            'HOST': '127.0.0.1',
            'PORT': '3306',
        },
        'pinball.persistence': {
            'ENGINE': 'django.db.backends.mysql',
            'NAME': 'pinball',
            'USER': 'flipper',
            'PASSWORD': 'flipper123',
            'HOST': '127.0.0.1',
            'PORT': '3306',
        }
    }

    # Authentication configuration.
    AUTHENTICATION_DOMAINS = []
    GOOGLE_CLIENT_ID = None
    GOOGLE_CLIENT_SECRET = None

    # Django configuration parameters.
    # Secret key must be set in Django >= 1.5.
    # This was used for encryption in the legacy version
    SECRET_KEY = '0ma^86f1%7z5dbw57a5spwplab64oh-3wtq+05@akoto_&c0!!'
    AES_CBC_KEY = '\x91@\xc8\xa5I\xb2\xf2u\xb6e\xac\xc0\x03\x86U\x0b\xd4\x13' +\
        '\xca\x9b)\x9d\xc4t\x81s\x95\x89\x16\x9e\x8f\xa7'
    HMAC_KEY = '\xaf[\x0b\xb0\xa0\x8e\xbe\xa1\xb9:%\x9c\xb4yB\xf2n{\xa9"\x96' +\
        '\x83\xe7\t\xca\xb3T\xd9l\x8e\x08\xed'
    CRYPTO_VERSION = 1


    # Configuration for self-serving ETL creation through Pinball
    GITHUB_API_URI = '/api/v3/'
    GITHUB_HTTP_TIMEOUT_SEC = 30
    GITHUB_OAUTH_TOKEN = None
    GITHUB_SERVER = None
    USER = 'data'

    # Application configuration.
    PARSER = 'pinball_ext.workflow.parser.PyWorkflowParser'
    PARSER_PARAMS = {}

    # Configuration for the default email in pinball.
    # We use it as default sender of email service right now.
    DEFAULT_EMAIL = None

    # Debugging should be disabled for long running Django apps to prevent caching
    # of queries leading to ever growing memory consumption.
    DEBUG = False

    ALLOWED_HOSTS = ['*']

    # SMTP Configuration
    SMTP_HOST = 'localhost'
    SMTP_PORT = 25
    SMTP_USER = ''
    SMTP_PASS = ''
    SMTP_SSL  = False

    @staticmethod
    def parse(config_file):
        # Assume the config file is "yaml" file
        yaml_file = None
        try:
            yaml_file = open(config_file, 'r')
            config_data = yaml.load(yaml_file)
        except:
            raise
        finally:
            if yaml_file:
                yaml_file.close()

        for key, value in config_data.iteritems():
            PinballConfig.update_config(key.upper(), value)
        PinballConfig._check_configs()
        PinballConfig._post_process_configs()

    @classmethod
    def update_config(cls_obj, config_name, config_value):
        setattr(cls_obj, config_name, config_value)

    @staticmethod
    def _post_process_configs():
        PinballConfig.LOCAL_LOGS_DIR = \
            PinballConfig.LOCAL_LOGS_DIR_PREFIX \
            + PinballConfig.JOB_LOG_PATH_PREFIX
        if PinballConfig.S3_LOGS_DIR_PREFIX is not None:
            PinballConfig.S3_LOGS_DIR = \
                PinballConfig.S3_LOGS_DIR_PREFIX \
                + PinballConfig.JOB_LOG_PATH_PREFIX
        if PinballConfig.AUTHENTICATION_DOMAINS:
            PinballConfig.MIDDLEWARE_CLASSES = \
                ('pinball.authentication.oauth2.RequireLogin',) \
                + PinballConfig.DEFAULT_MIDDLEWARE_CLASSES[:1] \
                + ('django.contrib.auth.middleware.AuthenticationMiddleware',) \
                + PinballConfig.DEFAULT_MIDDLEWARE_CLASSES[2:]

    @staticmethod
    def _check_configs():
        assert (PinballConfig.DEFAULT_EMAIL is not None), \
            'Please specify the default_email!'
        assert PinballConfig.PARSER_PARAMS, \
            'Please configure parser_params!'
        if PinballConfig.AUTHENTICATION_DOMAINS:
            assert (PinballConfig.GOOGLE_CLIENT_ID is not None), \
                'Please specify the google_client_id!'
            assert (PinballConfig.GOOGLE_CLIENT_SECRET is not None), \
                'Please specify the google_client_secret!'
            assert (PinballConfig.SECRET_KEY is not None), \
                'Please specify the secrent_key for django oauth!'
            assert (PinballConfig.AES_CBC_KEY is not None), \
                'Please specify the aes_cbc_key for django oauth!'
            assert (PinballConfig.HMAC_KEY is not None), \
                'Please specify the hmac_key for django oauth!'
            assert (PinballConfig.CRYPTO_VERSION), \
                'Please specify the crypto_version for django oauth!'
