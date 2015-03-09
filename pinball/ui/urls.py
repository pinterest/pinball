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

"""Web server urls."""
from django.conf.urls import include
from django.conf.urls import patterns
from django.conf.urls import url
from django.views.generic import TemplateView
# Uncomment the next two lines to enable the admin:
from django.contrib import admin
from django.http import HttpResponseRedirect

from pinball.config.pinball_config import PinballConfig
from pinball.ui.views import ExecutionView
from pinball.ui.views import ScheduleView
from pinball.ui.views import TokenPathsView
from pinball.ui.views import TokenView
from pinball.ui.views import workflows


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


admin.autodiscover()

urlpatterns = patterns('',
                       url(r'^$',
                           lambda x: HttpResponseRedirect('/workflows/')),
                       url(r'^workflows/$', TemplateView.as_view(
                           template_name='workflows.html'),
                           name='workflows'),
                       url(r'^ajax/workflows/$',
                           workflows,
                           name='ajax/workflows'),
                       url(r'^instances/$',
                           TemplateView.as_view(
                               template_name='instances.html'),
                           name='instances'),
                       url(r'^ajax/instances/$',
                           'pinball.ui.views.instances',
                           name='ajax/instances'),
                       url(r'^jobs/$',
                           TemplateView.as_view(template_name='jobs.html'),
                           name='jobs'),
                       url(r'^ajax/jobs/$', 'pinball.ui.views.jobs',
                           name='ajax/jobs'),
                       url(r'^graph/$', 'pinball.ui.views.graph',
                           name='graph'),
                       url(r'^executions/$',
                           TemplateView.as_view(
                               template_name='executions.html'),
                           name='executions'),
                       url(r'^job_executions/$',
                           TemplateView.as_view(
                               template_name='job_executions.html'),
                           name='job_executions'),
                       url(r'^ajax/executions/$',
                           'pinball.ui.views.executions',
                           name='ajax/executions'),
                       url(r'^execution/$', ExecutionView.as_view(),
                           name='execution'),
                       url(r'^file/$',
                           TemplateView.as_view(template_name='file.html'),
                           name='file'),
                       url(r'^file_content/$',
                           'pinball.ui.views.file_content',
                           name='file_content'),
                       url(r'^schedules/$',
                           TemplateView.as_view(
                               template_name='schedules.html'),
                           name='schedules'),
                       url(r'^ajax/schedules/$',
                           'pinball.ui.views.schedules',
                           name='ajax/schedules'),
                       url(r'^schedule/$', ScheduleView.as_view(),
                           name='schedule'),
                       url(r'^ajax/jobs_from_config/$',
                           'pinball.ui.views.jobs_from_config',
                           name='ajax/jobs_from_config'),
                       url(r'^ajax/command/$',
                           'pinball.ui.views.command',
                           name='ajax/command'),
                       url(r'^token_paths/$', TokenPathsView.as_view(),
                           name='token_paths'),
                       url(r'^ajax/token_paths/$',
                           'pinball.ui.views.token_paths',
                           name='ajax/token_paths'),
                       url(r'^token/$', TokenView.as_view(), name='token'),
                       url(r'^ajax/status/$',
                           'pinball.ui.views.status',
                           name='ajax/status'),
                       # Uncomment the next line to enable the admin:
                       # TODO(pawel): admin interface doesn't work because
                       # of some reason.
                       url(r'^admin/', include(admin.site.urls)),
                       # Authentication.
                       url(r'^signin/$',
                           'pinball.ui.views.signin'),
                       url(r'^oauth2callback/',
                           'pinball.ui.views.auth_return'),
                       url(r'^logout/',
                           'pinball.ui.views.logout'))

if not PinballConfig.DEBUG:  # if DEBUG is True it will be served automatically
    assert len(PinballConfig.STATICFILES_DIRS) == 1
    urlpatterns += patterns('', url(r'^static/(?P<path>.*)$',
                            'django.views.static.serve',
                            {'document_root': PinballConfig.STATICFILES_DIRS[0]}))
