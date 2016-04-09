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

"""Web request handlers."""
import collections
import httplib2
import json
import socket
import traceback
import time

from django.contrib import messages
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.http import HttpResponseServerError
from django.shortcuts import render_to_response
from django.template.context import RequestContext
from django.views.generic.base import TemplateView

from pinball.authentication import oauth2
from pinball.config.pinball_config import PinballConfig
from pinball.config.utils import get_log
from pinball.tools.workflow_util import run_command
from pinball.ui.cache_thread import get_workflows_json
from pinball.ui.data_builder import DataBuilder
from pinball.ui.utils import get_workflow_jobs_from_parser_by_web_viewer
from pinball.ui.workflow_graph import WorkflowGraph
from pinball.persistence.store import DbStore
from pinball.workflow.signaller import Signal


__author__ = 'Pawel Garbacki'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.ui.views')
# Custom message level
SIGNIN = 35


def _serialize(elements):
    elements_list = []
    for element in elements:
        elements_list.append(element.format())
    to_serialize = {'aaData': elements_list}
    return json.dumps(to_serialize)


def workflows(_):
    try:
        workflows_json = get_workflows_json()
    except:
        LOG.exception('')
        return HttpResponseServerError(traceback.format_exc())
    else:
        return HttpResponse(workflows_json, mimetype='application/json')


def instances(request):
    try:
        workflow = request.GET['workflow']
        data_builder = DataBuilder(DbStore(), use_cache=True)
        instances_data = data_builder.get_instances(workflow)
        instances_json = _serialize(instances_data)
    except:
        LOG.exception('')
        return HttpResponseServerError(traceback.format_exc())
    else:
        return HttpResponse(instances_json, mimetype='application/json')


def jobs(request):
    try:
        data_builder = DataBuilder(DbStore(), use_cache=True)
        workflow = request.GET['workflow']
        instance = request.GET['instance']
        if instance == 'latest':
            instance = data_builder.get_latest_instance(workflow).instance
        jobs_data = data_builder.get_jobs(workflow, instance)
        jobs_json = _serialize(jobs_data)
    except:
        LOG.exception('')
        return HttpResponseServerError(traceback.format_exc())
    else:
        return HttpResponse(jobs_json, mimetype='application/json')


def graph(request):
    try:
        data_builder = DataBuilder(DbStore(), use_cache=True)
        workflow = request.GET['workflow']
        if 'instance' in request.GET:
            instance = request.GET['instance']
            if instance == 'latest':
                instance = data_builder.get_latest_instance(workflow).instance
            jobs_data = data_builder.get_jobs(workflow=workflow,
                                              instance=instance)
            instance_data = data_builder.get_instance(workflow=workflow,
                                                      instance=instance)
            workflow_graph = WorkflowGraph(jobs_data, instance_data)
        else:
            workflow_graph = WorkflowGraph.from_parser(workflow)
    except:
        LOG.exception('')
        return HttpResponseServerError(traceback.format_exc())
    else:
        return HttpResponse(workflow_graph.get_svg(), mimetype='image/svg+xml')


def executions(request):
    try:
        workflow = request.GET['workflow']
        instance = request.GET.get('instance')
        job = request.GET['job']
        data_builder = DataBuilder(DbStore())
        if instance:
            executions_data = data_builder.get_executions(workflow,
                                                          instance,
                                                          job)
        else:
            executions_data = data_builder.get_executions_across_instances(
                workflow, job)
        executions_json = _serialize(executions_data)
    except:
        LOG.exception('')
        return HttpResponseServerError(traceback.format_exc())
    else:
        return HttpResponse(executions_json, mimetype='application/json')


class ExecutionView(TemplateView):
    template_name = 'execution.html'

    def get_context_data(self, **kwargs):
        context = super(ExecutionView, self).get_context_data(**kwargs)
        workflow = self.request.GET['workflow']
        instance = self.request.GET['instance']
        job = self.request.GET['job']
        execution = int(self.request.GET['execution'])
        data_builder = DataBuilder(DbStore())
        execution_data = data_builder.get_execution(workflow,
                                                    instance,
                                                    job,
                                                    execution)
        formatted_data = execution_data.format()
        for key, value in formatted_data.items():
            context[key] = value
        properties = []
        for key, value in execution_data.properties.items():
            properties.append('%s=%s' % (key, value))
        context['properties'] = ', '.join(properties)
        if not execution_data.end_time:
            context['end_time'] = ''
        if execution_data.exit_code is None:
            context['exit_code'] = ''
        return context


def file_content(request):
    try:
        workflow = request.GET['workflow']
        instance = request.GET['instance']
        job = request.GET['job']
        execution = int(request.GET['execution'])
        log_type = request.GET['log_type']
        if execution < 0:
            return HttpResponseServerError(
                'execution must not be negative; got ' + execution)
        data_builder = DataBuilder(DbStore())
        file_data = data_builder.get_file_content(workflow, instance, job,
                                                  execution, log_type)
    except:
        LOG.exception('')
        return HttpResponseServerError(traceback.format_exc())
    else:
        return HttpResponse(file_data, mimetype='text/plain')


def schedules(_):
    try:
        data_builder = DataBuilder(DbStore())
        schedules_data = data_builder.get_schedules()
        schedules_json = _serialize(schedules_data)
    except:
        LOG.exception('')
        return HttpResponseServerError(traceback.format_exc())
    else:
        return HttpResponse(schedules_json, mimetype='application/json')


class ScheduleView(TemplateView):
    template_name = 'schedule.html'

    def get_context_data(self, **kwargs):
        context = super(ScheduleView, self).get_context_data(**kwargs)
        workflow = self.request.GET['workflow']
        data_builder = DataBuilder(DbStore())
        schedule_data = data_builder.get_schedule(workflow)
        formatted_schedule = schedule_data.format()
        for key, value in formatted_schedule.items():
            context[key] = value
        context['emails'] = ' '.join(schedule_data.emails)
        return context


def jobs_from_config(request):
    try:
        workflow = request.GET['workflow']
        jobs_data = get_workflow_jobs_from_parser_by_web_viewer(workflow)
        jobs_json = _serialize(jobs_data)
    except:
        LOG.exception('')
        return HttpResponseServerError(traceback.format_exc())
    else:
        return HttpResponse(jobs_json, mimetype='application/json')


def command(request):
    try:
        args = {}
        for key in request.GET:
            args[key] = request.GET[key]
        args['force'] = True
        if 'workflow' not in args:
            args['workflow'] = None
        if 'instance' not in args:
            args['instance'] = None
        Options = collections.namedtuple('Options', args.keys())
        options = Options(**args)
        output = run_command(options)
    except:
        LOG.exception('')
        return HttpResponseServerError(traceback.format_exc())
    else:
        return HttpResponse(json.dumps(output), mimetype='application/json')


class TokenPathsView(TemplateView):
    template_name = 'token_paths.html'

    def get_context_data(self, **kwargs):
        context = super(TokenPathsView, self).get_context_data(**kwargs)
        path = self.request.GET['path']
        if not path or not path.startswith('/'):
            return context
        path_elements = path.split('/')[1:]
        if path.endswith('/'):
            path_elements.pop()
        if not path_elements:
            context['basename'] = ''
        else:
            context['basename'] = path_elements[-1]
        # A list of tuples (parent_name, parent_prefix).
        parents = []
        prefix = '/'
        for element in path_elements[:-1]:
            prefix += '%s/' % element
            parents.append((element, prefix))
        context['parents'] = parents
        return context


def token_paths(request):
    try:
        path = request.GET['path']
        data_builder = DataBuilder(DbStore())
        tokens_data = data_builder.get_token_paths(path)
        tokens_json = _serialize(tokens_data)
    except:
        LOG.exception('')
        return HttpResponseServerError(traceback.format_exc())
    else:
        return HttpResponse(tokens_json, mimetype='application/json')


class TokenView(TokenPathsView):
    template_name = 'token.html'

    def get_context_data(self, **kwargs):
        context = super(TokenView, self).get_context_data(**kwargs)
        token_name = self.request.GET['path']
        data_builder = DataBuilder(DbStore())
        token_data = data_builder.get_token(token_name)
        token_format = token_data.format()
        for key, value in token_format.items():
            context[key] = value
        return context


def _is_master_alive():
    try:
        s = socket.socket()
        host = 'localhost'
        s.connect((host, PinballConfig.MASTER_PORT))
        s.close()
    except:
        return False
    return True


def status(request):
    try:
        workflow = request.GET.get('workflow')
        instance = request.GET.get('instance')
        data_builder = DataBuilder(DbStore())
        status = []
        if data_builder.is_signal_set(workflow, instance, Signal.EXIT):
            status = ['exiting']
        elif data_builder.is_signal_set(workflow, instance, Signal.ABORT):
            status = ['aborting']
        elif data_builder.is_signal_set(workflow, instance, Signal.DRAIN):
            status = ['draining']
        if not _is_master_alive():
            status.append('no master at %s:%d' % (socket.gethostname(),
                                                  PinballConfig.MASTER_PORT))
        status_json = json.dumps(status)
    except:
        LOG.exception('')
        return HttpResponseServerError(traceback.format_exc())
    else:
        return HttpResponse(status_json, mimetype='application/json')


def signin(request):
    oauth2_flow = oauth2.OAuth2Flow()
    context = {'domains': oauth2_flow.get_domains(), 'STATIC_URL': PinballConfig.STATIC_URL}
    if request.method == 'POST' and 'signin-domain' in request.POST.keys():
        domain = request.POST.get('signin-domain')
        if not oauth2_flow.domain_authenticated(domain):
            messages.add_message(request, SIGNIN, 'Domain not authorized: %s.' % domain,
                                 fail_silently=True)
            return render_to_response('signin.html', context,
                                      context_instance=RequestContext(request),
                                      mimetype='text/html')
        else:
            flow = oauth2_flow.get_flow(domain)
            auth_uri = flow.step1_get_authorize_url()
            return HttpResponseRedirect(auth_uri)
    else:
        return render_to_response('signin.html', context,
                                  context_instance=RequestContext(request),
                                  mimetype='text/html')


def auth_return(request):
    oauth2_flow = oauth2.OAuth2Flow()
    domains = oauth2_flow.get_domains()
    flow = oauth2.OAuth2Flow().get_flow(domains[1])
    # disable SSL certificate validation for exchanging access code
    http = httplib2.Http()
    http.disable_ssl_certificate_validation = True
    credential = flow.step2_exchange(request.GET.get('code'), http)
    credential_token = json.loads(credential.to_json())['id_token']
    if credential_token['email_verified'] and credential_token['hd'] in domains:
        email = credential_token['email']
        crypter = oauth2.Crypter()
        encrypted_email = crypter.encrypt(email)
        encrypted_domain = crypter.encrypt(credential_token['hd'])
        encrypted_token = crypter.encrypt(credential.access_token)
        response = HttpResponseRedirect('/')
        # cookie expires after a week
        response.set_cookie('login', encrypted_email, max_age=7 * 24 * 60 * 60)
        response.set_cookie('domain_url', encrypted_domain, max_age=7 * 24 * 60 * 60)
        response.set_cookie('user_id', email, max_age=7 * 24 * 60 * 60)
        response.set_cookie('token', encrypted_token)
        return response
    else:
        messages.add_message(request, SIGNIN, 'Authentication failed.')
        response = HttpResponseRedirect('/logout/')


def logout(request):
    credential_token = request.COOKIES.get('token', '')
    if credential_token == '':
        messages.add_message(request, SIGNIN, 'Logged out successfully.',
                             fail_silently=True)
        return HttpResponseRedirect('/signin/')
    crypter = oauth2.Crypter()
    try:
        logout_uri = 'https://accounts.google.com/o/oauth2/revoke?token=%s' \
            % crypter.decrypt(credential_token)
    except oauth2.CryptoException:
        response = HttpResponseRedirect('/signin/')
    else:
        http = httplib2.Http()
        http.disable_ssl_certificate_validation = True
        resp = http.request(logout_uri, 'GET')

        response = HttpResponseRedirect('/signin/')
        # Need to wait for Google to process the revoke request
        if resp[0].status == 200:
            time.sleep(2)
            response = HttpResponseRedirect('/logout/')

    response.set_cookie('user_id', '')
    response.set_cookie('login', '')
    response.set_cookie('domain_url', '')
    response.set_cookie('token', '')
    return response
