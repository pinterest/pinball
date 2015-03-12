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

"""Format and send notification emails."""
import datetime
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pinball.config.pinball_config import PinballConfig
from pinball.config.utils import get_log
from pinball.config.utils import timestamp_to_str
from pinball.ui.data import Status


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = ['Pawel Garbacki', 'Mao Ye']
__license__ = 'Apache'
__version__ = '2.0'


LOG = get_log('pinball.workflow.emailer')


class Emailer(object):
    """Send emails representing certain events."""
    def __init__(self, ui_host, ui_port):
        self._ui_host = ui_host
        self._ui_port = ui_port

    def _send_message(self, subject, to, text, html):
        """Send a message through local SMTP server.

        Args:
            subject: The subject of the email message.
            to: The list of recipient email addresses.
            text: The email body in text format.
            html: The email body in html format.
        """
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = PinballConfig.DEFAULT_EMAIL
        msg['To'] = ', '.join(to)

        text_part = MIMEText(text, 'plain')
        html_part = MIMEText(html, 'html') if html else None

        # Attach parts into message container.  According to RFC 2046, the last
        # part of a multipart message, in this case the HTML message, is best
        # and preferred.
        msg.attach(text_part)
        if html_part:
            msg.attach(html_part)

        # Send the message via local SMTP server.
        smtp = smtplib.SMTP('localhost')
        smtp.sendmail(msg['From'], to, msg.as_string())
        smtp.quit()
        LOG.info('Sent email to %s with subject "%s"', msg['To'], subject)

    def _get_instance_end_text(self, instance_data, jobs_data):
        """Format text version of the workflow instance completion message.

        Args:
            instance_data: The data of the completed workflow instance.
            jobs_data: The list of data describing jobs in the workflow
                instance.

        Returns:
            Text message describing the workflow instance.
        """
        TEXT_TEMPLATE = """\
Workflow %(workflow)s instance %(instance)s started at %(start_time)s \
finished after %(run_time)s at %(end_time)s with status %(status)s.

Details are available at  http://%(ui_host)s:%(ui_port)s/jobs/?\
workflow=%(workflow)s&instance=%(instance)s

Jobs:

Name | Last start | Last end | Run time | Status | Url
%(jobs)s
"""
        jobs = ''
        for job_data in jobs_data:
            jobs += '%s | ' % job_data.job
            if not job_data.last_start_time:
                jobs += '| | '
            else:
                jobs += '%s | ' % timestamp_to_str(job_data.last_start_time)
                if not job_data.last_end_time:
                    jobs += '| | '
                else:
                    jobs += '%s | ' % timestamp_to_str(job_data.last_end_time)
                    delta = int(job_data.last_end_time -
                                job_data.last_start_time)
                    jobs += '%s | ' % datetime.timedelta(seconds=delta)
            jobs += '%s | ' % Status.to_string(job_data.status)
            jobs += ('http://%s:%s/executions/?workflow=%s&instance=%s&'
                     'job=%s\n' % (self._ui_host,
                                   self._ui_port,
                                   job_data.workflow,
                                   job_data.instance,
                                   job_data.job))
        start_time = timestamp_to_str(instance_data.start_time)
        end_time = timestamp_to_str(instance_data.end_time)
        delta = int(instance_data.end_time - instance_data.start_time)
        run_time = datetime.timedelta(seconds=delta)
        status = Status.to_string(instance_data.status)
        return TEXT_TEMPLATE % {'workflow': instance_data.workflow,
                                'instance': instance_data.instance,
                                'start_time': start_time,
                                'end_time': end_time,
                                'run_time': run_time,
                                'status': status,
                                'ui_host': self._ui_host,
                                'ui_port': self._ui_port,
                                'jobs': jobs}

    def _get_instance_end_html(self, instance_data, jobs_data):
        """Format html version of the workflow instance completion message.

        Args:
            instance_data: The data of the completed workflow instance.
            jobs_data: The list of data describing jobs in the workflow
                instance.

        Returns:
            Html message describing the workflow instance.
        """
        HTML_TEMPLATE = """\
<html>
    <head></head>
    <body>
        <p>
            Workflow %(workflow)s instance %(instance)s started at
            %(start_time)s finished after %(run_time)s at %(end_time)s
            with status <span
style="background-color:%(instance_status_color)s;">%(status)s</span>.
            Click
            <a href="http://%(ui_host)s:%(ui_port)s/jobs/?\
workflow=%(workflow)s&instance=%(instance)s">here</a> for details.
        </p>
        <p>Jobs:<br/>
            <table style="border-collapse:collapse;">
                <tr>
                    <th style="border:1px dotted grey;">Name</th>
                    <th style="border:1px dotted grey;">Last start</th>
                    <th style="border:1px dotted grey;">Last end</th>
                    <th style="border:1px dotted grey;">Run time</th>
                    <th style="border:1px dotted grey;">Status</th>
                </tr>
                %(jobs)s
            </table>
        </p>
    </body>
</html>
"""
        jobs = ''
        for job_data in jobs_data:
            jobs += '<tr>'
            jobs += ('<td style="border:1px dotted grey;">'
                     '<a href="http://%s:%s/executions/?workflow=%s&'
                     'instance=%s&job=%s">%s</a></td>' % (self._ui_host,
                                                          self._ui_port,
                                                          job_data.workflow,
                                                          job_data.instance,
                                                          job_data.job,
                                                          job_data.job))
            if not job_data.last_start_time:
                jobs += ('<td style="border:1px dotted grey;"></td>'
                         '<td style="border:1px dotted grey;"></td>'
                         '<td style="border:1px dotted grey;"></td>')
            else:
                jobs += ('<td style="border:1px dotted grey;">%s</td>' %
                         timestamp_to_str(job_data.last_start_time))
                if not job_data.last_end_time:
                    jobs += ('<td style="border:1px dotted grey;"></td>'
                             '<td style="border:1px dotted grey;"></td>')
                else:
                    delta = int(job_data.last_end_time -
                                job_data.last_start_time)
                    jobs += ('<td style="border:1px dotted grey;">%s</td>'
                             '<td style="border:1px dotted grey;">%s</td>' % (
                                 timestamp_to_str(job_data.last_end_time),
                                 datetime.timedelta(seconds=delta)))
            jobs += ('<td style="border:1px dotted grey;background-color:%s;'
                     'text-align: center;">%s</td>\n' % (
                         Status.COLORS[job_data.status],
                         Status.to_string(job_data.status)))
            jobs += '</tr>'
        start_time = timestamp_to_str(instance_data.start_time)
        end_time = timestamp_to_str(instance_data.end_time)
        delta = int(instance_data.end_time - instance_data.start_time)
        run_time = datetime.timedelta(seconds=delta)
        instance_status_color = Status.COLORS[instance_data.status]
        status = Status.to_string(instance_data.status)
        return HTML_TEMPLATE % {'workflow': instance_data.workflow,
                                'instance': instance_data.instance,
                                'start_time': start_time,
                                'end_time': end_time,
                                'run_time': run_time,
                                'instance_status_color': instance_status_color,
                                'status': status,
                                'ui_host': self._ui_host,
                                'ui_port': self._ui_port,
                                'jobs': jobs}

    @staticmethod
    def _sort_jobs(jobs_data):
        """Sort jobs on the start time.

        Args:
            jobs_data: The list of jobs to sort.

        Returns:
            Sorted list of jobs.
        """
        def _start_time(jobs_data):
            return jobs_data.last_start_time
        return sorted(jobs_data, key=_start_time)

    def send_instance_end_message(self, to, instance_data, jobs_data):
        """Send a message describing workflow instance run.

        Args:
            to: The list of recipient email addresses.
            instance_data: The data of the completed workflow instance.
            jobs_data: The list of data describing jobs in the workflow
                instance.
        """
        jobs_data = Emailer._sort_jobs(jobs_data)
        text = self._get_instance_end_text(instance_data, jobs_data)
        html = self._get_instance_end_html(instance_data, jobs_data)
        status = Status.to_string(instance_data.status)
        subject = '%s for workflow %s' % (status, instance_data.workflow)
        self._send_message(subject, to, text, html)

    def _get_job_execution_params(self, job_execution_data):
        start_time = timestamp_to_str(job_execution_data.start_time)
        return {'workflow': job_execution_data.workflow,
                'instance': job_execution_data.instance,
                'job': job_execution_data.job,
                'execution': job_execution_data.execution,
                'info': job_execution_data.info,
                'start_time': start_time,
                'ui_host': self._ui_host,
                'ui_port': self._ui_port}

    def _get_job_execution_end_text(self, job_execution_data):
        """Format text version of the job execution completion message.

        Args:
            job_execution_data: The job execution data described in the
            message.
        Returns:
            Html message describing the job execution.
        """
        TEXT_TEMPLATE = """\
Job %(job)s execution in workflow %(workflow)s instance %(instance)s started \
at %(start_time)s finished after %(run_time)s at %(end_time)s with exit code \
%(exit_code)s.

Details are available at http://%(ui_host)s:%(ui_port)s/executions/?workflow=\
%(workflow)s&instance=%(instance)s&job=%(job)s

Execution details:

Workflow: %(workflow)s
Instance: %(instance)s
Job: %(job)s
Execution: %(execution)s
Info: %(info)s
Start time: %(start_time)s
End time: %(end_time)s
Run time: %(run_time)s
Logs: %(logs)s
Exit code: %(exit_code)s
"""
        params = self._get_job_execution_params(job_execution_data)
        end_time = timestamp_to_str(job_execution_data.end_time)
        params['end_time'] = end_time
        delta = int(job_execution_data.end_time -
                    job_execution_data.start_time)
        run_time = datetime.timedelta(seconds=delta)
        params['run_time'] = run_time
        logs = ', '.join(job_execution_data.logs.keys())
        params['logs'] = logs
        params['exit_code'] = job_execution_data.exit_code
        return TEXT_TEMPLATE % params

    def _get_job_execution_end_html(self, job_execution_data):
        """Format html version of the job execution completion message.

        Args:
            job_execution_data: The job execution data described in the
            message.
        Returns:
            Html message describing the job execution.
        """
        HTML_TEMPLATE = """\
<html>
    <head></head>
    <body>
        <p>
            Job %(job)s execution in workflow %(workflow)s instance
            %(instance)s started at %(start_time)s finished after %(run_time)s
            at %(end_time)s with exit code <span
            style="background-color:%(exit_code_color)s;">%(exit_code)s</span>.
            Click
            <a href="http://%(ui_host)s:%(ui_port)s/executions/?workflow=\
%(workflow)s&instance=%(instance)s&job=%(job)s">here</a> for details.
        </p>
        <p>Execution details:<br/>
            <table style="border-collapse:collapse;">
                <tr>
                    <td style="border:1px dotted grey;"><b>Workflow</b></td>
                    <td style="border:1px dotted grey;">
                        <a href="http://%(ui_host)s:%(ui_port)s/instances/?\
workflow=%(workflow)s">%(workflow)s</a>
                    </td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>Instance</b></td>
                    <td style="border:1px dotted grey;">
                        <a href="http://%(ui_host)s:%(ui_port)s/jobs/?\
workflow=%(workflow)s&instance=%(instance)s">%(instance)s</a>
                    </td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>Job</b></td>
                    <td style="border:1px dotted grey;">
                    <a href="http://%(ui_host)s:%(ui_port)s/executions/?\
workflow=%(workflow)s&instance=%(instance)s&job=%(job)s">%(job)s</a>
                    </td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>Execution</b></td>
                    <td style="border:1px dotted grey;">%(execution)s</td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>Info</b></td>
                    <td style="border:1px dotted grey;">%(info)s</td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>Start time</b></td>
                    <td style="border:1px dotted grey;">%(start_time)s</td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>End time</b></td>
                    <td style="border:1px dotted grey;">%(end_time)s</td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>Run time</b></td>
                    <td style="border:1px dotted grey;">%(run_time)s</td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>Logs</b></td>
                    <td style="border:1px dotted grey;">%(logs)s</td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>Exit code</b></td>
                    <td
style="border:1px dotted grey;background-color:%(exit_code_color)s;">
                        %(exit_code)s
                    </td>
                </tr>
            </table>
        </p>
    </body>
</html>
"""
        params = self._get_job_execution_params(job_execution_data)
        end_time = timestamp_to_str(job_execution_data.end_time)
        params['end_time'] = end_time
        delta = int(job_execution_data.end_time -
                    job_execution_data.start_time)
        run_time = datetime.timedelta(seconds=delta)
        params['run_time'] = run_time
        params['exit_code'] = job_execution_data.exit_code
        exit_code_color = (Status.COLORS[Status.SUCCESS] if
                           job_execution_data.exit_code == 0 else
                           Status.COLORS[Status.FAILURE])
        params['exit_code_color'] = exit_code_color
        logs = []
        for log_type in job_execution_data.logs:
            params['log_type'] = log_type
            logs.append('<a href="http://%(ui_host)s:%(ui_port)s/file/?'
                        'workflow=%(workflow)s&instance=%(instance)s&'
                        'job=%(job)s&execution=%(execution)s&'
                        'log_type=%(log_type)s">%(log_type)s</a>' % params)
        params['logs'] = '&nbsp;|&nbsp;'.join(logs)
        return HTML_TEMPLATE % params

    def send_job_execution_end_message(self, to, job_execution_data):
        """Send a message describing a job execution failure.

        Args:
            to: The list of recipient email addresses.
            job_execution_data: The job execution data described in the
                message.
        """
        text = self._get_job_execution_end_text(job_execution_data)
        html = self._get_job_execution_end_html(job_execution_data)

        subject = "Workflow %s's job %s finished with exit code %d" % (
            job_execution_data.workflow, job_execution_data.job,
            job_execution_data.exit_code)
        self._send_message(subject, to, text, html)

    def _get_job_timeout_warning_text(self, job_execution_data):
        """Format text version of the job timeout message.

        Args:
            job_execution_data: The job execution data described in the
            message.
        Returns:
            text message describing the job execution.
        """
        TEXT_TEMPLATE = """\
Job %(job)s execution in workflow %(workflow)s instance %(instance)s started \
at %(start_time)s reached timeout.

Details are available at http://%(ui_host)s:%(ui_port)s/executions/?workflow=\
%(workflow)s&instance=%(instance)s&job=%(job)s

Execution details:

Workflow: %(workflow)s
Instance: %(instance)s
Job: %(job)s
Execution: %(execution)s
Info: %(info)s
Start time: %(start_time)s
Logs: %(logs)s
"""
        params = self._get_job_execution_params(job_execution_data)
        logs = ', '.join(job_execution_data.logs.keys())
        params['logs'] = logs
        return TEXT_TEMPLATE % params

    def _get_job_timeout_warning_html(self, job_execution_data):
        """Format html version of the job timeout message.

        Args:
            job_execution_data: The job execution data described in the
            message.
        Returns:
            Html message describing the job execution.
        """
        HTML_TEMPLATE = """\
<html>
    <head></head>
    <body>
        <p>
            Job %(job)s execution in workflow %(workflow)s instance
            %(instance)s started at %(start_time)s reached timeout.
            Click
            <a href="http://%(ui_host)s:%(ui_port)s/executions/?workflow=\
%(workflow)s&instance=%(instance)s&job=%(job)s">here</a> for details.
        </p>
        <p>Execution details:<br/>
            <table style="border-collapse:collapse;">
                <tr>
                    <td style="border:1px dotted grey;"><b>Workflow</b></td>
                    <td style="border:1px dotted grey;">
                        <a href="http://%(ui_host)s:%(ui_port)s/instances/?\
workflow=%(workflow)s">%(workflow)s</a>
                    </td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>Instance</b></td>
                    <td style="border:1px dotted grey;">
                        <a href="http://%(ui_host)s:%(ui_port)s/jobs/?\
workflow=%(workflow)s&instance=%(instance)s">%(instance)s</a>
                    </td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>Job</b></td>
                    <td style="border:1px dotted grey;">
                    <a href="http://%(ui_host)s:%(ui_port)s/executions/?\
workflow=%(workflow)s&instance=%(instance)s&job=%(job)s">%(job)s</a>
                    </td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>Execution</b></td>
                    <td style="border:1px dotted grey;">%(execution)s</td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>Info</b></td>
                    <td style="border:1px dotted grey;">%(info)s</td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>Start time</b></td>
                    <td style="border:1px dotted grey;">%(start_time)s</td>
                </tr>
                <tr>
                    <td style="border:1px dotted grey;"><b>Logs</b></td>
                    <td style="border:1px dotted grey;">%(logs)s</td>
                </tr>
            </table>
        </p>
    </body>
</html>
"""
        params = self._get_job_execution_params(job_execution_data)
        logs = []
        for log_type in job_execution_data.logs:
            params['log_type'] = log_type
            logs.append('<a href="http://%(ui_host)s:%(ui_port)s/file/?'
                        'workflow=%(workflow)s&instance=%(instance)s&'
                        'job=%(job)s&execution=%(execution)s&'
                        'log_type=%(log_type)s">%(log_type)s</a>' % params)
        params['logs'] = '&nbsp;|&nbsp;'.join(logs)
        return HTML_TEMPLATE % params

    def send_job_timeout_warning_message(self, to, job_execution_data):
        """Send a message warning about job execution exceeding timeout.

        Args:
            to: The list of recipient email addresses.
            job_execution_data: The job execution data described in the
                message.
        """
        text = self._get_job_timeout_warning_text(job_execution_data)
        html = self._get_job_timeout_warning_html(job_execution_data)

        subject = "Workflow %s's job %s exceeded timeout" % (
            job_execution_data.workflow, job_execution_data.job)
        try:
            self._send_message(subject, to, text, html)
        except:
            # Do not fail the job if sending out warning email failed.
            LOG.exception('failed to send warning email to %s for job %s'
                          % (to, job_execution_data.job))

    def send_too_many_running_instances_warning_message(self, to, workflow,
                                                        number_running_instances,
                                                        max_running_instances):
        """Send a message warning about number of running instances of a
        workflow exceeding the threshold.

        Args:
            to: Recipients of this email.
            workflow: Name of the workflow that the email is warning about.
            number_running_instances: Number of the current running instance for the
             given workflow.
            max_running_instances: Running instances of a workflow should lower
             than this number.
        """
        text = self._get_too_many_running_instances_warning_text(workflow,
                                                                 number_running_instances,
                                                                 max_running_instances)

        subject = "Too many (%s) instances running for workflow %s !" % (
            number_running_instances, workflow)

        try:
            self._send_message(subject, to, text, None)
        except:
            # Do not fail the job if sending out warning email failed.
            LOG.exception('failed to send too many instance running warning email to %s for workflow %s'
                          % (to, workflow))

    def _get_workflow_params(self, workflow, number_running_instances, max_running_instances):
        return {'ui_host': self._ui_host,
                'ui_port': self._ui_port,
                'running_instance_number': str(number_running_instances),
                'workflow': workflow,
                'max_running_instances': str(max_running_instances)}

    def _get_too_many_running_instances_warning_text(self,
                                                     workflow,
                                                     number_running_instances,
                                                     max_running_instances):
        """Format text version of too many running instance warning message.

        Args:
            workflow: Name of the workflow that the email is warning about.
            number_running_instances: Number of the current running instance for the
             given workflow.
            max_running_instances: Running instances of a workflow should lower than this number.

        Returns:
            Text message describing the workflow is running too many instances.
        """
        TEXT_TEMPLATE = """\
There are %(running_instance_number)s instance running at the same for workflow %(workflow)s,\
exceeding the threshold of %(max_running_instances)s!

Details are available at  http://%(ui_host)s:%(ui_port)s/instances/?workflow=%(workflow)s
"""
        params = self._get_workflow_params(workflow, number_running_instances,
                                           max_running_instances)

        return TEXT_TEMPLATE % params
