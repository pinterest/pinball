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

"""Configuration repository stored in Github."""
import base64
import json
import httplib
import time
import urlparse

from pinball.config.pinball_config import PinballConfig
from pinball.config.utils import PinballException
from pinball.repository.repository import Repository


__author__ = 'Pawel Garbacki, Mao Ye'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = [__author__]
__license__ = 'Apache'
__version__ = '2.0'


class GithubRepository(Repository):
    """Github-backed configuration store."""

    _COMMITTER_NAME = 'Pinball'
    _COMMITTER_EMAIL = PinballConfig.DEFAULT_EMAIL

    def _make_request(self, method, location, body, expected_status):
        """Make a request through the Github API.

        Args:
            method: The request method.
            location: The request path.
            body: The body of the request message.
            expected_status: List of http status codes expected on a successful
                request.
        Returns:
            The API response.
        """
        conn = httplib.HTTPSConnection(PinballConfig.GITHUB_SERVER,
                                       timeout=PinballConfig.GITHUB_HTTP_TIMEOUT_SEC)
        location = urlparse.urljoin(PinballConfig.GITHUB_API_URI, location)
        authorization = base64.b64encode('%s:x-oauth-basic' %
                                         PinballConfig.GITHUB_OAUTH_TOKEN)
        headers = {'Authorization': 'Basic %s' % authorization}
        conn.request(method, location, body, headers)
        response = conn.getresponse()
        if response.status not in expected_status:
            raise PinballException('failed request to %s method %s location '
                                   '%s status %s reason %s content %s' %
                                   (PinballConfig.GITHUB_API_URI, method, location,
                                    response.status, response.reason,
                                    response.read()))
        return response.read()

    def _get_config(self, path):
        location = 'repos/%s/configs/contents%s' % (PinballConfig.USER, path)
        response = self._make_request('GET', location, None, [httplib.OK])
        response_json = json.loads(response)
        encoded_content = response_json['content']
        return base64.b64decode(encoded_content)

    @staticmethod
    def _get_file_type(response):
        """Get the file type described by a response.

        Args:
            response: The parsed response describing a file.
        Returns:
            Type of the file described by the response.
        """
        if type(response) == list:
            return 'dir'
        return response.get('type')

    def _put_config(self, path, content):
        location = 'repos/%s/configs/contents%s' % (PinballConfig.USER, path)
        encoded_contnet = base64.b64encode(content)
        body = {'message': 'updating config',
                'committer': {'name': GithubRepository._COMMITTER_NAME,
                              'email': GithubRepository._COMMITTER_EMAIL},
                'content': encoded_contnet}

        # Check if path exists.
        response = self._make_request('GET', location, None,
                                      [httplib.OK, httplib.NOT_FOUND])
        response_json = json.loads(response)
        file_type = GithubRepository._get_file_type(response_json)
        if file_type:
            # Path exists.
            if file_type != 'file':
                raise PinballException('path %s is not a file but a %s' %
                                       (path, file_type))
            body['sha'] = response_json['sha']

        # Work around a bug in Github.  See
        # http://stackoverflow.com/questions/19576601/\
        # github-api-issue-with-file-upload
        time.sleep(0.5)

        # Create or update the file.
        self._make_request('PUT', location, json.dumps(body),
                           [httplib.CREATED, httplib.OK])

    def _delete_config(self, path):
        location = 'repos/%s/configs/contents%s' % (PinballConfig.USER, path)
        body = {'message': 'updating config',
                'committer': {'name': GithubRepository._COMMITTER_NAME,
                              'email': GithubRepository._COMMITTER_EMAIL}}

        # Get sha of the content.
        response = self._make_request('GET', location, None, [httplib.OK])
        response_json = json.loads(response)
        file_type = GithubRepository._get_file_type(response_json)
        if not file_type or file_type != 'file':
            raise PinballException('path %s is not a file but a %s' %
                                   (path, file_type))
        body['sha'] = response_json['sha']

        # Create or update the file.
        self._make_request('DELETE', location, json.dumps(body),
                           [httplib.CREATED, httplib.OK])

    def _list_directory(self, directory, allow_not_found):
        location = 'repos/%s/configs/contents%s' % (PinballConfig.USER, directory)
        # Remove the trailing slash.
        assert directory[-1] == '/'
        location = location[:-1]
        # Check if path exists.
        expected_status = [httplib.OK]
        if allow_not_found:
            expected_status.append(httplib.NOT_FOUND)
        response_json = self._make_request('GET', location, None,
                                           expected_status)
        response = json.loads(response_json)
        file_type = GithubRepository._get_file_type(response)
        if not file_type:
            assert allow_not_found
            return []
        if file_type != 'dir':
            raise PinballException('path %s is not a dir but a %s' %
                                   (directory, file_type))
        assert type(response) == list
        result = []
        for entry in response:
            file_type = GithubRepository._get_file_type(entry)
            if file_type == 'file':
                result.append(entry['name'])
            elif file_type == 'dir':
                result.append('%s/' % entry['name'])
            else:
                raise PinballException('found content %s of unsupported type '
                                       '%s' % (entry['path'], file_type))
        return result
