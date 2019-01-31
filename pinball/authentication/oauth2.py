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

"""Singleton classes needed for authentication."""

import binascii
import json
from re import compile

try:
    from hmac import compare_digest
except ImportError:
    def compare_digest(a, b):
        if len(a) != len(b):
            return False
        result = 0
        for x, y in zip(a, b):
            result |= ord(x) ^ ord(y)
        return result == 0

from Cryptodome.Cipher import AES, ARC2
from Cryptodome.Hash import HMAC, SHA256
from Cryptodome.Random import random
from django.http import HttpResponseRedirect
from oauth2client.client import OAuth2WebServerFlow

from pinball.config.pinball_config import PinballConfig


__author__ = 'Tongbo Huang, Devin Lundberg'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = ['Tongbo Huang', 'Devin Lundberg']
__license__ = 'Apache'
__version__ = '2.0'


class SingletonType(type):
    """This is a singleton metaclass.

    All classes created by this metaclass will only be initialized once.
    """
    def __call__(cls, *args, **kwargs):
        try:
            return cls.__instance
        except AttributeError:
            cls.__instance = super(SingletonType, cls).__call__(*args, **kwargs)
            return cls.__instance


class OAuth2Flow(object):
    """This class is used to create a google OAuth2WebServerFlow object with
    Pinball client credentials.

    You can created new ones from Google Cloud Console.
    """
    __metaclass__ = SingletonType
    _scope = 'https://www.googleapis.com/auth/userinfo.email '\
             'https://www.googleapis.com/auth/admin.directory.group.readonly'
    _redirect_uri = ''
    if not PinballConfig.UI_HOST:
        _redirect_uri = 'http://localhost:8080/oauth2callback/'
    else:
        _redirect_uri = 'https://%s/oauth2callback/' % PinballConfig.UI_HOST
    _flow = {}
    for domain in PinballConfig.AUTHENTICATION_DOMAINS:
        _flow[domain] = OAuth2WebServerFlow(client_id=PinballConfig.GOOGLE_CLIENT_ID,
                                            client_secret=PinballConfig.GOOGLE_CLIENT_SECRET,
                                            scope=_scope,
                                            redirect_uri=_redirect_uri,
                                            hd=domain)

    def get_flow(self, domain):
        """Getter for the google OAuth2WebServerFlow object with
        Pinball client credentials.

        Returns:
            The google OAuth2WebServerFlow object with Pinball client
            credentials.
        """
        return self._flow[domain]

    def domain_authenticated(self, domain):
        """Check if the given domain is authorized with the application.

        Returns:
            The domain to be checked.
        """
        return domain in self._flow.keys()

    def get_domains(self):
        """Get all authenticated domains.

        Returns:
            Authenticated domains
        """
        return PinballConfig.AUTHENTICATION_DOMAINS


class CryptoException(Exception):
    pass


class Crypter(object):
    """Implements modern authenticated encryption for strings

    The current version (1) uses HMAC-SHA256 and AES256-CBC using
    Encrypt-then-MAC. Should be secure >>>2030 according to NIST standards.
    http://www.keylength.com/ for a summary of algorithms and expected
    security. The message is padded with null characters. This means that
    strings with null characters should not use this method unless a
    different padding scheme is added

    AES in GCM mode is faster, but the pycrypto implementation is immature.
    This may be a better choice for future versions.

    There is currently legacy support for decryption under the old key
    using ARC2
    TODO(devinlundberg): remove legacy ARC2 decryption support
    """
    __metaclass__ = SingletonType
    # Remove legacy crypter in future.
    _legacy_crypter = ARC2.new(PinballConfig.SECRET_KEY, ARC2.MODE_ECB)
    _aes_block_size = 16
    _padding_char = '\x00'

    def _serialize(self, ciphertext, mac, **params):
        """Creates a serialized crypto object with current version and key"""
        encoded_params = {k: v.encode('base64') for k, v in params.items()}
        return json.dumps({
            'version': PinballConfig.CRYPTO_VERSION,
            'ciphertext': ciphertext.encode('base64'),
            'auth': mac.encode('base64'),
            'params': encoded_params
        }).encode('base64')

    def _deserialize(self, encoded_ciphertext):
        """Gets version, ciphertext, auth, and params from serialized object"""
        try:
            ciphertext_json = encoded_ciphertext.decode('base64')
        except binascii.Error:
            raise CryptoException('Invalid Base64')
        try:
            ciphertext_obj = json.loads(ciphertext_json)
        except ValueError:
            raise CryptoException('Invalid JSON format')
        if any(key not in ciphertext_obj
               for key in ('version', 'ciphertext', 'auth', 'params')):
            raise CryptoException('Invalid JSON parameters')
        version = ciphertext_obj['version']
        try:
            ciphertext = ciphertext_obj['ciphertext'].decode('base64')
            auth = ciphertext_obj['auth'].decode('base64')
            params = {k: v.decode('base64')
                      for k, v in ciphertext_obj['params'].items()}
        except binascii.Error:
            raise CryptoException('Invalid Base64')
        except AttributeError:
            raise CryptoException('Unsupported types')
        return version, ciphertext, auth, params

    def _cbc_hmac_sha256_decrypt(self, ciphertext, auth, iv):
        """Authenticated decrypt using AES-CBC and HMAC SHA256
        Encrypt-then-MAC"""
        hmac = HMAC.new(PinballConfig.HMAC_KEY, digestmod=SHA256)
        hmac.update(ciphertext)
        hmac.update(iv)
        if not compare_digest(hmac.hexdigest(), auth):
            raise CryptoException('Decryption Failed')
        aes = AES.new(PinballConfig.AES_CBC_KEY, AES.MODE_CBC, iv)
        return aes.decrypt(ciphertext).rstrip(self._padding_char)

    def encrypt(self, message):
        """Encrypts string of any length using the current crypto version

        Args:
            message: The string that needs to be encrypted.

        Returns:
            A serialized authenticated encrypted object
        """
        iv = ''.join(chr(random.randint(0, 255))
                     for _ in range(self._aes_block_size))
        aes = AES.new(PinballConfig.AES_CBC_KEY, AES.MODE_CBC, iv)
        hmac = HMAC.new(PinballConfig.HMAC_KEY, digestmod=SHA256)
        padded_length = (len(message) + self._aes_block_size -
                         len(message) % (self._aes_block_size))
        padded_message = message.ljust(padded_length, self._padding_char)
        ciphertext = aes.encrypt(padded_message)
        hmac.update(ciphertext)
        hmac.update(iv)
        return self._serialize(ciphertext, hmac.hexdigest(), iv=iv)

    def decrypt(self, encoded_ciphertext):
        """Deserializes and decrypts a string with the current or legacy
        algorithms

        Args:
            encoded_ciphertext: The string that needs to be decrypted.

        Returns:
            The decrypted message.

        Throws:
            CryptoException: on failed decryption.
        """
        try:
            version, ciphertext, auth, params = self._deserialize(encoded_ciphertext)
        except CryptoException:
            # This should raise an exception when support for ARC2 ends.
            return self._legacy_crypter.decrypt(encoded_ciphertext).rstrip('0')
        if version == 1:
            if 'iv' not in params:
                raise CryptoException('Missing IV')
            return self._cbc_hmac_sha256_decrypt(ciphertext, auth, params['iv'])
        else:
            raise CryptoException('Unsupported Crypto Version')


class RequireLogin(object):
    """This middleware requires a user to be authenticated to view any pages.

    Exemptions to this requirement can optionally be specified
    in settings via a list of regular expressions in EXEMPT_URLS (which
    you can copy from your urls.py).

    Requires authentication middleware and template context processors to be
    loaded. You'll get an error if they aren't.
    """
    def process_request(self, request):
        """The Login Required middleware requires authentication middleware to
        be installed. Edit your MIDDLEWARE_CLASSES setting to insert
        'django.contrib.auth.middlware.AuthenticationMiddleware'.
        If that doesn't work, ensure your TEMPLATE_CONTEXT_PROCESSORS setting
        includes 'django.core.context_processors.auth'.

        Args:
            request: The request sent towards any application url.

        Returns:
            Redirect user to signin page if the user has not logged in or their
            client side credential cookie is malform.
        """

        domains = OAuth2Flow().get_domains()
        EXEMPT_URLS = [compile('/signin/'),
                       compile('/oauth2callback/'),
                       compile('/logout/'),
                       compile('/static/.*')]
        path = request.path_info
        if any(m.match(path) for m in EXEMPT_URLS):
            pass
        elif 'login' in request.COOKIES and 'user_id' in request.COOKIES \
                and 'domain_url' in request.COOKIES:
            crypter = Crypter()
            try:
                user_id = crypter.decrypt(request.COOKIES['login'])
                domain = crypter.decrypt(request.COOKIES['domain_url'])
            except CryptoException:
                return HttpResponseRedirect('/signin/')
            if user_id == request.COOKIES['user_id'] and user_id != '' \
                    and domain in domains:
                pass
            else:
                return HttpResponseRedirect('/signin/')
        else:
            return HttpResponseRedirect('/signin/')
