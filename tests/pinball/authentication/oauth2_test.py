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

"""Validation tests for oauth2"""
import json
import unittest

from pinball.authentication.oauth2 import Crypter, CryptoException
from pinball.config.pinball_config import PinballConfig


__author__ = 'Tongbo Huang, Devin Lundberg'
__copyright__ = 'Copyright 2015, Pinterest, Inc.'
__credits__ = ['Tongbo Huang', 'Devin Lundberg']
__license__ = 'Apache'
__version__ = '2.0'


class CrypterTestCase(unittest.TestCase):
    """Test cases for Crypter"""
    plaintext = 'secret'
    arc2_ciphertext = '\x1a\x9c^\xbc\xe5\xef\xf9\x97'

    def setUp(self):
        self.crypter = Crypter()

    def test_encryption(self):
        """Encrypt and decrypt a test string."""

        encrypted_string = self.crypter.encrypt(self.plaintext)
        decrypted_string = self.crypter.decrypt(encrypted_string)

        self.assertNotEqual(encrypted_string, self.plaintext)
        self.assertEqual(decrypted_string, self.plaintext)

    def test_nondeterministic_encryption(self):
        """Makes sure encryption is nondeterministic"""
        encrypted_string = self.crypter.encrypt(self.plaintext)
        encrypted_string2 = self.crypter.encrypt(self.plaintext)
        self.assertNotEqual(encrypted_string, encrypted_string2)

    def test_legacy_encryption(self):
        """Decrypt arc2 ciphertext (for legacy support)"""
        decrypted_string = self.crypter.decrypt(self.arc2_ciphertext)

        self.assertEqual(decrypted_string, self.plaintext)

    def test_serialize_deserialize(self):
        ciphertext = 'ciphertext'
        auth = 'auth'
        test = 'test'
        out = self.crypter._serialize(ciphertext, auth, test=test)
        v, ret_ciphertext, ret_auth, params = self.crypter._deserialize(out)

        self.assertEqual(v, PinballConfig.CRYPTO_VERSION)
        self.assertEqual(ciphertext, ret_ciphertext)
        self.assertEqual(auth, ret_auth)
        self.assertEqual(1, len(params))
        self.assertTrue('test' in params)
        self.assertEqual(params['test'], test)

    def test_deserialize_encoding_error(self):
        unencoded_data = json.dumps({
            'ciphertext': '',
            'auth': '',
            'params': {}
        })
        with self.assertRaises(CryptoException):
            self.crypter._deserialize(unencoded_data)

    def test_deserialize_not_json_error(self):
        not_json_data = 'This is not json'.encode('base64')
        with self.assertRaises(CryptoException):
            self.crypter._deserialize(not_json_data)

    def test_deserialize_wrong_json_error(self):
        serialized_without_version = json.dumps({
            'ciphertext': '',
            'auth': '',
            'params': {}
        }).encode('base64')
        with self.assertRaises(CryptoException):
            self.crypter._deserialize(serialized_without_version)
