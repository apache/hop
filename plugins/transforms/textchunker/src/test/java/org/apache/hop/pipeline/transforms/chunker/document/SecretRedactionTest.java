/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.pipeline.transforms.chunker.document;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SecretRedactionTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "password",
        "PASSWORD",
        "proxyPassword",
        "client_secret",
        "API-KEY",
        "apiKey",
        "privateKey",
        "private_key",
        "accessToken",
        "passphrase",
        "credentials"
      })
  void recognisesCredentialNames(String name) {
    assertTrue(SecretRedaction.isSecretName(name), name);
  }

  @ParameterizedTest
  @ValueSource(strings = {"key", "keyField", "name", "hostname", "databaseName", "lookupKeys"})
  void leavesOrdinaryFieldNamesAlone(String name) {
    // A bare "key" is a normal field name in lookups and joins. Redacting it would gut the chunk.
    assertFalse(SecretRedaction.isSecretName(name), name);
  }

  @Test
  void redactsHopEncryptedValuesWhateverTheFieldIsCalled() {
    assertEquals(
        SecretRedaction.REDACTED, SecretRedaction.redact("someField", "Encrypted 2be98afc86aa"));
  }

  @Test
  void leavesOrdinaryValuesUntouched() {
    assertEquals("localhost", SecretRedaction.redact("hostname", "localhost"));
  }

  @Test
  void keepsValuesThatOnlyReferenceAVariable() {
    // ${PROD_DB_PASSWORD} names where the credential comes from, it is not the credential.
    assertEquals("${PROD_DB_PASSWORD}", SecretRedaction.redact("password", "${PROD_DB_PASSWORD}"));
    assertEquals(SecretRedaction.REDACTED, SecretRedaction.redact("password", "x${SUFFIX}"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "AES 2be98afc86aa7f2e4cb79ce10df90acde",
        "AES2 q83vEjRWeJCrze8SNFZ4kA==",
        "Encrypted 2be98afc86aa"
      })
  void recognisesEncodedPasswords(String value) {
    assertTrue(SecretRedaction.isEncryptedValue(value), value);
  }

  @ParameterizedTest
  @ValueSource(strings = {"AES 256", "AES2 mode", "Encryption at rest"})
  void leavesTextThatOnlyStartsLikeAnEncoderPrefixAlone(String value) {
    assertFalse(SecretRedaction.isEncryptedValue(value), value);
  }

  @Test
  void restConnectionHeaderValueIsASecretButItsNameAndPrefixAreNot() {
    assertTrue(SecretRedaction.isSecretName("auth_header_value"));
    assertFalse(SecretRedaction.isSecretName("auth_header_name"));
    assertFalse(SecretRedaction.isSecretName("auth_header_prefix"));
    // An OAuth authorization URL is not a secret, so "authorization" is only checked on entry
    // names, such as a header called Authorization.
    assertFalse(SecretRedaction.isSecretName("oauth2_authorization_url"));
    assertTrue(SecretRedaction.isSecretEntryName("Authorization"));
    assertTrue(SecretRedaction.isSecretEntryName("Proxy-Authorization"));
    assertTrue(SecretRedaction.isSecretEntryName("API_TOKEN"));
    assertFalse(SecretRedaction.isSecretEntryName("Accept"));
  }
}
