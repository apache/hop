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
}
