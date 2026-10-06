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

package org.apache.hop.ai.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class AiTextUtilTest {

  @Test
  void redactSecretsStripsPasswordsAndKeys() {
    String xml = "<connection password=\"s3cret\" user=\"hop\"/>";
    String redacted = AiTextUtil.redactSecrets(xml);
    assertFalse(redacted.contains("s3cret"));
    assertTrue(redacted.contains("password=\"***\""));
    assertTrue(redacted.contains("user=\"hop\""));

    String json = "api_key=\"sk-live-123\" token='abc'";
    String jsonRedacted = AiTextUtil.redactSecrets(json);
    assertFalse(jsonRedacted.contains("sk-live-123"));
    assertFalse(jsonRedacted.contains("abc"));
  }

  @Test
  void redactSecretsStripsXmlElementsAndJsonKeys() {
    String xml = "<password>Encrypted 2be98afc86aa7f2e4cb79ce10be9b9d83</password>";
    String xmlRedacted = AiTextUtil.redactSecrets(xml);
    assertEquals("<password>***</password>", xmlRedacted);

    String json = "{\"password\": \"s3cret\", \"user\": \"hop\"}";
    String jsonRedacted = AiTextUtil.redactSecrets(json);
    assertFalse(jsonRedacted.contains("s3cret"));
    assertTrue(jsonRedacted.contains("\"password\": \"***\""));
    assertTrue(jsonRedacted.contains("\"user\": \"hop\""));
  }

  @Test
  void redactSecretsMasksEveryEncodedValue() {
    // Hop writes password=true fields through the encoder, whatever they are called.
    String json =
        "{\"storageAccountKey\":\"Encrypted 2be98afc86aa7f2e4cb79ce10be9b9d83\","
            + "\"x\":\"AES2 c2VjcmV0LXZhbHVlLTEyMw==\"}";
    String redacted = AiTextUtil.redactSecrets(json);
    assertFalse(redacted.contains("2be98afc"), redacted);
    assertFalse(redacted.contains("c2VjcmV0"), redacted);
  }

  @Test
  void redactSecretsMasksSecretFieldsByNameSuffix() {
    String json =
        "{\"secretKey\":\"k1\",\"awsSecretAccessKey\":\"k2\",\"privateKeyPassphrase\":\"k3\","
            + "\"keyPassphrase\":\"k4\",\"sasKey\":\"k5\",\"authorizationHeaderValue\":"
            + "\"Bearer k6\",\"credential\":\"k7\",\"clientSecret\":\"k8\",\"dbPassword\":\"k9\"}";
    String redacted = AiTextUtil.redactSecrets(json);
    for (int i = 1; i <= 9; i++) {
      assertFalse(redacted.contains("k" + i), redacted);
    }

    String xml = "<httpPassword>plain</httpPassword><proxyPassword>p2</proxyPassword>";
    String xmlRedacted = AiTextUtil.redactSecrets(xml);
    assertEquals("<httpPassword>***</httpPassword><proxyPassword>***</proxyPassword>", xmlRedacted);
  }

  @Test
  void redactSecretsMasksUrlCredentials() {
    String redacted =
        AiTextUtil.redactSecrets("jdbc:postgresql://reporter:s3cret@db.example.com:5432/sales");
    assertFalse(redacted.contains("s3cret"), redacted);
    assertTrue(redacted.contains("reporter:***@db.example.com"), redacted);
  }

  @Test
  void redactSecretsKeepsVariableReferences() {
    String json = "{\"password\":\"${DB_PASSWORD}\",\"apiKey\":\"%%API_KEY%%\"}";
    assertEquals(json, AiTextUtil.redactSecrets(json));
    String xml = "<password>${DB_PASSWORD}</password>";
    assertEquals(xml, AiTextUtil.redactSecrets(xml));
  }

  @Test
  void redactSecretsLeavesProseAndOrdinaryFieldsAlone() {
    String text = "Encrypted passwords use AES. The primaryKey and keyField stay.";
    assertEquals(text, AiTextUtil.redactSecrets(text));
    String json = "{\"hostname\":\"db\",\"username\":\"hop\",\"port\":\"5432\"}";
    assertEquals(json, AiTextUtil.redactSecrets(json));
  }

  @Test
  void truncateAddsMarker() {
    assertEquals("abc", AiTextUtil.truncate("abc", 10));
    assertTrue(AiTextUtil.truncate("abcdefghij", 4).startsWith("abcd"));
    assertTrue(AiTextUtil.truncate("abcdefghij", 4).contains("truncated"));
  }

  @Test
  void jsonStringEscapes() {
    assertEquals("null", AiTextUtil.jsonString(null));
    assertEquals("\"a\\\"b\"", AiTextUtil.jsonString("a\"b"));
  }
}
