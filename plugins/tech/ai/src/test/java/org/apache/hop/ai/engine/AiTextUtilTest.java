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
