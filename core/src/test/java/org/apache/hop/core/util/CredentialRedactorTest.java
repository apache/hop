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

package org.apache.hop.core.util;

import static org.apache.hop.core.util.CredentialRedactor.MASK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class CredentialRedactorTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "Authorization",
        "Proxy-Authorization",
        "Cookie",
        "Set-Cookie",
        "X-Api-Key",
        "api_key",
        "apiKey",
        "key",
        "access_token",
        "X-Auth-Token",
        "client_secret",
        "password",
        "passwd",
        "X-Amz-Signature",
        "X-Amz-Credential",
        "sig",
        "Ocp-Apim-Subscription-Key",
        "JSESSIONID"
      })
  void credentialNames(String name) {
    assertTrue(CredentialRedactor.isSensitiveName(name), name);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"Content-Type", "Accept", "q", "page", "limit", "author", "id", "monkey", ""})
  void ordinaryNames(String name) {
    assertFalse(CredentialRedactor.isSensitiveName(name), name);
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "https://user:secret@example.com/file|https://" + MASK + "@example.com/file",
        "http://user@example.com:8080/|http://" + MASK + "@example.com:8080/",
        "https://example.com/a?q=hop&api_key=abc123&page=2|https://example.com/a?q=hop&api_key="
            + MASK
            + "&page=2",
        "https://example.com/cb#access_token=abc&state=x|https://example.com/cb#access_token="
            + MASK
            + "&state=x",
        "https://acct.blob.core.windows.net/c/b?sv=2022&sig=abc%2Fdef|"
            + "https://acct.blob.core.windows.net/c/b?sv=2022&sig="
            + MASK,
        "https://example.com/plain/path|https://example.com/plain/path"
      })
  void urls(String url, String expected) {
    assertEquals(expected, CredentialRedactor.redact(url));
  }

  @Test
  void aUrlInsideAnExceptionMessage() {
    String message = "Illegal character in path at index 30: http://user:secret@example.com/a b";

    assertEquals(
        "Illegal character in path at index 30: http://" + MASK + "@example.com/a b",
        CredentialRedactor.redact(message));
  }

  @Test
  void jsonBodies() {
    String body = "{\"user\":\"hop\",\"password\" : \"p\\\"w\",\"access_token\":\"xyz\",\"n\":1}";

    assertEquals(
        "{\"user\":\"hop\",\"password\" : \""
            + MASK
            + "\",\"access_token\":\""
            + MASK
            + "\",\"n\":1}",
        CredentialRedactor.redact(body));
  }

  @Test
  void formBodies() {
    assertEquals(
        "grant_type=client_credentials&client_secret=" + MASK,
        CredentialRedactor.redact("grant_type=client_credentials&client_secret=s3cr3t"));
  }

  @Test
  void authorizationSchemesInText() {
    assertEquals(
        "sent Bearer " + MASK + " to the server",
        CredentialRedactor.redact("sent Bearer eyJhbGciOiJIUzI1NiJ9.abc to the server"));
  }

  @Test
  void values() {
    assertEquals(MASK, CredentialRedactor.redactValue("Authorization", "Basic dXNlcjpwYXNz"));
    assertEquals("application/json", CredentialRedactor.redactValue("Accept", "application/json"));
    assertEquals(
        "http://" + MASK + "@host/", CredentialRedactor.redactValue("Referer", "http://u:p@host/"));
    assertEquals("", CredentialRedactor.redactValue("password", ""));
    assertNull(CredentialRedactor.redact(null));
  }
}
