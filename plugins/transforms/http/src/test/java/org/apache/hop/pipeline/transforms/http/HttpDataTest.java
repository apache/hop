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

package org.apache.hop.pipeline.transforms.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.row.RowMeta;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class HttpDataTest {

  private HttpData data;

  @BeforeEach
  void setUp() {
    data = new HttpData();
  }

  @Test
  void testDefaultConstructor() {
    assertNotNull(data, "HttpData should not be null");
    assertEquals(-1, data.indexOfUrlField, "indexOfUrlField should be -1 by default");
    assertNull(data.realProxyHost, "realProxyHost should be null by default");
    assertEquals(8080, data.realProxyPort, "realProxyPort should be 8080 by default");
    assertNull(data.realHttpLogin, "realHttpLogin should be null by default");
    assertNull(data.realHttpPassword, "realHttpPassword should be null by default");
  }

  @Test
  void testIndexOfUrlField() {
    data.indexOfUrlField = 5;
    assertEquals(5, data.indexOfUrlField);

    data.indexOfUrlField = -1;
    assertEquals(-1, data.indexOfUrlField);
  }

  @Test
  void testRealUrl() {
    assertNull(data.realUrl, "realUrl should be null initially");

    data.realUrl = "http://example.com";
    assertEquals("http://example.com", data.realUrl);
  }

  @Test
  void testRealProxyHost() {
    assertNull(data.realProxyHost, "realProxyHost should be null by default");

    data.realProxyHost = "proxy.example.com";
    assertEquals("proxy.example.com", data.realProxyHost);
  }

  @Test
  void testRealProxyPort() {
    assertEquals(8080, data.realProxyPort, "Default proxy port should be 8080");

    data.realProxyPort = 3128;
    assertEquals(3128, data.realProxyPort);
  }

  @Test
  void testRealHttpLogin() {
    assertNull(data.realHttpLogin, "realHttpLogin should be null by default");

    data.realHttpLogin = "username";
    assertEquals("username", data.realHttpLogin);
  }

  @Test
  void testRealHttpPassword() {
    assertNull(data.realHttpPassword, "realHttpPassword should be null by default");

    data.realHttpPassword = "password123";
    assertEquals("password123", data.realHttpPassword);
  }

  @Test
  void testArgnrs() {
    assertNull(data.argnrs, "argnrs should be null initially");

    data.argnrs = new int[] {0, 1, 2};
    assertEquals(3, data.argnrs.length);
    assertEquals(0, data.argnrs[0]);
    assertEquals(1, data.argnrs[1]);
    assertEquals(2, data.argnrs[2]);
  }

  @Test
  void testOutputRowMeta() {
    assertNull(data.outputRowMeta, "outputRowMeta should be null initially");

    data.outputRowMeta = new RowMeta();
    assertNotNull(data.outputRowMeta);
  }

  @Test
  void testInputRowMeta() {
    assertNull(data.inputRowMeta, "inputRowMeta should be null initially");

    data.inputRowMeta = new RowMeta();
    assertNotNull(data.inputRowMeta);
  }

  @Test
  void testHeaderParametersNrs() {
    assertNull(data.headerParametersNrs, "headerParametersNrs should be null initially");

    data.headerParametersNrs = new int[] {1, 3, 5};
    assertEquals(3, data.headerParametersNrs.length);
    assertEquals(1, data.headerParametersNrs[0]);
    assertEquals(3, data.headerParametersNrs[1]);
    assertEquals(5, data.headerParametersNrs[2]);
  }

  @Test
  void testUseHeaderParameters() {
    assertFalse(data.useHeaderParameters, "useHeaderParameters should be false by default");

    data.useHeaderParameters = true;
    assertEquals(true, data.useHeaderParameters);
  }

  @Test
  void testHeaderParameters() {
    assertNull(data.headerParameters, "headerParameters should be null initially");

    NameValuePair[] params =
        new NameValuePair[] {
          new BasicNameValuePair("Authorization", "Bearer token"),
          new BasicNameValuePair("Content-Type", "application/json")
        };
    data.headerParameters = params;

    assertNotNull(data.headerParameters);
    assertEquals(2, data.headerParameters.length);
    assertEquals("Authorization", data.headerParameters[0].getName());
    assertEquals("Bearer token", data.headerParameters[0].getValue());
  }

  @Test
  void testRealSocketTimeout() {
    assertEquals(0, data.realSocketTimeout, "realSocketTimeout should be 0 initially");

    data.realSocketTimeout = 5000;
    assertEquals(5000, data.realSocketTimeout);
  }

  @Test
  void testRealConnectionTimeout() {
    assertEquals(0, data.realConnectionTimeout, "realConnectionTimeout should be 0 initially");

    data.realConnectionTimeout = 3000;
    assertEquals(3000, data.realConnectionTimeout);
  }

  @Test
  void testWithoutPreviousTransforms() {
    assertFalse(
        data.withoutPreviousTransforms, "withoutPreviousTransforms should be false by default");

    data.withoutPreviousTransforms = true;
    assertTrue(data.withoutPreviousTransforms);
  }

  @Test
  void testMultipleFieldsSetAndGet() {
    // Set multiple fields
    data.indexOfUrlField = 10;
    data.realUrl = "http://api.example.com";
    data.realProxyHost = "proxy.corp.com";
    data.realProxyPort = 9090;
    data.realHttpLogin = "admin";
    data.realHttpPassword = "secret";
    data.realSocketTimeout = 15000;
    data.realConnectionTimeout = 8000;
    data.useHeaderParameters = true;
    data.withoutPreviousTransforms = true;

    // Verify all fields
    assertEquals(10, data.indexOfUrlField);
    assertEquals("http://api.example.com", data.realUrl);
    assertEquals("proxy.corp.com", data.realProxyHost);
    assertEquals(9090, data.realProxyPort);
    assertEquals("admin", data.realHttpLogin);
    assertEquals("secret", data.realHttpPassword);
    assertEquals(15000, data.realSocketTimeout);
    assertEquals(8000, data.realConnectionTimeout);
    assertTrue(data.useHeaderParameters);
    assertTrue(data.withoutPreviousTransforms);
  }

  @Test
  void testArgnrsWithEmptyArray() {
    data.argnrs = new int[0];
    assertNotNull(data.argnrs);
    assertEquals(0, data.argnrs.length);
  }

  @Test
  void testHeaderParametersWithEmptyArray() {
    data.headerParameters = new NameValuePair[0];
    assertNotNull(data.headerParameters);
    assertEquals(0, data.headerParameters.length);
  }

  @Test
  void testNegativeIndexOfUrlField() {
    data.indexOfUrlField = -999;
    assertEquals(-999, data.indexOfUrlField);
  }

  @Test
  void testZeroTimeouts() {
    data.realSocketTimeout = 0;
    data.realConnectionTimeout = 0;

    assertEquals(0, data.realSocketTimeout);
    assertEquals(0, data.realConnectionTimeout);
  }

  @Test
  void testLargeTimeouts() {
    data.realSocketTimeout = Integer.MAX_VALUE;
    data.realConnectionTimeout = Integer.MAX_VALUE;

    assertEquals(Integer.MAX_VALUE, data.realSocketTimeout);
    assertEquals(Integer.MAX_VALUE, data.realConnectionTimeout);
  }
}
