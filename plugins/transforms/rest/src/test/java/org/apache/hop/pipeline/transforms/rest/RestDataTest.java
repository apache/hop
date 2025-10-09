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

package org.apache.hop.pipeline.transforms.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class RestDataTest {

  @Test
  void testDefaultConstructor() {
    RestData data = new RestData();
    assertNotNull(data);
    assertEquals(-1, data.indexOfUrlField);
    assertNull(data.realProxyHost);
    assertEquals(8080, data.realProxyPort);
    assertNull(data.realHttpLogin);
    assertNull(data.realHttpPassword);
    assertNull(data.resultFieldName);
    assertNull(data.resultCodeFieldName);
    assertNull(data.resultResponseFieldName);
    assertNull(data.resultHeaderFieldName);
    assertEquals(0, data.nrParams);
    assertEquals(0, data.nrMatrixParams);
    assertNull(data.method);
    assertEquals(-1, data.indexOfBodyField);
    assertEquals(-1, data.indexOfMethod);
    assertNull(data.config);
    assertNull(data.trustStoreFile);
    assertNull(data.trustStorePassword);
    assertNull(data.basicAuthentication);
    assertNull(data.sslContext);
  }

  @Test
  void testFieldAssignments() {
    RestData data = new RestData();

    data.indexOfUrlField = 5;
    data.realUrl = "http://example.com";
    data.method = "GET";
    data.realConnectionTimeout = 5000;
    data.realReadTimeout = 10000;
    data.indexOfMethod = 2;
    data.nrheader = 3;
    data.nrParams = 4;
    data.nrMatrixParams = 2;
    data.realProxyHost = "proxy.example.com";
    data.realProxyPort = 8888;
    data.realHttpLogin = "user";
    data.realHttpPassword = "pass";
    data.resultFieldName = "result";
    data.resultCodeFieldName = "code";
    data.resultResponseFieldName = "responseTime";
    data.resultHeaderFieldName = "headers";
    data.useHeaders = true;
    data.useParams = true;
    data.useMatrixParams = true;
    data.useBody = true;
    data.indexOfBodyField = 7;
    data.trustStoreFile = "/path/to/truststore";
    data.trustStorePassword = "trustpass";

    assertEquals(5, data.indexOfUrlField);
    assertEquals("http://example.com", data.realUrl);
    assertEquals("GET", data.method);
    assertEquals(5000, data.realConnectionTimeout);
    assertEquals(10000, data.realReadTimeout);
    assertEquals(2, data.indexOfMethod);
    assertEquals(3, data.nrheader);
    assertEquals(4, data.nrParams);
    assertEquals(2, data.nrMatrixParams);
    assertEquals("proxy.example.com", data.realProxyHost);
    assertEquals(8888, data.realProxyPort);
    assertEquals("user", data.realHttpLogin);
    assertEquals("pass", data.realHttpPassword);
    assertEquals("result", data.resultFieldName);
    assertEquals("code", data.resultCodeFieldName);
    assertEquals("responseTime", data.resultResponseFieldName);
    assertEquals("headers", data.resultHeaderFieldName);
    assertEquals(true, data.useHeaders);
    assertEquals(true, data.useParams);
    assertEquals(true, data.useMatrixParams);
    assertEquals(true, data.useBody);
    assertEquals(7, data.indexOfBodyField);
    assertEquals("/path/to/truststore", data.trustStoreFile);
    assertEquals("trustpass", data.trustStorePassword);
  }

  @Test
  void testDefaultBooleanValues() {
    RestData data = new RestData();
    assertFalse(data.useHeaders);
    assertFalse(data.useParams);
    assertFalse(data.useMatrixParams);
    assertFalse(data.useBody);
  }
}
