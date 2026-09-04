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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.Test;

class HttpClientManagerTest {

  @Test
  void createHttpHostReadsAServerBasedAuthority() {
    HttpHost host = HttpClientManager.createHttpHost(URI.create("https://example.org:8443/api"));

    assertEquals("https", host.getSchemeName());
    assertEquals("example.org", host.getHostName());
    assertEquals(8443, host.getPort());
  }

  @Test
  void createHttpHostAcceptsAnUnderscoreInTheHostName() {
    // java.net.URI treats this authority as registry-based, so getHost(), getPort() and
    // getUserInfo() are all unavailable and HttpHost.create(URI) fails with a NullPointerException.
    HttpHost host =
        HttpClientManager.createHttpHost(URI.create("http://my_service.internal:8080/api"));

    assertEquals("http", host.getSchemeName());
    assertEquals("my_service.internal", host.getHostName());
    assertEquals(8080, host.getPort());
  }

  @Test
  void createHttpHostDefaultsThePortWhenTheAuthorityOmitsIt() {
    HttpHost host = HttpClientManager.createHttpHost(URI.create("http://my_service.internal/api"));

    assertEquals("my_service.internal", host.getHostName());
    assertEquals(-1, host.getPort());
  }

  @Test
  void createHttpHostDropsUserInfoFromTheOrigin() {
    HttpHost host =
        HttpClientManager.createHttpHost(
            URI.create("http://user:secret@my_service.internal:8080/"));

    assertEquals("my_service.internal", host.getHostName());
    assertEquals(8080, host.getPort());
  }

  @Test
  void createHttpHostKeepsIpv6LiteralsIntact() {
    HttpHost host = HttpClientManager.createHttpHost(URI.create("http://[::1]:8080/api"));

    assertEquals("[::1]", host.getHostName());
    assertEquals(8080, host.getPort());
  }

  @Test
  void createHttpHostRejectsAUriWithoutAHost() {
    assertThrows(
        IllegalArgumentException.class,
        () -> HttpClientManager.createHttpHost(URI.create("file:///tmp/data.json")));
  }

  @Test
  void createHttpHostRejectsANonNumericPort() {
    assertThrows(
        IllegalArgumentException.class,
        () -> HttpClientManager.createHttpHost(URI.create("http://my_service.internal:http/api")));
  }
}
