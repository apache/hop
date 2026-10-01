/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.metadata.rest.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.Test;

/**
 * The proxy decision is made per request, so a REST transform reading its URL from an input field
 * gets the bypass list evaluated against the host each row actually targets.
 */
class RestProxyRoutePlannerTest {

  private static RestClientSettings withProxy(String nonProxyHosts) {
    RestClientSettings settings = new RestClientSettings();
    settings.setProxyHost("proxy.example.com");
    settings.setProxyPort(3128);
    settings.setNonProxyHosts(nonProxyHosts);
    return settings;
  }

  private static HttpHost determineProxy(RestClientSettings settings, String host)
      throws Exception {
    return new RestProxyRoutePlanner(settings) {
      HttpHost proxyFor(String target) throws Exception {
        return determineProxy(new HttpHost("https", target, 443), null);
      }
    }.proxyFor(host);
  }

  @Test
  void aPlainProxyDefaultsToHttpOnPort8080() {
    RestClientSettings settings = new RestClientSettings();
    settings.setProxyHost("proxy.example.com");

    HttpHost proxy = RestProxyRoutePlanner.proxyOf(settings);

    assertEquals("http", proxy.getSchemeName());
    assertEquals("proxy.example.com", proxy.getHostName());
    assertEquals(8080, proxy.getPort());
  }

  @Test
  void anHttpsProxyDefaultsToPort443() {
    RestClientSettings settings = new RestClientSettings();
    settings.setProxyScheme("https");
    settings.setProxyHost("proxy.example.com");

    HttpHost proxy = RestProxyRoutePlanner.proxyOf(settings);

    // Reaching the proxy itself over TLS is exactly what the JDK connector could never do.
    assertEquals("https", proxy.getSchemeName());
    assertEquals(443, proxy.getPort());
  }

  @Test
  void noProxyHostMeansNoProxy() {
    assertNull(RestProxyRoutePlanner.proxyOf(new RestClientSettings()));
  }

  @Test
  void withoutABypassListEveryHostGoesThroughTheProxy() throws Exception {
    RestClientSettings settings = withProxy(null);

    assertEquals("proxy.example.com", determineProxy(settings, "api.example.com").getHostName());
    assertEquals("proxy.example.com", determineProxy(settings, "localhost").getHostName());
  }

  @Test
  void aListedHostIsReachedDirectly() throws Exception {
    RestClientSettings settings = withProxy("localhost|internal.example.com");

    assertNull(determineProxy(settings, "localhost"));
    assertNull(determineProxy(settings, "internal.example.com"));
    assertEquals("proxy.example.com", determineProxy(settings, "api.example.com").getHostName());
  }

  @Test
  void wildcardsMatchTheJdkSyntax() throws Exception {
    RestClientSettings settings = withProxy("127.*|*.internal.example.com");

    assertNull(determineProxy(settings, "127.0.0.1"));
    assertNull(determineProxy(settings, "host.internal.example.com"));
    assertEquals("proxy.example.com", determineProxy(settings, "127x0x0x1").getHostName());
    assertEquals(
        "proxy.example.com",
        determineProxy(settings, "internal.example.com.evil.net").getHostName());
  }

  @Test
  void commasAndSemicolonsWorkAsSeparatorsToo() {
    RestProxyRoutePlanner planner =
        new RestProxyRoutePlanner(withProxy("localhost, 10.*; *.internal"));

    assertTrue(planner.bypasses("localhost"));
    assertTrue(planner.bypasses("10.1.2.3"));
    assertTrue(planner.bypasses("db.internal"));
    assertFalse(planner.bypasses("api.example.com"));
  }

  @Test
  void matchingIsCaseInsensitive() {
    assertTrue(
        new RestProxyRoutePlanner(withProxy("*.Internal.Example.COM"))
            .bypasses("HOST.internal.example.com"));
  }

  @Test
  void aDottedEntryIsNotTreatedAsARegularExpression() {
    // "10.0.0.1" must not match "10x0y0z1": the dots are literal.
    RestProxyRoutePlanner planner = new RestProxyRoutePlanner(withProxy("10.0.0.1"));

    assertTrue(planner.bypasses("10.0.0.1"));
    assertFalse(planner.bypasses("10x0y0z1"));
  }

  @Test
  void blankEntriesAreIgnored() {
    RestProxyRoutePlanner planner = new RestProxyRoutePlanner(withProxy("localhost||  |"));

    assertTrue(planner.bypasses("localhost"));
    assertFalse(planner.bypasses("api.example.com"));
  }
}
