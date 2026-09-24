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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.Test;

class ProxyRoutePlannerTest {

  private static final HttpHost PROXY = new HttpHost("http", "proxy.example.com", 3128);

  /** Exposes the protected routing decision, which is what the client actually calls. */
  private static HttpHost proxyFor(String nonProxyHosts, String targetHost) throws Exception {
    return new ProxyRoutePlanner(PROXY, nonProxyHosts) {
      HttpHost decide(String host) throws Exception {
        return determineProxy(new HttpHost("https", host, 443), null);
      }
    }.decide(targetHost);
  }

  @Test
  void aTargetOutsideTheBypassListGoesThroughTheProxy() throws Exception {
    assertEquals(PROXY, proxyFor(null, "api.example.com"));
    assertEquals(PROXY, proxyFor("localhost", "api.example.com"));
  }

  @Test
  void aBypassedTargetIsReachedDirectly() throws Exception {
    // A null proxy is how DefaultRoutePlanner expresses a direct route.
    assertNull(proxyFor("api.example.com", "api.example.com"));
  }

  @Test
  void withoutAProxyEveryRouteIsDirect() throws Exception {
    assertNull(
        new ProxyRoutePlanner(null, "localhost") {
          HttpHost decide() throws Exception {
            return determineProxy(new HttpHost("https", "api.example.com", 443), null);
          }
        }.decide());
  }

  @Test
  void entriesAreSeparatedByPipeCommaOrSemicolon() {
    ProxyRoutePlanner planner = new ProxyRoutePlanner(PROXY, "localhost, 10.*; *.internal");

    assertTrue(planner.bypasses("localhost"));
    assertTrue(planner.bypasses("10.1.2.3"));
    assertTrue(planner.bypasses("db.internal"));
    assertFalse(planner.bypasses("api.example.com"));
  }

  @Test
  void matchingIgnoresCase() {
    assertTrue(
        new ProxyRoutePlanner(PROXY, "*.Internal.Example.COM")
            .bypasses("HOST.internal.example.com"));
  }

  @Test
  void anEntryIsALiteralRatherThanARegularExpression() {
    ProxyRoutePlanner planner = new ProxyRoutePlanner(PROXY, "10.0.0.1");

    assertTrue(planner.bypasses("10.0.0.1"));
    // The dots are literal: they must not match any character.
    assertFalse(planner.bypasses("10x0y0z1"));
  }

  @Test
  void wildcardsMatchInAnyPosition() {
    assertTrue(new ProxyRoutePlanner(PROXY, "*.internal").bypasses("db.internal"));
    assertTrue(new ProxyRoutePlanner(PROXY, "10.*").bypasses("10.1.2.3"));
    assertTrue(new ProxyRoutePlanner(PROXY, "a*z").bypasses("abcz"));
    assertTrue(new ProxyRoutePlanner(PROXY, "*").bypasses("anything"));
  }

  @Test
  void emptyEntriesAndAnEmptyListAreIgnored() {
    assertTrue(new ProxyRoutePlanner(PROXY, "localhost||  |").bypasses("localhost"));
    assertFalse(new ProxyRoutePlanner(PROXY, "localhost||  |").bypasses("api.example.com"));
    assertFalse(new ProxyRoutePlanner(PROXY, "").bypasses("api.example.com"));
    assertFalse(new ProxyRoutePlanner(PROXY, null).bypasses("api.example.com"));
  }

  @Test
  void anUnknownHostNameNeverBypasses() {
    assertFalse(new ProxyRoutePlanner(PROXY, "localhost").bypasses(null));
  }
}
