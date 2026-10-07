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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.net.URI;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.routing.HttpRoutePlanner;
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

  @Test
  void credentialsAccumulateSoServerAndProxyKeepTheirOwn() throws Exception {
    HttpHost origin = new HttpHost("https", "api.example.com", 443);
    HttpHost proxy = new HttpHost("http", "proxy.example.com", 3128);

    HttpClientManager.HttpClientBuilderFacade facade =
        HttpClientManager.getInstance().createBuilder();
    facade.setCredentials("proxyuser", "proxypassword", new AuthScope(proxy));
    facade.setCredentials("serveruser", "serverpassword", new AuthScope(origin));

    CredentialsProvider provider = credentialsProviderOf(facade);
    assertNotNull(provider, "both calls must be kept, not the last one only");
    assertEquals(
        "serveruser",
        provider.getCredentials(new AuthScope(origin), null).getUserPrincipal().getName());
    assertEquals(
        "proxyuser",
        provider.getCredentials(new AuthScope(proxy), null).getUserPrincipal().getName());
  }

  @Test
  void credentialsWithoutAScopeAnswerAnyHost() throws Exception {
    HttpClientManager.HttpClientBuilderFacade facade =
        HttpClientManager.getInstance().createBuilder();
    facade.setCredentials("someone", "secret");

    CredentialsProvider provider = credentialsProviderOf(facade);
    assertNotNull(provider);
    assertEquals(
        "someone",
        provider
            .getCredentials(new AuthScope(new HttpHost("https", "anywhere.example.com", 443)), null)
            .getUserPrincipal()
            .getName());
  }

  @Test
  void aBypassedTargetIsRoutedDirectlyWhileOthersUseTheProxy() throws Exception {
    HttpClientManager.HttpClientBuilderFacade facade =
        HttpClientManager.getInstance().createBuilder();
    facade.setProxy("proxy.example.com", 3128);
    facade.setNonProxyHosts("localhost|*.internal");

    HttpRoutePlanner planner = routePlannerOf(facade);
    assertNotNull(planner, "a proxy must install a route planner, not a request-config proxy");
    assertNull(
        planner.determineRoute(new HttpHost("http", "db.internal", 80), context()).getProxyHost());
    assertEquals(
        "proxy.example.com",
        planner
            .determineRoute(new HttpHost("http", "api.example.com", 80), context())
            .getProxyHost()
            .getHostName());
  }

  @Test
  void withoutAProxyEveryRouteIsDirect() throws Exception {
    HttpClientManager.HttpClientBuilderFacade facade =
        HttpClientManager.getInstance().createBuilder();

    // HttpClient always installs a planner of its own, so the assertion is on the route it
    // produces rather than on the absence of a planner.
    assertNull(
        routePlannerOf(facade)
            .determineRoute(new HttpHost("http", "api.example.com", 80), context())
            .getProxyHost());
  }

  private static HttpClientContext context() {
    return HttpClientContext.create();
  }

  /** The credentials provider the facade put on the client it builds. */
  private static CredentialsProvider credentialsProviderOf(
      HttpClientManager.HttpClientBuilderFacade facade) throws Exception {
    return (CredentialsProvider) fieldOf(facade.build(), "credentialsProvider");
  }

  /** The route planner the facade put on the client it builds. */
  private static HttpRoutePlanner routePlannerOf(HttpClientManager.HttpClientBuilderFacade facade)
      throws Exception {
    return (HttpRoutePlanner) fieldOf(facade.build(), "routePlanner");
  }

  /**
   * Reads one field of the built client. HttpClient exposes neither its credentials provider nor
   * its route planner, and both are exactly what this facade is responsible for wiring up.
   */
  private static Object fieldOf(CloseableHttpClient client, String name) throws Exception {
    for (Class<?> type = client.getClass(); type != null; type = type.getSuperclass()) {
      try {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(client);
      } catch (NoSuchFieldException keepLooking) {
        // try the superclass
      }
    }
    throw new NoSuchFieldException(name + " on " + client.getClass());
  }
}
