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

package org.apache.hop.pipeline.transforms.salesforce;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.sforce.ws.ConnectorConfig;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.KeyPair;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

/**
 * Tests that every authentication path applies the JVM proxy configuration to the SOAP connector
 * config. The Salesforce connector defaults to {@link Proxy#NO_PROXY}, which bypasses the JVM proxy
 * selector, so a config without an explicit proxy connects directly and times out behind a proxy.
 */
@WireMockTest
class SalesforceConnectionProxyTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String PROXY_HOST = "proxy.example.com";
  private static final String PROXY_PORT = "3128";
  private static final String INSTANCE_URL = "https://na123.salesforce.com";

  private static final String[] PROXY_PROPERTIES = {
    "http.proxyHost",
    "http.proxyPort",
    "https.proxyHost",
    "https.proxyPort",
    "http.nonProxyHosts",
    "http.proxyUser",
    "http.proxyPassword"
  };

  private ILogChannel mockLog;
  private String testPrivateKey;

  @BeforeAll
  static void setUpClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @BeforeEach
  void setUp() throws Exception {
    mockLog = Mockito.mock(ILogChannel.class);
    KeyPair keyPair = SalesforceTestUtils.generateTestKeyPair();
    testPrivateKey = SalesforceTestUtils.privateKeyToPem(keyPair.getPrivate());
    clearProxyProperties();
  }

  @AfterEach
  void tearDown() {
    clearProxyProperties();
    mockLog = null;
  }

  private void clearProxyProperties() {
    for (String property : PROXY_PROPERTIES) {
      System.clearProperty(property);
    }
  }

  private SalesforceConnection jwtConnection(String tokenEndpoint) throws HopException {
    return SalesforceConnection.createJwtConnection(
        mockLog, "test.user@example.com", "3MVG9TestConsumerKey", testPrivateKey, tokenEndpoint);
  }

  private Proxy selectProxy(SalesforceConnection connection, String endpoint) throws Exception {
    Method method = SalesforceConnection.class.getDeclaredMethod("selectProxy", String.class);
    method.setAccessible(true);
    return (Proxy) method.invoke(connection, endpoint);
  }

  private void stubTokenEndpoint(WireMockRuntimeInfo wmRuntimeInfo) {
    wmRuntimeInfo
        .getWireMock()
        .register(
            post(urlEqualTo("/services/oauth2/token"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withBody(
                            SalesforceTestUtils.buildOAuthTokenResponse(
                                "00D5g000001JvToken!Test", null, INSTANCE_URL))));
  }

  /** Regression test for the OAuth JWT path connecting directly and timing out behind a proxy. */
  @Test
  void testConnectWithOAuthJwt_appliesProxyToSoapConfig(WireMockRuntimeInfo wmRuntimeInfo)
      throws Exception {
    stubTokenEndpoint(wmRuntimeInfo);
    System.setProperty("https.proxyHost", PROXY_HOST);
    System.setProperty("https.proxyPort", PROXY_PORT);

    SalesforceConnection connection = jwtConnection(wmRuntimeInfo.getHttpBaseUrl());
    connection.connect();

    ConnectorConfig config = connection.getBinding().getConfig();
    assertEquals(
        new Proxy(Proxy.Type.HTTP, InetSocketAddress.createUnresolved(PROXY_HOST, 3128)),
        config.getProxy(),
        "The SOAP connector config must carry the configured proxy");
    assertEquals(INSTANCE_URL + "/services/Soap/u/64.0", config.getServiceEndpoint());
  }

  @Test
  void testConnectWithOAuthJwt_appliesProxyCredentials(WireMockRuntimeInfo wmRuntimeInfo)
      throws Exception {
    stubTokenEndpoint(wmRuntimeInfo);
    System.setProperty("https.proxyHost", PROXY_HOST);
    System.setProperty("https.proxyPort", PROXY_PORT);
    System.setProperty("http.proxyUser", "proxyuser");
    System.setProperty("http.proxyPassword", "proxypassword");

    SalesforceConnection connection = jwtConnection(wmRuntimeInfo.getHttpBaseUrl());
    connection.connect();

    ConnectorConfig config = connection.getBinding().getConfig();
    assertEquals("proxyuser", config.getProxyUsername());
    assertEquals("proxypassword", config.getProxyPassword());
  }

  @Test
  void testConnectWithOAuthJwt_noProxyConfigured(WireMockRuntimeInfo wmRuntimeInfo)
      throws Exception {
    stubTokenEndpoint(wmRuntimeInfo);

    SalesforceConnection connection = jwtConnection(wmRuntimeInfo.getHttpBaseUrl());
    connection.connect();

    assertSame(
        Proxy.NO_PROXY,
        connection.getBinding().getConfig().getProxy(),
        "Without proxy properties the connection must stay direct");
  }

  /** The Salesforce endpoints are https, so https.proxyHost has to be picked up. */
  @Test
  void testSelectProxy_httpsProxyHost() throws Exception {
    System.setProperty("https.proxyHost", PROXY_HOST);
    System.setProperty("https.proxyPort", PROXY_PORT);

    Proxy proxy = selectProxy(jwtConnection("https://login.salesforce.com"), INSTANCE_URL);

    assertNotNull(proxy);
    assertEquals(PROXY_HOST, ((InetSocketAddress) proxy.address()).getHostString());
    assertEquals(3128, ((InetSocketAddress) proxy.address()).getPort());
  }

  /** Hop has always applied http.proxyHost to the https Salesforce endpoints; keep doing that. */
  @Test
  void testSelectProxy_fallsBackToHttpProxyHost() throws Exception {
    System.setProperty("http.proxyHost", PROXY_HOST);
    System.setProperty("http.proxyPort", PROXY_PORT);

    Proxy proxy = selectProxy(jwtConnection("https://login.salesforce.com"), INSTANCE_URL);

    assertNotNull(proxy);
    assertEquals(PROXY_HOST, ((InetSocketAddress) proxy.address()).getHostString());
    assertEquals(3128, ((InetSocketAddress) proxy.address()).getPort());
  }

  @Test
  void testSelectProxy_honoursNonProxyHosts() throws Exception {
    System.setProperty("https.proxyHost", PROXY_HOST);
    System.setProperty("https.proxyPort", PROXY_PORT);
    System.setProperty("http.nonProxyHosts", "*.salesforce.com");

    assertNull(selectProxy(jwtConnection("https://login.salesforce.com"), INSTANCE_URL));
  }

  @Test
  void testSelectProxy_honoursNonProxyHostsOnTheHttpFallback() throws Exception {
    System.setProperty("http.proxyHost", PROXY_HOST);
    System.setProperty("http.proxyPort", PROXY_PORT);
    System.setProperty("http.nonProxyHosts", "*.salesforce.com");

    assertNull(selectProxy(jwtConnection("https://login.salesforce.com"), INSTANCE_URL));
  }

  @Test
  void testSelectProxy_noProxyConfigured() throws Exception {
    assertNull(selectProxy(jwtConnection("https://login.salesforce.com"), INSTANCE_URL));
  }

  @Test
  void testSelectProxy_malformedEndpoint() throws Exception {
    System.setProperty("https.proxyHost", PROXY_HOST);
    System.setProperty("https.proxyPort", PROXY_PORT);

    assertNull(selectProxy(jwtConnection("https://login.salesforce.com"), "not a valid uri"));
  }
}
