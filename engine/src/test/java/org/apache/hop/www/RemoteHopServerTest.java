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

package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.hc.client5.http.auth.AuthCache;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.server.HopServerMeta;
import org.apache.hop.server.ServerConnectionManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

/**
 * Tests for the {@link RemoteHopServer} client. The connection metadata is held by {@link
 * HopServerMeta}; this test wraps it in a RemoteHopServer and exercises the HTTP interactions.
 */
class RemoteHopServerTest {
  RemoteHopServer hopServer;
  HopServerMeta serverMeta;
  IVariables variables;

  @BeforeAll
  static void beforeClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @BeforeEach
  void init() throws Exception {
    ServerConnectionManager connectionManager = ServerConnectionManager.getInstance();
    HttpClient httpClient = spy(connectionManager.createHttpClient());

    // mock response
    CloseableHttpResponse closeableHttpResponseMock = mock(CloseableHttpResponse.class);

    doReturn(HttpStatus.SC_NOT_FOUND).when(closeableHttpResponseMock).getCode();

    // mock entity
    HttpEntity httpEntityMock = mock(HttpEntity.class);
    doReturn(httpEntityMock).when(closeableHttpResponseMock).getEntity();

    doReturn(closeableHttpResponseMock).when(httpClient).execute(any(HttpGet.class));
    doReturn(closeableHttpResponseMock).when(httpClient).execute(any(HttpPost.class));
    doReturn(closeableHttpResponseMock)
        .when(httpClient)
        .execute(any(HttpPost.class), nullable(HttpClientContext.class));

    serverMeta = new HopServerMeta();
    hopServer = spy(new RemoteHopServer(serverMeta));
    variables = new Variables();
    doReturn(httpClient).when(hopServer).getHttpClient();
    doReturn("response_body").when(hopServer).getResponseBodyAsString(nullable(InputStream.class));
  }

  private ClassicHttpResponse mockResponse(int statusCode, String entityText) throws IOException {
    ClassicHttpResponse resp = mock(ClassicHttpResponse.class);
    when(resp.getCode()).thenReturn(statusCode);
    HttpEntity entity = mock(HttpEntity.class);
    when(entity.getContent())
        .thenReturn(new ByteArrayInputStream(entityText.getBytes(StandardCharsets.UTF_8)));
    when(resp.getEntity()).thenReturn(entity);
    return resp;
  }

  @Test
  void testExecService() throws Exception {
    String nonExistingAppName = "wrong_app_name";
    HttpGet httpGetMock = mock(HttpGet.class);

    URI uriMock = new URI(nonExistingAppName);
    doReturn(uriMock).when(httpGetMock).getUri();

    HttpClient clientMock = mock(HttpClient.class);
    when(clientMock.execute(any(ClassicHttpRequest.class), any(HttpContext.class)))
        .then(
            invocation -> {
              ClassicHttpRequest request = invocation.getArgument(0);
              if (request.getUri().equals(uriMock)) {
                return mockResponse(404, "");
              }
              return mockResponse(200, "");
            });
    when(hopServer.getHttpClient()).thenReturn(clientMock);

    doReturn(httpGetMock)
        .when(hopServer)
        .buildExecuteServiceMethod(any(IVariables.class), anyString(), anyMap());
    serverMeta.setHostname("hostNameStub");
    serverMeta.setUsername("userNAmeStub");

    assertThrows(
        HopException.class,
        () -> hopServer.execService(Variables.getADefaultVariableSpace(), nonExistingAppName));
  }

  @Test
  void testSendXml() throws Exception {
    serverMeta.setHostname("hostNameStub");
    serverMeta.setUsername("userNAmeStub");
    HttpPost httpPostMock = mock(HttpPost.class);
    URI uriMock = new URI("fake");
    doReturn(uriMock).when(httpPostMock).getUri();
    doReturn(httpPostMock)
        .when(hopServer)
        .buildSendXmlMethod(any(Variables.class), any(byte[].class), anyString());

    assertThrows(HopException.class, () -> hopServer.sendXml(variables, "", ""));
  }

  @Test
  void testSendExport() throws Exception {
    serverMeta.setHostname("hostNameStub");
    serverMeta.setUsername("userNAmeStub");
    HttpPost httpPostMock = mock(HttpPost.class);
    URI uriMock = new URI("fake");
    doReturn(uriMock).when(httpPostMock).getUri();
    doReturn(httpPostMock)
        .when(hopServer)
        .buildSendExportMethod(
            any(Variables.class), anyString(), anyString(), any(InputStream.class));
    File tempFile;
    tempFile = File.createTempFile("ApacheHop-", "tmp");
    tempFile.deleteOnExit();

    assertThrows(
        HopException.class,
        () -> hopServer.sendExport(variables, tempFile.getAbsolutePath(), "", ""));
  }

  @Test
  void testSendExportOk() throws Exception {
    serverMeta.setUsername("uname");
    serverMeta.setPassword("passw");
    serverMeta.setHostname("hname");
    serverMeta.setPort("1111");
    HttpPost httpPostMock = mock(HttpPost.class);
    URI uriMock = new URI("fake");
    final String responseContent = "baah";
    when(httpPostMock.getUri()).thenReturn(uriMock);
    doReturn(uriMock).when(httpPostMock).getUri();

    HttpClient client = mock(HttpClient.class);
    when(client.execute(any(ClassicHttpRequest.class), any(HttpContext.class)))
        .then((Answer<ClassicHttpResponse>) invocation -> mockResponse(200, responseContent));
    // override init
    when(hopServer.getHttpClient()).thenReturn(client);
    when(hopServer.getResponseBodyAsString(any())).thenCallRealMethod();

    doReturn(httpPostMock)
        .when(hopServer)
        .buildSendExportMethod(
            any(Variables.class), anyString(), anyString(), any(InputStream.class));
    File tempFile;
    tempFile = File.createTempFile("ApacheHop-", "tmp");
    tempFile.deleteOnExit();
    String result = hopServer.sendExport(variables, tempFile.getAbsolutePath(), null, null);
    assertEquals(responseContent, result);
  }

  @Test
  void testAddCredentials() {
    String testUser = "test_username";
    serverMeta.setUsername(testUser);
    String testPassword = "test_password";
    serverMeta.setPassword(testPassword);
    String host = "somehost";
    serverMeta.setHostname(host);
    int port = 1000;
    serverMeta.setPort("" + port);

    HttpClientContext auth = hopServer.getAuthContext(variables);
    var cred = auth.getCredentialsProvider().getCredentials(new AuthScope(host, port), null);
    assertEquals(testUser, cred.getUserPrincipal().getName());

    String user2 = "user2";
    serverMeta.setUsername(user2);
    serverMeta.setPassword("pass2");
    auth = hopServer.getAuthContext(variables);
    cred = auth.getCredentialsProvider().getCredentials(new AuthScope(host, port), null);
    assertEquals(user2, cred.getUserPrincipal().getName());
  }

  @Test
  void testAuthCredentialsSchemeWithSSL() {
    serverMeta.setUsername("admin");
    serverMeta.setPassword("password");
    serverMeta.setHostname("localhost");
    serverMeta.setPort("8443");
    serverMeta.setSslMode(true);

    AuthCache cache = hopServer.getAuthContext(variables).getAuthCache();
    assertNotNull(cache.get(new HttpHost("https", "localhost", 8443)));
    assertNull(cache.get(new HttpHost("http", "localhost", 8443)));
  }

  @Test
  void testAuthCredentialsSchemeWithoutSSL() {
    serverMeta.setUsername("admin");
    serverMeta.setPassword("password");
    serverMeta.setHostname("localhost");
    serverMeta.setPort("8080");
    serverMeta.setSslMode(false);

    AuthCache cache = hopServer.getAuthContext(variables).getAuthCache();
    assertNull(cache.get(new HttpHost("https", "localhost", 8080)));
    assertNotNull(cache.get(new HttpHost("http", "localhost", 8080)));
  }

  @Test
  void testConstructUrl() {
    serverMeta.setHostname("localhost");
    serverMeta.setPort("8080");
    assertEquals(
        "http://localhost:8080/hop/status", hopServer.constructUrl(variables, "/hop/status"));

    // https when ssl mode is enabled
    serverMeta.setSslMode(true);
    assertEquals(
        "https://localhost:8080/hop/status", hopServer.constructUrl(variables, "/hop/status"));
    serverMeta.setSslMode(false);

    // port 80 is left out of the URL
    serverMeta.setPort("80");
    assertEquals("http://localhost/hop/status", hopServer.constructUrl(variables, "/hop/status"));
    serverMeta.setPort("8080");

    // the web application name is inserted before the service path
    serverMeta.setWebAppName("web");
    assertEquals(
        "http://localhost:8080/web/hop/status", hopServer.constructUrl(variables, "/hop/status"));
    serverMeta.setWebAppName(null);

    // spaces are percent-encoded
    assertEquals("http://localhost:8080/hop/a%20b", hopServer.constructUrl(variables, "/hop/a b"));

    // localhost is rewritten to 127.0.0.1 when a proxy is configured
    serverMeta.setProxyHostname("proxy.example.com");
    assertEquals(
        "http://127.0.0.1:8080/hop/status", hopServer.constructUrl(variables, "/hop/status"));
  }

  @Test
  void testGetPortSpecification() {
    serverMeta.setPort("8080");
    assertEquals(":8080", hopServer.getPortSpecification(variables));
    serverMeta.setPort("80");
    assertEquals("", hopServer.getPortSpecification(variables));
    serverMeta.setPort("");
    assertEquals("", hopServer.getPortSpecification(variables));
  }

  @Test
  void testGetDelay() {
    // first attempt: backoff + a jitter of at most backoff/4
    assertTrue(RemoteHopServer.getDelay(0, 1000) >= 1000);
    assertTrue(RemoteHopServer.getDelay(0, 1000) < 1250);
    // the delay grows (fibonacci-style) with the number of attempts
    assertTrue(RemoteHopServer.getDelay(3, 1000) >= 3000);
  }
}
