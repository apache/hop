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

package org.apache.hop.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import java.util.ArrayList;
import java.util.List;
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
import org.apache.hop.utils.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

/**
 * Tests for HopServer class
 *
 * @see HopServerMeta
 */
class HopServerTest {
  HopServerMeta hopServer;
  IVariables variables;

  @BeforeAll
  static void beforeClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @AfterAll
  static void tearDown() {
    PluginRegistry.getInstance().reset();
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

    hopServer = spy(new HopServerMeta());
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
    hopServer.setHostname("hostNameStub");
    hopServer.setUsername("userNAmeStub");

    assertThrows(
        HopException.class,
        () -> hopServer.execService(Variables.getADefaultVariableSpace(), nonExistingAppName));
  }

  @Test
  void testSendXml() throws Exception {
    hopServer.setHostname("hostNameStub");
    hopServer.setUsername("userNAmeStub");
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
    hopServer.setHostname("hostNameStub");
    hopServer.setUsername("userNAmeStub");
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
    hopServer.setUsername("uname");
    hopServer.setPassword("passw");
    hopServer.setHostname("hname");
    hopServer.setPort("1111");
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
    hopServer.setUsername(testUser);
    String testPassword = "test_password";
    hopServer.setPassword(testPassword);
    String host = "somehost";
    hopServer.setHostname(host);
    int port = 1000;
    hopServer.setPort("" + port);

    HttpClientContext auth = hopServer.getAuthContext(variables);
    var cred = auth.getCredentialsProvider().getCredentials(new AuthScope(host, port), null);
    assertEquals(testUser, cred.getUserPrincipal().getName());

    String user2 = "user2";
    hopServer.setUsername(user2);
    hopServer.setPassword("pass2");
    auth = hopServer.getAuthContext(variables);
    cred = auth.getCredentialsProvider().getCredentials(new AuthScope(host, port), null);
    assertEquals(user2, cred.getUserPrincipal().getName());
  }

  @Test
  void testAuthCredentialsSchemeWithSSL() {
    hopServer.setUsername("admin");
    hopServer.setPassword("password");
    hopServer.setHostname("localhost");
    hopServer.setPort("8443");
    hopServer.setSslMode(true);

    AuthCache cache = hopServer.getAuthContext(variables).getAuthCache();
    assertNotNull(cache.get(new HttpHost("https", "localhost", 8443)));
    assertNull(cache.get(new HttpHost("http", "localhost", 8443)));
  }

  @Test
  void testAuthCredentialsSchemeWithoutSSL() {
    hopServer.setUsername("admin");
    hopServer.setPassword("password");
    hopServer.setHostname("localhost");
    hopServer.setPort("8080");
    hopServer.setSslMode(false);

    AuthCache cache = hopServer.getAuthContext(variables).getAuthCache();
    assertNull(cache.get(new HttpHost("https", "localhost", 8080)));
    assertNotNull(cache.get(new HttpHost("http", "localhost", 8080)));
  }

  @Test
  void testModifyingName() {
    hopServer.setName("test");
    List<HopServerMeta> list = new ArrayList<>();
    list.add(hopServer);

    HopServerMeta hopServer2 = spy(new HopServerMeta());
    hopServer2.setName("test");

    hopServer2.verifyAndModifyHopServerName(list, null);

    assertNotEquals(hopServer.getName(), hopServer2.getName());
  }

  @Test
  void testEqualsHashCodeConsistency() {
    HopServerMeta server = new HopServerMeta();
    server.setName("server");
    TestUtils.checkEqualsHashCodeConsistency(server, server);

    HopServerMeta serverSame = new HopServerMeta();
    serverSame.setName("server");
    assertEquals(server, serverSame);
    TestUtils.checkEqualsHashCodeConsistency(server, serverSame);

    HopServerMeta serverCaps = new HopServerMeta();
    serverCaps.setName("SERVER");
    TestUtils.checkEqualsHashCodeConsistency(server, serverCaps);

    HopServerMeta serverOther = new HopServerMeta();
    serverOther.setName("something else");
    TestUtils.checkEqualsHashCodeConsistency(server, serverOther);
  }
}
