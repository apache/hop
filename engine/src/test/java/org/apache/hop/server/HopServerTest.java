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

package org.apache.hop.server;

import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.utils.TestUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for HopServer class
 *
 * @author Pavel Sakun
 * @see HopServer
 */
public class HopServerTest {
  HopServer hopServer;
  IVariables variables;

  @BeforeClass
  public static void beforeClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @AfterClass
  public static void tearDown() {
    PluginRegistry.getInstance().reset();
  }

  @Before
  public void init() throws IOException {
    ServerConnectionManager connectionManager = ServerConnectionManager.getInstance();
    HttpClient httpClient = spy(connectionManager.createHttpClient());

    // mock response
    CloseableHttpResponse closeableHttpResponseMock = mock(CloseableHttpResponse.class);

    // mock status line
    StatusLine statusLineMock = mock(StatusLine.class);
    doReturn(HttpStatus.SC_NOT_FOUND).when(statusLineMock).getStatusCode();
    doReturn(statusLineMock).when(closeableHttpResponseMock).getStatusLine();

    // mock entity
    HttpEntity httpEntityMock = mock(HttpEntity.class);
    doReturn(httpEntityMock).when(closeableHttpResponseMock).getEntity();

    doReturn(closeableHttpResponseMock).when(httpClient).execute(any(HttpGet.class));
    doReturn(closeableHttpResponseMock).when(httpClient).execute(any(HttpPost.class));
    doReturn(closeableHttpResponseMock)
        .when(httpClient)
        .execute(any(HttpPost.class), any(HttpClientContext.class));

    hopServer = spy(new HopServer());
    variables = new Variables();
    doReturn(httpClient).when(hopServer).getHttpClient();
    doReturn("response_body").when(hopServer).getResponseBodyAsString(any(InputStream.class));
  }

  private HttpResponse mockResponse(int statusCode, String entityText) throws IOException {
    HttpResponse resp = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);
    when(status.getStatusCode()).thenReturn(statusCode);
    when(resp.getStatusLine()).thenReturn(status);
    HttpEntity entity = mock(HttpEntity.class);
    when(entity.getContent())
        .thenReturn(new ByteArrayInputStream(entityText.getBytes(StandardCharsets.UTF_8)));
    when(resp.getEntity()).thenReturn(entity);
    return resp;
  }

  @Test(expected = HopException.class)
  public void testExecService() throws Exception {
    HttpGet httpGetMock = mock(HttpGet.class);
    URI uriMock = new URI("fake");
    doReturn(uriMock).when(httpGetMock).getURI();
    doReturn(httpGetMock)
        .when(hopServer)
        .buildExecuteServiceMethod(
            any(IVariables.class), anyString(), anyMapOf(String.class, String.class));
    hopServer.setHostname("hostNameStub");
    hopServer.setUsername("userNAmeStub");
    hopServer.execService( Variables.getADefaultVariableSpace(), "wrong_app_name");
    fail("Incorrect connection details had been used, but no exception was thrown");
  }

  @Test(expected = HopException.class)
  public void testSendXml() throws Exception {
    hopServer.setHostname("hostNameStub");
    hopServer.setUsername("userNAmeStub");
    HttpPost httpPostMock = mock(HttpPost.class);
    URI uriMock = new URI("fake");
    doReturn(uriMock).when(httpPostMock).getURI();
    doReturn(httpPostMock).when(hopServer).buildSendXmlMethod(any(Variables.class), any(byte[].class), anyString());
    hopServer.sendXml(variables, "", "");
    fail("Incorrect connection details had been used, but no exception was thrown");
  }

  @Test(expected = HopException.class)
  public void testSendExport() throws Exception {
    hopServer.setHostname("hostNameStub");
    hopServer.setUsername("userNAmeStub");
    HttpPost httpPostMock = mock(HttpPost.class);
    URI uriMock = new URI("fake");
    doReturn(uriMock).when(httpPostMock).getURI();
    doReturn(httpPostMock)
        .when(hopServer)
        .buildSendExportMethod(any(Variables.class), anyString(), anyString(), any(InputStream.class));
    File tempFile;
    tempFile = File.createTempFile("PDI-", "tmp");
    tempFile.deleteOnExit();
    hopServer.sendExport(variables, tempFile.getAbsolutePath(), "", "");
    fail("Incorrect connection details had been used, but no exception was thrown");
  }

  @Test
  public void testSendExportOk() throws Exception {
    hopServer.setUsername("uname");
    hopServer.setPassword("passw");
    hopServer.setHostname("hname");
    hopServer.setPort("1111");
    HttpPost httpPostMock = mock(HttpPost.class);
    URI uriMock = new URI("fake");
    final String responseContent = "baah";
    when(httpPostMock.getURI()).thenReturn(uriMock);
    doReturn(uriMock).when(httpPostMock).getURI();

    HttpClient client = mock(HttpClient.class);
    when(client.execute(any(), any(HttpContext.class)))
        .then(
            (Answer<HttpResponse>)
                invocation -> {
                  HttpClientContext context = (HttpClientContext) invocation.getArguments()[1];
                  Credentials cred =
                      context.getCredentialsProvider().getCredentials(new AuthScope("hname", 1111));
                  assertEquals("uname", cred.getUserPrincipal().getName());
                  return mockResponse(200, responseContent);
                });
    // override init
    when(hopServer.getHttpClient()).thenReturn(client);
    when(hopServer.getResponseBodyAsString(any())).thenCallRealMethod();

    doReturn(httpPostMock)
        .when(hopServer)
        .buildSendExportMethod(any(Variables.class), anyString(), anyString(), any(InputStream.class));
    File tempFile;
    tempFile = File.createTempFile("PDI-", "tmp");
    tempFile.deleteOnExit();
    String result = hopServer.sendExport(variables, tempFile.getAbsolutePath(), null, null);
    assertEquals(responseContent, result);
  }

  @Test
  public void testAddCredentials() throws IOException, ClassNotFoundException {
    String testUser = "test_username";
    hopServer.setUsername(testUser);
    String testPassword = "test_password";
    hopServer.setPassword(testPassword);
    String host = "somehost";
    hopServer.setHostname(host);
    int port = 1000;
    hopServer.setPort("" + port);

    HttpClientContext auth = hopServer.getAuthContext(variables);
    Credentials cred = auth.getCredentialsProvider().getCredentials(new AuthScope(host, port));
    assertEquals(testUser, cred.getUserPrincipal().getName());
    assertEquals(testPassword, cred.getPassword());

    String user2 = "user2";
    hopServer.setUsername(user2);
    hopServer.setPassword("pass2");
    auth = hopServer.getAuthContext(variables);
    cred = auth.getCredentialsProvider().getCredentials(new AuthScope(host, port));
    assertEquals(user2, cred.getUserPrincipal().getName());
  }

  @Test
  public void testAuthCredentialsSchemeWithSSL() {
    hopServer.setUsername("admin");
    hopServer.setPassword("password");
    hopServer.setHostname("localhost");
    hopServer.setPort("8443");
    hopServer.setSslMode(true);

    AuthCache cache = hopServer.getAuthContext(variables).getAuthCache();
    assertNotNull(cache.get(new HttpHost("localhost", 8443, "https")));
    assertNull(cache.get(new HttpHost("localhost", 8443, "http")));
  }

  @Test
  public void testAuthCredentialsSchemeWithoutSSL() {
    hopServer.setUsername("admin");
    hopServer.setPassword("password");
    hopServer.setHostname("localhost");
    hopServer.setPort("8080");
    hopServer.setSslMode(false);

    AuthCache cache = hopServer.getAuthContext(variables).getAuthCache();
    assertNull(cache.get(new HttpHost("localhost", 8080, "https")));
    assertNotNull(cache.get(new HttpHost("localhost", 8080, "http")));
  }

  @Test
  public void testModifyingName() {
    hopServer.setName("test");
    List<HopServer> list = new ArrayList<>();
    list.add(hopServer);

    HopServer hopServer2 = spy(new HopServer());
    hopServer2.setName("test");

    hopServer2.verifyAndModifyHopServerName(list, null);

    assertTrue(!hopServer.getName().equals(hopServer2.getName()));
  }

  @Test
  public void testEqualsHashCodeConsistency() throws Exception {
    HopServer server = new HopServer();
    server.setName("server");
    TestUtils.checkEqualsHashCodeConsistency(server, server);

    HopServer serverSame = new HopServer();
    serverSame.setName("server");
    assertTrue(server.equals(serverSame));
    TestUtils.checkEqualsHashCodeConsistency(server, serverSame);

    HopServer serverCaps = new HopServer();
    serverCaps.setName("SERVER");
    TestUtils.checkEqualsHashCodeConsistency(server, serverCaps);

    HopServer serverOther = new HopServer();
    serverOther.setName("something else");
    TestUtils.checkEqualsHashCodeConsistency(server, serverOther);
  }
}
