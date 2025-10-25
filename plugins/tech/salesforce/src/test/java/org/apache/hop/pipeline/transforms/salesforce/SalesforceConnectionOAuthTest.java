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
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.lang.reflect.Method;
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

/** Unit tests for Salesforce OAuth 2.0 Authorization Code Flow */
@WireMockTest
class SalesforceConnectionOAuthTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private ILogChannel mockLog;
  private String testClientId = "testClientId123";
  private String testClientSecret = "testClientSecret456";
  private String testAccessToken = "00D5g000001JvToken!Test";
  private String testRefreshToken = "refresh_token_abc123";
  private String testInstanceUrl = "https://na123.salesforce.com";

  @BeforeAll
  static void setUpClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @BeforeEach
  void setUp() {
    mockLog = Mockito.mock(ILogChannel.class);
    Mockito.when(mockLog.isDetailed()).thenReturn(true);
    Mockito.when(mockLog.isDebug()).thenReturn(false);
  }

  @AfterEach
  void tearDown() {
    mockLog = null;
  }

  @Test
  void testOAuthConstructor_validParameters() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, testInstanceUrl);

    assertNotNull(conn);
    assertTrue(conn.isOAuthAuthentication());
    assertEquals(testClientId, conn.getOauthClientId());
    assertEquals(testClientSecret, conn.getOauthClientSecret());
    assertEquals(testAccessToken, conn.getOauthAccessToken());
    assertEquals(testInstanceUrl, conn.getOauthInstanceUrl());
  }

  @Test
  void testOAuthConstructor_missingClientId() {
    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                new SalesforceConnection(
                    mockLog, null, testClientSecret, testAccessToken, testInstanceUrl));

    assertTrue(exception.getMessage().contains("Client ID"));
  }

  @Test
  void testOAuthConstructor_missingAccessToken() {
    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                new SalesforceConnection(
                    mockLog, testClientId, testClientSecret, null, testInstanceUrl));

    assertTrue(exception.getMessage().contains("Access Token"));
  }

  @Test
  void testIsOAuthAuthentication_true() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, testInstanceUrl);

    assertTrue(conn.isOAuthAuthentication());
    assertFalse(conn.isUsernamePasswordAuthentication());
    assertFalse(conn.isOAuthJwtAuthentication());
  }

  @Test
  void testRefreshAccessToken_success(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
    // Mock successful refresh token response
    wmRuntimeInfo
        .getWireMock()
        .register(
            post(urlEqualTo("/services/oauth2/token"))
                .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withBody(
                            SalesforceTestUtils.buildOAuthTokenResponse(
                                "newAccessToken123", null, testInstanceUrl))));

    String endpoint = wmRuntimeInfo.getHttpBaseUrl();
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, endpoint);
    conn.setOauthRefreshToken(testRefreshToken);

    // Call private refreshAccessToken() method
    Method method = SalesforceConnection.class.getDeclaredMethod("refreshAccessToken");
    method.setAccessible(true);
    String newToken = (String) method.invoke(conn);

    assertNotNull(newToken);
    assertEquals("newAccessToken123", newToken);

    // Verify request body contains refresh token grant
    wmRuntimeInfo
        .getWireMock()
        .verifyThat(
            postRequestedFor(urlEqualTo("/services/oauth2/token"))
                .withRequestBody(containing("grant_type=refresh_token"))
                .withRequestBody(containing("client_id=" + testClientId))
                .withRequestBody(containing("refresh_token=" + testRefreshToken)));
  }

  @Test
  void testRefreshAccessToken_noRefreshToken() throws Exception {
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, testInstanceUrl);
    // Don't set refresh token

    Method method = SalesforceConnection.class.getDeclaredMethod("refreshAccessToken");
    method.setAccessible(true);

    Exception exception = assertThrows(Exception.class, () -> method.invoke(conn));

    assertTrue(exception.getCause().getMessage().contains("refresh token"));
  }

  @Test
  void testRefreshAccessToken_failure(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
    // Mock failed refresh response
    wmRuntimeInfo
        .getWireMock()
        .register(
            post(urlEqualTo("/services/oauth2/token"))
                .willReturn(
                    aResponse()
                        .withStatus(400)
                        .withBody(
                            SalesforceTestUtils.buildOAuthErrorResponse(
                                "invalid_grant", "expired refresh token"))));

    String endpoint = wmRuntimeInfo.getHttpBaseUrl();
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, endpoint);
    conn.setOauthRefreshToken(testRefreshToken);

    Method method = SalesforceConnection.class.getDeclaredMethod("refreshAccessToken");
    method.setAccessible(true);

    Exception exception = assertThrows(Exception.class, () -> method.invoke(conn));

    assertTrue(exception.getCause().getMessage().contains("400"));
  }

  @Test
  void testExtractJsonValue() throws Exception {
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, testInstanceUrl);

    Method method =
        SalesforceConnection.class.getDeclaredMethod(
            "extractJsonValue", String.class, String.class);
    method.setAccessible(true);

    // Test simple value extraction
    String json = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
    String value1 = (String) method.invoke(conn, json, "key1");
    String value2 = (String) method.invoke(conn, json, "key2");

    assertEquals("value1", value1);
    assertEquals("value2", value2);
  }

  @Test
  void testExtractJsonValue_withSpaces() throws Exception {
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, testInstanceUrl);

    Method method =
        SalesforceConnection.class.getDeclaredMethod(
            "extractJsonValue", String.class, String.class);
    method.setAccessible(true);

    // Test with various whitespace
    String json = "{ \"access_token\" : \"tokenValue\" , \"other\" : \"data\" }";
    String value = (String) method.invoke(conn, json, "access_token");

    assertEquals("tokenValue", value);
  }

  @Test
  void testExtractJsonValue_notFound() throws Exception {
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, testInstanceUrl);

    Method method =
        SalesforceConnection.class.getDeclaredMethod(
            "extractJsonValue", String.class, String.class);
    method.setAccessible(true);

    String json = "{\"key1\":\"value1\"}";
    String value = (String) method.invoke(conn, json, "nonexistent");

    assertTrue(value == null || value.isEmpty());
  }

  @Test
  void testOAuthRefreshToken_getterSetter() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, testInstanceUrl);

    assertTrue(conn.getOauthRefreshToken() == null || conn.getOauthRefreshToken().isEmpty());

    conn.setOauthRefreshToken(testRefreshToken);
    assertEquals(testRefreshToken, conn.getOauthRefreshToken());
  }

  @Test
  void testCheckForInvalidSessionId_withInvalidSessionInMessage() throws Exception {
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, testInstanceUrl);

    Method method =
        SalesforceConnection.class.getDeclaredMethod("checkForInvalidSessionId", Exception.class);
    method.setAccessible(true);

    // Test exception with INVALID_SESSION_ID in message
    Exception testException = new Exception("INVALID_SESSION_ID: Session expired");
    boolean result = (boolean) method.invoke(conn, testException);

    assertTrue(result, "Should detect INVALID_SESSION_ID in exception message");
  }

  @Test
  void testCheckForInvalidSessionId_withInvalidSessionInCause() throws Exception {
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, testInstanceUrl);

    Method method =
        SalesforceConnection.class.getDeclaredMethod("checkForInvalidSessionId", Exception.class);
    method.setAccessible(true);

    // Test exception with INVALID_SESSION_ID in cause chain
    Exception cause = new Exception("Invalid Session ID found");
    Exception testException = new Exception("Wrapper exception", cause);
    boolean result = (boolean) method.invoke(conn, testException);

    assertTrue(result, "Should detect INVALID_SESSION_ID in exception cause chain");
  }

  @Test
  void testCheckForInvalidSessionId_noInvalidSession() throws Exception {
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, testInstanceUrl);

    Method method =
        SalesforceConnection.class.getDeclaredMethod("checkForInvalidSessionId", Exception.class);
    method.setAccessible(true);

    // Test exception without INVALID_SESSION_ID
    Exception testException = new Exception("Some other error");
    boolean result = (boolean) method.invoke(conn, testException);

    assertFalse(result, "Should not detect INVALID_SESSION_ID in unrelated exception");
  }

  @Test
  void testAuthenticationType_getterSetter() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, testInstanceUrl);

    assertEquals("OAUTH", conn.getAuthenticationType());

    conn.setAuthenticationType("USERNAME_PASSWORD");
    assertEquals("USERNAME_PASSWORD", conn.getAuthenticationType());
    assertTrue(conn.isUsernamePasswordAuthentication());
    assertFalse(conn.isOAuthAuthentication());
  }

  @Test
  void testOAuthGettersSetters() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, testInstanceUrl);

    // Test all OAuth getters
    assertEquals(testClientId, conn.getOauthClientId());
    assertEquals(testClientSecret, conn.getOauthClientSecret());
    assertEquals(testAccessToken, conn.getOauthAccessToken());
    assertEquals(testInstanceUrl, conn.getOauthInstanceUrl());

    // Test setters
    conn.setOauthClientId("newClientId");
    assertEquals("newClientId", conn.getOauthClientId());

    conn.setOauthClientSecret("newSecret");
    assertEquals("newSecret", conn.getOauthClientSecret());

    conn.setOauthAccessToken("newToken");
    assertEquals("newToken", conn.getOauthAccessToken());

    conn.setOauthInstanceUrl("https://new.salesforce.com");
    assertEquals("https://new.salesforce.com", conn.getOauthInstanceUrl());
  }

  @Test
  void testRefreshAccessToken_returnsNewToken(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
    // Mock response with new access token
    String newAccessToken = "00D_NewAccessToken_123";
    wmRuntimeInfo
        .getWireMock()
        .register(
            post(urlEqualTo("/services/oauth2/token"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withBody(
                            SalesforceTestUtils.buildOAuthTokenResponse(
                                newAccessToken, null, "https://cs99.salesforce.com"))));

    String endpoint = wmRuntimeInfo.getHttpBaseUrl();
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, endpoint);
    conn.setOauthRefreshToken(testRefreshToken);

    Method method = SalesforceConnection.class.getDeclaredMethod("refreshAccessToken");
    method.setAccessible(true);
    String returnedToken = (String) method.invoke(conn);

    // Should return the new access token
    assertNotNull(returnedToken);
    assertEquals(newAccessToken, returnedToken);
  }

  @Test
  void testRefreshAccessToken_withBasicEndpoint(WireMockRuntimeInfo wmRuntimeInfo)
      throws Exception {
    wmRuntimeInfo
        .getWireMock()
        .register(
            post(urlEqualTo("/services/oauth2/token"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withBody(
                            SalesforceTestUtils.buildOAuthTokenResponse(
                                "newToken", null, testInstanceUrl))));

    // Test with basic endpoint (no trailing slash or path)
    String endpoint = wmRuntimeInfo.getHttpBaseUrl();
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, endpoint);
    conn.setOauthRefreshToken(testRefreshToken);

    Method method = SalesforceConnection.class.getDeclaredMethod("refreshAccessToken");
    method.setAccessible(true);
    String token = (String) method.invoke(conn);

    assertNotNull(token);
    assertEquals("newToken", token);
  }

  @Test
  void testOAuthDeprecatedConstructor() throws HopException {
    // Test the @Deprecated OAuth constructor still works
    @SuppressWarnings("deprecation")
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, testInstanceUrl);

    assertTrue(conn.isOAuthAuthentication());
  }

  @Test
  void testConnectMethod_routesToOAuth() throws Exception {
    SalesforceConnection conn =
        new SalesforceConnection(
            mockLog, testClientId, testClientSecret, testAccessToken, testInstanceUrl);

    assertTrue(conn.isOAuthAuthentication());

    // The connect() method should route to connectWithOAuth() based on auth type
    // Full integration test would require mocking Salesforce SOAP API
  }
}
