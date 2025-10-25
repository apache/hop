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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.lang.reflect.Method;
import java.security.KeyPair;
import java.util.Base64;
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

/** Unit tests for Salesforce OAuth JWT Bearer authentication */
@WireMockTest
class SalesforceConnectionJwtTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private ILogChannel mockLog;
  private String testUsername = "test.user@example.com";
  private String testConsumerKey = "3MVG9TestConsumerKey123";
  private String testPrivateKey;
  private String testTokenEndpoint;

  @BeforeAll
  static void setUpClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @BeforeEach
  void setUp(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
    mockLog = Mockito.mock(ILogChannel.class);
    Mockito.when(mockLog.isDetailed()).thenReturn(true);
    Mockito.when(mockLog.isDebug()).thenReturn(false);

    // Generate test key pair
    KeyPair keyPair = SalesforceTestUtils.generateTestKeyPair();
    testPrivateKey = SalesforceTestUtils.privateKeyToPem(keyPair.getPrivate());

    // Use WireMock server URL
    testTokenEndpoint = wmRuntimeInfo.getHttpBaseUrl();
  }

  @AfterEach
  void tearDown() {
    mockLog = null;
  }

  @Test
  void testCreateJwtConnection_validParameters() throws HopException {
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, testTokenEndpoint);

    assertNotNull(conn);
    assertTrue(conn.isOAuthJwtAuthentication());
    assertEquals(testUsername, conn.getUsername());
    assertEquals(testConsumerKey, conn.getOauthClientId());
  }

  @Test
  void testCreateJwtConnection_missingUsername() {
    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                SalesforceConnection.createJwtConnection(
                    mockLog, null, testConsumerKey, testPrivateKey, testTokenEndpoint));

    assertTrue(exception.getMessage().contains("username"));
  }

  @Test
  void testCreateJwtConnection_emptyUsername() {
    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                SalesforceConnection.createJwtConnection(
                    mockLog, "", testConsumerKey, testPrivateKey, testTokenEndpoint));

    assertTrue(exception.getMessage().contains("username"));
  }

  @Test
  void testCreateJwtConnection_missingConsumerKey() {
    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                SalesforceConnection.createJwtConnection(
                    mockLog, testUsername, null, testPrivateKey, testTokenEndpoint));

    assertTrue(exception.getMessage().contains("consumer key"));
  }

  @Test
  void testCreateJwtConnection_emptyConsumerKey() {
    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                SalesforceConnection.createJwtConnection(
                    mockLog, testUsername, "", testPrivateKey, testTokenEndpoint));

    assertTrue(exception.getMessage().contains("consumer key"));
  }

  @Test
  void testCreateJwtConnection_missingPrivateKey() {
    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                SalesforceConnection.createJwtConnection(
                    mockLog, testUsername, testConsumerKey, null, testTokenEndpoint));

    assertTrue(exception.getMessage().contains("private key"));
  }

  @Test
  void testCreateJwtConnection_emptyPrivateKey() {
    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                SalesforceConnection.createJwtConnection(
                    mockLog, testUsername, testConsumerKey, "", testTokenEndpoint));

    assertTrue(exception.getMessage().contains("private key"));
  }

  @Test
  void testBuildJwtAssertion_structure() throws Exception {
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, testTokenEndpoint);

    // Use reflection to call private buildJwtAssertion()
    Method method = SalesforceConnection.class.getDeclaredMethod("buildJwtAssertion");
    method.setAccessible(true);
    String jwt = (String) method.invoke(conn);

    assertNotNull(jwt);

    // JWT should have 3 parts: header.claims.signature
    String[] parts = jwt.split("\\.");
    assertEquals(3, parts.length, "JWT should have header, claims, and signature");

    // Decode header
    String headerJson = new String(Base64.getUrlDecoder().decode(parts[0]));
    assertTrue(headerJson.contains("\"alg\":\"RS256\""), "JWT should use RS256 algorithm");

    // Decode claims
    String claimsJson = new String(Base64.getUrlDecoder().decode(parts[1]));
    assertTrue(
        claimsJson.contains("\"iss\":\"" + testConsumerKey + "\""),
        "JWT should have correct issuer");
    assertTrue(
        claimsJson.contains("\"sub\":\"" + testUsername + "\""), "JWT should have correct subject");
    assertTrue(claimsJson.contains("\"exp\":"), "JWT should have expiration");
  }

  @Test
  void testBuildJwtAssertion_audienceClaim() throws Exception {
    // Test with full token endpoint URL
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog,
            testUsername,
            testConsumerKey,
            testPrivateKey,
            "https://login.salesforce.com/services/oauth2/token");

    Method method = SalesforceConnection.class.getDeclaredMethod("buildJwtAssertion");
    method.setAccessible(true);
    String jwt = (String) method.invoke(conn);

    String[] parts = jwt.split("\\.");
    String claimsJson = new String(Base64.getUrlDecoder().decode(parts[1]));

    // Audience should be base URL without /services/oauth2/token
    assertTrue(
        claimsJson.contains("\"aud\":\"https://login.salesforce.com\""),
        "Audience should be base URL without path");
  }

  @Test
  void testBuildJwtAssertion_audienceWithTrailingSlash() throws Exception {
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, "https://test.salesforce.com/");

    Method method = SalesforceConnection.class.getDeclaredMethod("buildJwtAssertion");
    method.setAccessible(true);
    String jwt = (String) method.invoke(conn);

    String[] parts = jwt.split("\\.");
    String claimsJson = new String(Base64.getUrlDecoder().decode(parts[1]));

    // Trailing slash should be removed
    assertTrue(
        claimsJson.contains("\"aud\":\"https://test.salesforce.com\""),
        "Audience should not have trailing slash");
  }

  @Test
  void testBuildJwtAssertion_expirationTime() throws Exception {
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, testTokenEndpoint);

    long beforeTime = System.currentTimeMillis() / 1000;

    Method method = SalesforceConnection.class.getDeclaredMethod("buildJwtAssertion");
    method.setAccessible(true);
    String jwt = (String) method.invoke(conn);

    long afterTime = System.currentTimeMillis() / 1000;

    String[] parts = jwt.split("\\.");
    String claimsJson = new String(Base64.getUrlDecoder().decode(parts[1]));

    // Extract exp claim (simple JSON parsing)
    int expStart = claimsJson.indexOf("\"exp\":") + 6;
    int expEnd = claimsJson.indexOf("}", expStart);
    long exp = Long.parseLong(claimsJson.substring(expStart, expEnd));

    // Expiration should be 5 minutes (300 seconds) from now
    assertTrue(
        exp >= beforeTime + 299 && exp <= afterTime + 301, "JWT should expire in ~5 minutes");
  }

  @Test
  void testSignWithRSA_validSignature() throws Exception {
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, testTokenEndpoint);

    String testData = "header.claims";

    Method method =
        SalesforceConnection.class.getDeclaredMethod("signWithRSA", String.class, String.class);
    method.setAccessible(true);
    String signature = (String) method.invoke(conn, testData, testPrivateKey);

    assertNotNull(signature);
    assertTrue(signature.length() > 0, "Signature should not be empty");

    // Signature should be base64url encoded
    // Should not contain +, /, or = characters
    assertTrue(!signature.contains("+") && !signature.contains("/") && !signature.contains("="));
  }

  @Test
  void testSignWithRSA_invalidPrivateKey() throws Exception {
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, testTokenEndpoint);

    String invalidKey = "-----BEGIN PRIVATE KEY-----\ninvalidbase64data\n-----END PRIVATE KEY-----";

    Method method =
        SalesforceConnection.class.getDeclaredMethod("signWithRSA", String.class, String.class);
    method.setAccessible(true);

    Exception exception =
        assertThrows(
            Exception.class,
            () -> {
              method.invoke(conn, "test", invalidKey);
            });

    // Should throw exception due to invalid key format
    assertNotNull(exception.getCause());
  }

  @Test
  void testBase64UrlEncode() throws Exception {
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, testTokenEndpoint);

    Method method = SalesforceConnection.class.getDeclaredMethod("base64UrlEncode", byte[].class);
    method.setAccessible(true);

    byte[] testData = "Hello World!".getBytes();
    String encoded = (String) method.invoke(conn, testData);

    assertNotNull(encoded);
    // Should be base64url (no padding)
    assertTrue(!encoded.contains("="));
    // Should be decodable
    String decoded = new String(Base64.getUrlDecoder().decode(encoded));
    assertEquals("Hello World!", decoded);
  }

  @Test
  void testGenerateJwtAndGetAccessToken_success(WireMockRuntimeInfo wmRuntimeInfo)
      throws Exception {
    // Mock successful token response
    wmRuntimeInfo
        .getWireMock()
        .register(
            post(urlEqualTo("/services/oauth2/token"))
                .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(
                            SalesforceTestUtils.buildOAuthTokenResponse(
                                "00D5g000001JvToken!Test", null, "https://na123.salesforce.com"))));

    String endpoint = wmRuntimeInfo.getHttpBaseUrl();
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, endpoint);

    // Call private method
    Method method = SalesforceConnection.class.getDeclaredMethod("generateJwtAndGetAccessToken");
    method.setAccessible(true);
    String accessToken = (String) method.invoke(conn);

    assertNotNull(accessToken);
    assertEquals("00D5g000001JvToken!Test", accessToken);

    // Verify instance URL was updated
    assertEquals("https://na123.salesforce.com", conn.getOauthInstanceUrl());

    // Verify request was made with JWT assertion
    wmRuntimeInfo
        .getWireMock()
        .verifyThat(
            postRequestedFor(urlEqualTo("/services/oauth2/token"))
                .withRequestBody(
                    containing("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"))
                .withRequestBody(containing("assertion=")));
  }

  @Test
  void testGenerateJwtAndGetAccessToken_tokenEndpointWithFullPath(WireMockRuntimeInfo wmRuntimeInfo)
      throws Exception {
    // Mock successful token response
    wmRuntimeInfo
        .getWireMock()
        .register(
            post(urlEqualTo("/services/oauth2/token"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withBody(
                            SalesforceTestUtils.buildOAuthTokenResponse(
                                "testToken", null, "https://na123.salesforce.com"))));

    // Token endpoint already has the path
    String endpointWithPath = wmRuntimeInfo.getHttpBaseUrl() + "/services/oauth2/token";
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, endpointWithPath);

    Method method = SalesforceConnection.class.getDeclaredMethod("generateJwtAndGetAccessToken");
    method.setAccessible(true);
    String accessToken = (String) method.invoke(conn);

    assertNotNull(accessToken);
    // Should not duplicate the path
  }

  @Test
  void testGenerateJwtAndGetAccessToken_failure(WireMockRuntimeInfo wmRuntimeInfo)
      throws Exception {
    // Mock failed token response
    wmRuntimeInfo
        .getWireMock()
        .register(
            post(urlEqualTo("/services/oauth2/token"))
                .willReturn(
                    aResponse()
                        .withStatus(400)
                        .withBody(
                            SalesforceTestUtils.buildOAuthErrorResponse(
                                "invalid_grant", "user hasn't approved this consumer"))));

    String endpoint = wmRuntimeInfo.getHttpBaseUrl();
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, endpoint);

    Method method = SalesforceConnection.class.getDeclaredMethod("generateJwtAndGetAccessToken");
    method.setAccessible(true);

    Exception exception =
        assertThrows(
            Exception.class,
            () -> {
              method.invoke(conn);
            });

    // Should contain error message from Salesforce
    assertTrue(
        exception.getCause().getMessage().contains("user hasn't approved")
            || exception.getCause().getMessage().contains("400"));
  }

  @Test
  void testIsOAuthJwtAuthentication_true() throws HopException {
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, testTokenEndpoint);

    assertTrue(conn.isOAuthJwtAuthentication());
    assertTrue(!conn.isOAuthAuthentication());
    assertTrue(!conn.isUsernamePasswordAuthentication());
  }

  @Test
  void testConnectWithOAuthJwt_success(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
    // Mock successful token response with instance URL
    wmRuntimeInfo
        .getWireMock()
        .register(
            post(urlEqualTo("/services/oauth2/token"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withBody(
                            SalesforceTestUtils.buildOAuthTokenResponse(
                                "00D5g000001JvToken!Test", null, "https://na123.salesforce.com"))));

    String endpoint = wmRuntimeInfo.getHttpBaseUrl();
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, endpoint);

    // Call private connectWithOAuthJwt() method
    Method method = SalesforceConnection.class.getDeclaredMethod("connectWithOAuthJwt");
    method.setAccessible(true);
    method.invoke(conn);

    // Verify access token was set
    assertEquals("00D5g000001JvToken!Test", conn.getOauthAccessToken());

    // Verify instance URL was set
    assertEquals("https://na123.salesforce.com", conn.getOauthInstanceUrl());

    // Verify binding was created
    assertNotNull(conn.getBinding());
  }

  @Test
  void testConnectWithOAuthJwt_missingInstanceUrl(WireMockRuntimeInfo wmRuntimeInfo)
      throws Exception {
    // Mock response without instance_url
    wmRuntimeInfo
        .getWireMock()
        .register(
            post(urlEqualTo("/services/oauth2/token"))
                .willReturn(
                    aResponse().withStatus(200).withBody("{\"access_token\":\"testToken\"}")));

    String endpoint = wmRuntimeInfo.getHttpBaseUrl();
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, endpoint);

    Method method = SalesforceConnection.class.getDeclaredMethod("connectWithOAuthJwt");
    method.setAccessible(true);

    Exception exception = assertThrows(Exception.class, () -> method.invoke(conn));

    // Should fail because instance URL is required
    assertTrue(
        exception.getCause().getMessage().contains("Instance URL")
            || exception.getCause().getMessage().contains("instance_url"));
  }

  @Test
  void testJwtSignatureConsistency() throws Exception {
    // Generate connection
    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, testTokenEndpoint);

    Method method = SalesforceConnection.class.getDeclaredMethod("buildJwtAssertion");
    method.setAccessible(true);

    String jwt1 = (String) method.invoke(conn);

    // Wait 1 second to ensure different timestamp
    Thread.sleep(1100);

    String jwt2 = (String) method.invoke(conn);

    // JWTs should be different due to different exp timestamps
    assertTrue(!jwt1.equals(jwt2), "JWTs with different timestamps should differ");

    // But they should have same algorithm and issuer
    String[] parts1 = jwt1.split("\\.");
    String[] parts2 = jwt2.split("\\.");

    // Same header
    assertEquals(parts1[0], parts2[0], "JWT headers should be identical");
  }

  @Test
  void testPrivateKeyParsing_withHeaders() throws Exception {
    String keyWithHeaders =
        "-----BEGIN PRIVATE KEY-----\n"
            + testPrivateKey.split("-----")[2]
            + "\n-----END PRIVATE KEY-----";

    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, keyWithHeaders, testTokenEndpoint);

    // Should successfully parse and sign
    Method method = SalesforceConnection.class.getDeclaredMethod("buildJwtAssertion");
    method.setAccessible(true);
    String jwt = (String) method.invoke(conn);

    assertNotNull(jwt);
    assertEquals(3, jwt.split("\\.").length);
  }

  @Test
  void testPrivateKeyParsing_withExtraWhitespace() throws Exception {
    String keyWithWhitespace = "\n\n   " + testPrivateKey + "   \n\n";

    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, keyWithWhitespace, testTokenEndpoint);

    // Should successfully parse by stripping whitespace
    Method method = SalesforceConnection.class.getDeclaredMethod("buildJwtAssertion");
    method.setAccessible(true);
    String jwt = (String) method.invoke(conn);

    assertNotNull(jwt);
  }

  @Test
  void testConnectMethod_routesToJwt() throws Exception {
    // This tests that connect() properly routes to connectWithOAuthJwt()
    // We can't fully test this without mocking PartnerConnection,
    // but we can verify the routing logic

    SalesforceConnection conn =
        SalesforceConnection.createJwtConnection(
            mockLog, testUsername, testConsumerKey, testPrivateKey, testTokenEndpoint);

    assertTrue(conn.isOAuthJwtAuthentication());

    // The connect() method should route to connectWithOAuthJwt() based on auth type
    // Full integration test would require mocking Salesforce SOAP API
  }
}
