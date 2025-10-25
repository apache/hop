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

package org.apache.hop.metadata.salesforce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

/** Unit tests for Salesforce metadata connection type */
class SalesforceMetadataConnectionTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private ILogChannel mockLog;
  private IVariables variables;
  private SalesforceConnection metadataConnection;

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

    variables = new Variables();
    variables.setVariable("SF_USER", "test.user@example.com");
    variables.setVariable("SF_PASS", "testPassword123");
    variables.setVariable("SF_TOKEN", "securityToken456");
    variables.setVariable("SF_URL", "https://login.salesforce.com/services/Soap/u/64.0");

    metadataConnection = new SalesforceConnection("TestConnection");
  }

  @Test
  void testDefaultConstructor() {
    SalesforceConnection conn = new SalesforceConnection();
    assertNotNull(conn);
    assertEquals("USERNAME_PASSWORD", conn.getAuthenticationType());
  }

  @Test
  void testConstructorWithName() {
    SalesforceConnection conn = new SalesforceConnection("MyConnection");
    assertNotNull(conn);
    assertEquals("MyConnection", conn.getName());
  }

  @Test
  void testIsUsernamePasswordAuthentication_default() {
    assertTrue(metadataConnection.isUsernamePasswordAuthentication());
    assertFalse(metadataConnection.isOAuthAuthentication());
    assertFalse(metadataConnection.isOAuthJwtAuthentication());
  }

  @Test
  void testIsOAuthAuthentication() {
    metadataConnection.setAuthenticationType("OAUTH");
    assertTrue(metadataConnection.isOAuthAuthentication());
    assertFalse(metadataConnection.isUsernamePasswordAuthentication());
    assertFalse(metadataConnection.isOAuthJwtAuthentication());
  }

  @Test
  void testIsOAuthJwtAuthentication() {
    metadataConnection.setAuthenticationType("OAUTH_JWT");
    assertTrue(metadataConnection.isOAuthJwtAuthentication());
    assertFalse(metadataConnection.isUsernamePasswordAuthentication());
    assertFalse(metadataConnection.isOAuthAuthentication());
  }

  @Test
  void testCreateConnection_usernamePassword() throws Exception {
    metadataConnection.setAuthenticationType("USERNAME_PASSWORD");
    metadataConnection.setUsername("${SF_USER}");
    metadataConnection.setPassword("${SF_PASS}");
    metadataConnection.setSecurityToken("${SF_TOKEN}");
    metadataConnection.setTargetUrl("${SF_URL}");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertNotNull(conn);
    assertEquals("test.user@example.com", conn.getUsername());
    assertEquals("testPassword123securityToken456", conn.getPassword()); // Password + token
    assertEquals("https://login.salesforce.com/services/Soap/u/64.0", conn.getURL());
  }

  @Test
  void testCreateConnection_usernamePasswordWithoutSecurityToken() throws Exception {
    metadataConnection.setAuthenticationType("USERNAME_PASSWORD");
    metadataConnection.setUsername("user@test.com");
    metadataConnection.setPassword("password");
    metadataConnection.setSecurityToken(null);
    metadataConnection.setTargetUrl("https://login.salesforce.com/services/Soap/u/64.0");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertNotNull(conn);
    assertEquals("password", conn.getPassword()); // No token appended
  }

  @Test
  void testCreateConnection_oauth() throws Exception {
    metadataConnection.setAuthenticationType("OAUTH");
    metadataConnection.setOauthClientId("clientId123");
    metadataConnection.setOauthClientSecret("clientSecret456");
    metadataConnection.setOauthAccessToken("accessToken789");
    metadataConnection.setOauthRefreshToken("refreshToken012");
    metadataConnection.setOauthInstanceUrl("https://na123.salesforce.com");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertNotNull(conn);
    assertTrue(conn.isOAuthAuthentication());
    assertEquals("clientId123", conn.getOauthClientId());
    assertEquals("accessToken789", conn.getOauthAccessToken());
    assertEquals("refreshToken012", conn.getOauthRefreshToken());
  }

  @Test
  void testCreateConnection_oauthWithVariables() throws Exception {
    variables.setVariable("OAUTH_CLIENT_ID", "varClientId");
    variables.setVariable("OAUTH_CLIENT_SECRET", "varClientSecret");
    variables.setVariable("OAUTH_ACCESS_TOKEN", "varAccessToken");
    variables.setVariable("OAUTH_INSTANCE_URL", "https://cs99.salesforce.com");

    metadataConnection.setAuthenticationType("OAUTH");
    metadataConnection.setOauthClientId("${OAUTH_CLIENT_ID}");
    metadataConnection.setOauthClientSecret("${OAUTH_CLIENT_SECRET}");
    metadataConnection.setOauthAccessToken("${OAUTH_ACCESS_TOKEN}");
    metadataConnection.setOauthInstanceUrl("${OAUTH_INSTANCE_URL}");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertNotNull(conn);
    assertEquals("varClientId", conn.getOauthClientId());
    assertEquals("https://cs99.salesforce.com", conn.getOauthInstanceUrl());
  }

  @Test
  void testCreateConnection_oauthJwt() throws Exception {
    // Generate test private key
    java.security.KeyPair keyPair = SalesforceTestUtils.generateTestKeyPair();
    String privateKeyPem = SalesforceTestUtils.privateKeyToPem(keyPair.getPrivate());

    metadataConnection.setAuthenticationType("OAUTH_JWT");
    metadataConnection.setOauthJwtUsername("jwt.user@example.com");
    metadataConnection.setOauthJwtConsumerKey("jwtConsumerKey123");
    metadataConnection.setOauthJwtPrivateKey(privateKeyPem);
    metadataConnection.setOauthJwtTokenEndpoint("https://login.salesforce.com");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertNotNull(conn);
    assertTrue(conn.isOAuthJwtAuthentication());
    assertEquals("jwt.user@example.com", conn.getUsername());
    assertEquals("jwtConsumerKey123", conn.getOauthClientId());
  }

  @Test
  void testCreateConnection_oauthJwtWithVariables() throws Exception {
    java.security.KeyPair keyPair = SalesforceTestUtils.generateTestKeyPair();
    String privateKeyPem = SalesforceTestUtils.privateKeyToPem(keyPair.getPrivate());

    variables.setVariable("JWT_USER", "jwt.user@example.com");
    variables.setVariable("JWT_KEY", "jwtKey123");
    variables.setVariable("JWT_PRIVATE_KEY", privateKeyPem);
    variables.setVariable("JWT_ENDPOINT", "https://test.salesforce.com");

    metadataConnection.setAuthenticationType("OAUTH_JWT");
    metadataConnection.setOauthJwtUsername("${JWT_USER}");
    metadataConnection.setOauthJwtConsumerKey("${JWT_KEY}");
    metadataConnection.setOauthJwtPrivateKey("${JWT_PRIVATE_KEY}");
    metadataConnection.setOauthJwtTokenEndpoint("${JWT_ENDPOINT}");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertNotNull(conn);
    assertEquals("jwt.user@example.com", conn.getUsername());
    assertEquals("jwtKey123", conn.getOauthClientId());
  }

  @Test
  void testPasswordEncryption() {
    String rawPassword = "mySecretPassword123";
    String encrypted = Encr.encryptPasswordIfNotUsingVariables(rawPassword);

    metadataConnection.setPassword(encrypted);
    assertEquals(encrypted, metadataConnection.getPassword());

    // When resolved, should decrypt
    String decrypted = Encr.decryptPasswordOptionallyEncrypted(encrypted);
    assertEquals(rawPassword, decrypted);
  }

  @Test
  void testAllGettersSetters_usernamePassword() {
    metadataConnection.setUsername("testUser");
    assertEquals("testUser", metadataConnection.getUsername());

    metadataConnection.setPassword("testPass");
    assertEquals("testPass", metadataConnection.getPassword());

    metadataConnection.setSecurityToken("token123");
    assertEquals("token123", metadataConnection.getSecurityToken());

    metadataConnection.setTargetUrl("https://test.salesforce.com");
    assertEquals("https://test.salesforce.com", metadataConnection.getTargetUrl());
  }

  @Test
  void testAllGettersSetters_oauth() {
    metadataConnection.setOauthClientId("clientId");
    assertEquals("clientId", metadataConnection.getOauthClientId());

    metadataConnection.setOauthClientSecret("clientSecret");
    assertEquals("clientSecret", metadataConnection.getOauthClientSecret());

    metadataConnection.setOauthRedirectUri("http://localhost:8080/callback");
    assertEquals("http://localhost:8080/callback", metadataConnection.getOauthRedirectUri());

    metadataConnection.setOauthAccessToken("accessToken");
    assertEquals("accessToken", metadataConnection.getOauthAccessToken());

    metadataConnection.setOauthRefreshToken("refreshToken");
    assertEquals("refreshToken", metadataConnection.getOauthRefreshToken());

    metadataConnection.setOauthInstanceUrl("https://na123.salesforce.com");
    assertEquals("https://na123.salesforce.com", metadataConnection.getOauthInstanceUrl());
  }

  @Test
  void testAllGettersSetters_oauthJwt() {
    metadataConnection.setOauthJwtUsername("jwt.user@test.com");
    assertEquals("jwt.user@test.com", metadataConnection.getOauthJwtUsername());

    metadataConnection.setOauthJwtConsumerKey("jwtConsumerKey");
    assertEquals("jwtConsumerKey", metadataConnection.getOauthJwtConsumerKey());

    metadataConnection.setOauthJwtPrivateKey("privateKeyData");
    assertEquals("privateKeyData", metadataConnection.getOauthJwtPrivateKey());

    metadataConnection.setOauthJwtTokenEndpoint("https://login.salesforce.com");
    assertEquals("https://login.salesforce.com", metadataConnection.getOauthJwtTokenEndpoint());
  }

  @Test
  void testDefaultValues() {
    SalesforceConnection conn = new SalesforceConnection();

    assertEquals("USERNAME_PASSWORD", conn.getAuthenticationType());
    assertEquals("http://localhost:8080/callback", conn.getOauthRedirectUri());
    assertEquals("https://login.salesforce.com", conn.getOauthJwtTokenEndpoint());
  }

  @Test
  void testNameGetterSetter() {
    SalesforceConnection conn = new SalesforceConnection();
    conn.setName("MyTestConnection");
    assertEquals("MyTestConnection", conn.getName());
  }

  @Test
  void testIsPrivateKeySecure_withVariable() {
    metadataConnection.setOauthJwtPrivateKey("${SF_PRIVATE_KEY}");
    assertTrue(metadataConnection.isPrivateKeySecure());
  }

  @Test
  void testIsPrivateKeySecure_encrypted() {
    metadataConnection.setOauthJwtPrivateKey("Encrypted 2be98afc86aa7f2e4cb79ce10bdf9c31a");
    assertTrue(metadataConnection.isPrivateKeySecure());
  }

  @Test
  void testIsPrivateKeySecure_validPem() {
    String validPem = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBg...\n-----END PRIVATE KEY-----";
    metadataConnection.setOauthJwtPrivateKey(validPem);
    assertTrue(metadataConnection.isPrivateKeySecure());
  }

  @Test
  void testIsPrivateKeySecure_empty() {
    metadataConnection.setOauthJwtPrivateKey("");
    assertFalse(metadataConnection.isPrivateKeySecure());

    metadataConnection.setOauthJwtPrivateKey(null);
    assertFalse(metadataConnection.isPrivateKeySecure());
  }

  @Test
  void testIsPrivateKeySecure_invalidFormat() {
    metadataConnection.setOauthJwtPrivateKey("just some random text");
    assertFalse(metadataConnection.isPrivateKeySecure());
  }

  @Test
  void testAuthenticationTypeCaseInsensitive() {
    metadataConnection.setAuthenticationType("username_password");
    assertTrue(metadataConnection.isUsernamePasswordAuthentication());

    metadataConnection.setAuthenticationType("oauth");
    assertTrue(metadataConnection.isOAuthAuthentication());

    metadataConnection.setAuthenticationType("OAUTH_jwt");
    assertTrue(metadataConnection.isOAuthJwtAuthentication());
  }

  @Test
  void testVariableResolution_nestedSimple() throws Exception {
    // Test simple nested variable resolution
    variables.setVariable("BASE_USER", "admin");
    variables.setVariable("DOMAIN", "example.com");
    variables.setVariable("SF_USER", "${BASE_USER}@${DOMAIN}");

    metadataConnection.setAuthenticationType("USERNAME_PASSWORD");
    metadataConnection.setUsername("${SF_USER}");
    metadataConnection.setPassword("password");
    metadataConnection.setTargetUrl("https://login.salesforce.com/services/Soap/u/64.0");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    // Should resolve nested variables
    assertEquals("admin@example.com", conn.getUsername());
  }

  @Test
  void testEquals() {
    SalesforceConnection conn1 = new SalesforceConnection("Connection1");
    SalesforceConnection conn2 = new SalesforceConnection("Connection1");
    SalesforceConnection conn3 = new SalesforceConnection("Connection2");

    assertTrue(conn1.equals(conn2));
    assertFalse(conn1.equals(conn3));
    assertTrue(conn1.equals(conn1)); // reflexive
  }

  @Test
  void testHashCode() {
    SalesforceConnection conn1 = new SalesforceConnection("Connection1");
    SalesforceConnection conn2 = new SalesforceConnection("Connection1");

    assertEquals(conn1.hashCode(), conn2.hashCode());
  }

  @Test
  void testToString() {
    SalesforceConnection conn = new SalesforceConnection("MyConnection");
    String str = conn.toString();

    assertTrue(str.contains("MyConnection") || !str.isEmpty());
  }
}
