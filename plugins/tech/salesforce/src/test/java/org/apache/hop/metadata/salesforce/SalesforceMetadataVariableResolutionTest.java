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

/** Unit tests for variable resolution in Salesforce metadata connections */
class SalesforceMetadataVariableResolutionTest {

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
    Mockito.when(mockLog.isDetailed()).thenReturn(false);

    variables = new Variables();
    metadataConnection = new SalesforceConnection("TestConnection");
  }

  @Test
  void testVariableResolution_usernamePassword() throws Exception {
    variables.setVariable("SF_USER", "test.user@example.com");
    variables.setVariable("SF_PASS", "myPassword");
    variables.setVariable("SF_TOKEN", "securityToken123");
    variables.setVariable("SF_URL", "https://login.salesforce.com/services/Soap/u/64.0");

    metadataConnection.setAuthenticationType("USERNAME_PASSWORD");
    metadataConnection.setUsername("${SF_USER}");
    metadataConnection.setPassword("${SF_PASS}");
    metadataConnection.setSecurityToken("${SF_TOKEN}");
    metadataConnection.setTargetUrl("${SF_URL}");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertEquals("test.user@example.com", conn.getUsername());
    assertEquals("myPasswordsecurityToken123", conn.getPassword());
    assertEquals("https://login.salesforce.com/services/Soap/u/64.0", conn.getUrl());
  }

  @Test
  void testVariableResolution_oauth() throws Exception {
    variables.setVariable("OAUTH_CLIENT_ID", "3MVG9TestClientId");
    variables.setVariable("OAUTH_CLIENT_SECRET", "clientSecretValue");
    variables.setVariable("OAUTH_ACCESS_TOKEN", "00D5g000001JvToken");
    variables.setVariable("OAUTH_REFRESH_TOKEN", "5Aep861TSESvWeug");
    variables.setVariable("OAUTH_INSTANCE_URL", "https://na123.salesforce.com");

    metadataConnection.setAuthenticationType("OAUTH");
    metadataConnection.setOauthClientId("${OAUTH_CLIENT_ID}");
    metadataConnection.setOauthClientSecret("${OAUTH_CLIENT_SECRET}");
    metadataConnection.setOauthAccessToken("${OAUTH_ACCESS_TOKEN}");
    metadataConnection.setOauthRefreshToken("${OAUTH_REFRESH_TOKEN}");
    metadataConnection.setOauthInstanceUrl("${OAUTH_INSTANCE_URL}");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertEquals("3MVG9TestClientId", conn.getOauthClientId());
    assertEquals("clientSecretValue", conn.getOauthClientSecret());
    assertEquals("00D5g000001JvToken", conn.getOauthAccessToken());
    assertEquals("5Aep861TSESvWeug", conn.getOauthRefreshToken());
    assertEquals("https://na123.salesforce.com", conn.getOauthInstanceUrl());
  }

  @Test
  void testVariableResolution_oauthJwt() throws Exception {
    java.security.KeyPair keyPair = SalesforceTestUtils.generateTestKeyPair();
    String privateKeyPem = SalesforceTestUtils.privateKeyToPem(keyPair.getPrivate());

    variables.setVariable("JWT_USER", "jwt.user@example.com");
    variables.setVariable("JWT_CONSUMER_KEY", "3MVG9JwtConsumerKey");
    variables.setVariable("JWT_PRIVATE_KEY", privateKeyPem);
    variables.setVariable("JWT_ENDPOINT", "https://test.salesforce.com");

    metadataConnection.setAuthenticationType("OAUTH_JWT");
    metadataConnection.setOauthJwtUsername("${JWT_USER}");
    metadataConnection.setOauthJwtConsumerKey("${JWT_CONSUMER_KEY}");
    metadataConnection.setOauthJwtPrivateKey("${JWT_PRIVATE_KEY}");
    metadataConnection.setOauthJwtTokenEndpoint("${JWT_ENDPOINT}");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertEquals("jwt.user@example.com", conn.getUsername());
    assertEquals("3MVG9JwtConsumerKey", conn.getOauthClientId());
  }

  @Test
  void testVariableResolution_concatenation() throws Exception {
    // Test variable concatenation in values
    variables.setVariable("USER_BASE", "admin");
    variables.setVariable("DOMAIN", "example.com");

    metadataConnection.setAuthenticationType("USERNAME_PASSWORD");
    metadataConnection.setUsername("${USER_BASE}@${DOMAIN}");
    metadataConnection.setPassword("password");
    metadataConnection.setTargetUrl("https://login.salesforce.com/services/Soap/u/64.0");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertEquals("admin@example.com", conn.getUsername());
  }

  @Test
  void testPasswordEncryption_encrypted() throws Exception {
    String rawPassword = "mySecretPassword123";
    String encrypted = Encr.encryptPasswordIfNotUsingVariables(rawPassword);

    metadataConnection.setAuthenticationType("USERNAME_PASSWORD");
    metadataConnection.setUsername("user");
    metadataConnection.setPassword(encrypted);
    metadataConnection.setTargetUrl("https://login.salesforce.com/services/Soap/u/64.0");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    // Should decrypt automatically
    assertEquals(rawPassword, conn.getPassword());
  }

  @Test
  void testPasswordEncryption_plainText() throws Exception {
    metadataConnection.setAuthenticationType("USERNAME_PASSWORD");
    metadataConnection.setUsername("user");
    metadataConnection.setPassword("plainPassword");
    metadataConnection.setTargetUrl("https://login.salesforce.com/services/Soap/u/64.0");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertEquals("plainPassword", conn.getPassword());
  }

  @Test
  void testPasswordEncryption_withVariable() throws Exception {
    variables.setVariable("SECRET_PASS", "variablePassword");

    metadataConnection.setAuthenticationType("USERNAME_PASSWORD");
    metadataConnection.setUsername("user");
    metadataConnection.setPassword("${SECRET_PASS}");
    metadataConnection.setTargetUrl("https://login.salesforce.com/services/Soap/u/64.0");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertEquals("variablePassword", conn.getPassword());
  }

  @Test
  void testSecurityTokenConcatenation() throws Exception {
    metadataConnection.setAuthenticationType("USERNAME_PASSWORD");
    metadataConnection.setUsername("user");
    metadataConnection.setPassword("password");
    metadataConnection.setSecurityToken("TOKEN123");
    metadataConnection.setTargetUrl("https://login.salesforce.com/services/Soap/u/64.0");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    // Password should have token appended
    assertEquals("passwordTOKEN123", conn.getPassword());
  }

  @Test
  void testSecurityTokenConcatenation_emptyToken() throws Exception {
    metadataConnection.setAuthenticationType("USERNAME_PASSWORD");
    metadataConnection.setUsername("user");
    metadataConnection.setPassword("password");
    metadataConnection.setSecurityToken("");
    metadataConnection.setTargetUrl("https://login.salesforce.com/services/Soap/u/64.0");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    // Password should remain unchanged
    assertEquals("password", conn.getPassword());
  }

  @Test
  void testSecurityTokenConcatenation_nullToken() throws Exception {
    metadataConnection.setAuthenticationType("USERNAME_PASSWORD");
    metadataConnection.setUsername("user");
    metadataConnection.setPassword("password");
    metadataConnection.setSecurityToken(null);
    metadataConnection.setTargetUrl("https://login.salesforce.com/services/Soap/u/64.0");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertEquals("password", conn.getPassword());
  }

  @Test
  void testOAuthRefreshToken_optional() throws Exception {
    metadataConnection.setAuthenticationType("OAUTH");
    metadataConnection.setOauthClientId("clientId");
    metadataConnection.setOauthClientSecret("secret");
    metadataConnection.setOauthAccessToken("accessToken");
    metadataConnection.setOauthInstanceUrl("https://na123.salesforce.com");
    // No refresh token set

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertNotNull(conn);
    // Refresh token should be null/empty
    assertTrue(conn.getOauthRefreshToken() == null || conn.getOauthRefreshToken().isEmpty());
  }

  @Test
  void testOAuthRefreshToken_provided() throws Exception {
    metadataConnection.setAuthenticationType("OAUTH");
    metadataConnection.setOauthClientId("clientId");
    metadataConnection.setOauthClientSecret("secret");
    metadataConnection.setOauthAccessToken("accessToken");
    metadataConnection.setOauthRefreshToken("refreshToken123");
    metadataConnection.setOauthInstanceUrl("https://na123.salesforce.com");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertNotNull(conn);
    assertEquals("refreshToken123", conn.getOauthRefreshToken());
  }

  @Test
  void testEnvironmentSpecificConfiguration() throws Exception {
    // Simulate environment-specific configuration using direct variable
    variables.setVariable("ENVIRONMENT", "production");
    variables.setVariable("SF_URL", "https://login.salesforce.com/services/Soap/u/64.0");

    metadataConnection.setAuthenticationType("USERNAME_PASSWORD");
    metadataConnection.setUsername("user");
    metadataConnection.setPassword("pass");
    metadataConnection.setTargetUrl("${SF_URL}");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertEquals("https://login.salesforce.com/services/Soap/u/64.0", conn.getUrl());
  }

  @Test
  void testMixedVariablesAndLiterals() throws Exception {
    variables.setVariable("DOMAIN", "example.com");

    metadataConnection.setAuthenticationType("USERNAME_PASSWORD");
    metadataConnection.setUsername("admin@${DOMAIN}");
    metadataConnection.setPassword("password");
    metadataConnection.setTargetUrl("https://login.salesforce.com/services/Soap/u/64.0");

    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection conn =
        metadataConnection.createConnection(variables, mockLog);

    assertEquals("admin@example.com", conn.getUsername());
  }
}
