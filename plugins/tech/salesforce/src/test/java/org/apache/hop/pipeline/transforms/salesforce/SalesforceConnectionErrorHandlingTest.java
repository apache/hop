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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.sforce.ws.ConnectionException;
import java.lang.reflect.Method;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit tests for Salesforce error handling and exception scenarios */
class SalesforceConnectionErrorHandlingTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private ILogChannel mockLog;
  private SalesforceConnection connection;

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
    mockLog = mock(ILogChannel.class);
    connection = new SalesforceConnection(mockLog, "https://test.com", "user", "pass");
  }

  @Test
  void testCheckForInvalidSessionId_directMessage() throws Exception {
    Exception testException = new Exception("INVALID_SESSION_ID: Your session has expired");

    Method method =
        SalesforceConnection.class.getDeclaredMethod("checkForInvalidSessionId", Exception.class);
    method.setAccessible(true);
    boolean result = (boolean) method.invoke(connection, testException);

    assertTrue(result, "Should detect INVALID_SESSION_ID in direct message");
  }

  @Test
  void testCheckForInvalidSessionId_alternateMessage() throws Exception {
    Exception testException = new Exception("Invalid Session ID found: please login again");

    Method method =
        SalesforceConnection.class.getDeclaredMethod("checkForInvalidSessionId", Exception.class);
    method.setAccessible(true);
    boolean result = (boolean) method.invoke(connection, testException);

    assertTrue(result, "Should detect 'Invalid Session ID' in message");
  }

  @Test
  void testCheckForInvalidSessionId_inCauseChain() throws Exception {
    Exception rootCause = new Exception("INVALID_SESSION_ID");
    Exception middleException = new Exception("Wrapper exception", rootCause);
    Exception topException = new Exception("Top level exception", middleException);

    Method method =
        SalesforceConnection.class.getDeclaredMethod("checkForInvalidSessionId", Exception.class);
    method.setAccessible(true);
    boolean result = (boolean) method.invoke(connection, topException);

    assertTrue(result, "Should detect INVALID_SESSION_ID in cause chain");
  }

  @Test
  void testCheckForInvalidSessionId_deepCauseChain() throws Exception {
    // Build a chain 10 levels deep
    Exception current = new Exception("INVALID_SESSION_ID: Session expired");
    for (int i = 0; i < 9; i++) {
      current = new Exception("Wrapper " + i, current);
    }

    Method method =
        SalesforceConnection.class.getDeclaredMethod("checkForInvalidSessionId", Exception.class);
    method.setAccessible(true);
    boolean result = (boolean) method.invoke(connection, current);

    assertTrue(result, "Should detect INVALID_SESSION_ID even in deep cause chain");
  }

  @Test
  void testCheckForInvalidSessionId_noInvalidSession() throws Exception {
    Exception testException = new Exception("Some other error occurred");

    Method method =
        SalesforceConnection.class.getDeclaredMethod("checkForInvalidSessionId", Exception.class);
    method.setAccessible(true);
    boolean result = (boolean) method.invoke(connection, testException);

    assertFalse(result, "Should not detect invalid session in unrelated exception");
  }

  @Test
  void testCheckForInvalidSessionId_nullException() throws Exception {
    Method method =
        SalesforceConnection.class.getDeclaredMethod("checkForInvalidSessionId", Exception.class);
    method.setAccessible(true);
    boolean result = (boolean) method.invoke(connection, (Exception) null);

    assertFalse(result, "Should handle null exception gracefully");
  }

  @Test
  void testCheckForInvalidSessionId_exceptionWithNullMessage() throws Exception {
    Exception testException = new Exception((String) null);

    Method method =
        SalesforceConnection.class.getDeclaredMethod("checkForInvalidSessionId", Exception.class);
    method.setAccessible(true);
    boolean result = (boolean) method.invoke(connection, testException);

    assertFalse(result, "Should handle null message gracefully");
  }

  @Test
  void testCheckForInvalidSessionId_connectionException() throws Exception {
    ConnectionException testException = new ConnectionException("INVALID_SESSION_ID");

    Method method =
        SalesforceConnection.class.getDeclaredMethod("checkForInvalidSessionId", Exception.class);
    method.setAccessible(true);
    boolean result = (boolean) method.invoke(connection, testException);

    assertTrue(result, "Should detect INVALID_SESSION_ID in ConnectionException");
  }

  @Test
  void testConstructor_missingUrl() {
    HopException exception =
        assertThrows(
            HopException.class, () -> new SalesforceConnection(mockLog, null, "user", "pass"));

    assertTrue(exception.getMessage().toLowerCase().contains("url"));
  }

  @Test
  void testConstructor_emptyUrl() {
    HopException exception =
        assertThrows(
            HopException.class, () -> new SalesforceConnection(mockLog, "", "user", "pass"));

    assertTrue(exception.getMessage().toLowerCase().contains("url"));
  }

  @Test
  void testConstructor_missingUsername() {
    HopException exception =
        assertThrows(
            HopException.class,
            () -> new SalesforceConnection(mockLog, "https://test.com", null, "pass"));

    assertTrue(exception.getMessage().toLowerCase().contains("username"));
  }

  @Test
  void testConstructor_emptyUsername() {
    HopException exception =
        assertThrows(
            HopException.class,
            () -> new SalesforceConnection(mockLog, "https://test.com", "", "pass"));

    assertTrue(exception.getMessage().toLowerCase().contains("username"));
  }

  @Test
  void testConstructor_nullPassword() throws HopException {
    // Password can be null/empty (for OAuth scenarios)
    SalesforceConnection conn = new SalesforceConnection(mockLog, "https://test.com", "user", null);

    assertTrue(conn.getPassword() == null || conn.getPassword().isEmpty());
  }

  @Test
  void testOAuthConstructor_missingClientId() {
    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                new SalesforceConnection(
                    mockLog, null, "secret", "accessToken", "https://instance.salesforce.com"));

    assertTrue(exception.getMessage().toLowerCase().contains("client"));
  }

  @Test
  void testOAuthConstructor_missingAccessToken() {
    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                new SalesforceConnection(
                    mockLog, "clientId", "secret", null, "https://instance.salesforce.com"));

    assertTrue(exception.getMessage().toLowerCase().contains("token"));
  }

  @Test
  void testJwtConstructor_missingUsername() throws Exception {
    java.security.KeyPair keyPair = SalesforceTestUtils.generateTestKeyPair();
    String privateKey = SalesforceTestUtils.privateKeyToPem(keyPair.getPrivate());

    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                SalesforceConnection.createJwtConnection(
                    mockLog, null, "consumerKey", privateKey, "https://login.salesforce.com"));

    assertTrue(exception.getMessage().toLowerCase().contains("username"));
  }

  @Test
  void testJwtConstructor_missingConsumerKey() throws Exception {
    java.security.KeyPair keyPair = SalesforceTestUtils.generateTestKeyPair();
    String privateKey = SalesforceTestUtils.privateKeyToPem(keyPair.getPrivate());

    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                SalesforceConnection.createJwtConnection(
                    mockLog, "user@test.com", null, privateKey, "https://login.salesforce.com"));

    assertTrue(exception.getMessage().toLowerCase().contains("consumer"));
  }

  @Test
  void testJwtConstructor_missingPrivateKey() {
    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                SalesforceConnection.createJwtConnection(
                    mockLog, "user@test.com", "consumerKey", null, "https://login.salesforce.com"));

    assertTrue(exception.getMessage().toLowerCase().contains("private key"));
  }

  @Test
  void testExtractJsonValue_malformedJson() throws Exception {
    Method method =
        SalesforceConnection.class.getDeclaredMethod(
            "extractJsonValue", String.class, String.class);
    method.setAccessible(true);

    // Malformed JSON
    String malformedJson = "{invalid json";
    String result = (String) method.invoke(connection, malformedJson, "key");

    // Should handle gracefully and return null/empty
    assertTrue(result == null || result.isEmpty());
  }

  @Test
  void testExtractJsonValue_emptyJson() throws Exception {
    Method method =
        SalesforceConnection.class.getDeclaredMethod(
            "extractJsonValue", String.class, String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(connection, "", "key");

    assertTrue(result == null || result.isEmpty());
  }

  @Test
  void testExtractJsonValue_nullJson() throws Exception {
    Method method =
        SalesforceConnection.class.getDeclaredMethod(
            "extractJsonValue", String.class, String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(connection, (String) null, "key");

    assertTrue(result == null || result.isEmpty());
  }
}
