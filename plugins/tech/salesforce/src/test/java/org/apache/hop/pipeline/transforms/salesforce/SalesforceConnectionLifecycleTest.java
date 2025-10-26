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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.mockito.Mockito;

/** Unit tests for Salesforce connection lifecycle management */
class SalesforceConnectionLifecycleTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private ILogChannel mockLog;

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
    Mockito.when(mockLog.isDebug()).thenReturn(false);
  }

  @Test
  void testSetTimeOut() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(mockLog, "https://login.salesforce.com", "user", "pass");

    conn.setTimeOut(30000);
    assertEquals(30000, conn.getTimeOut());

    conn.setTimeOut(0);
    assertEquals(0, conn.getTimeOut());
  }

  @Test
  void testCompressionSetting() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(mockLog, "https://login.salesforce.com", "user", "pass");

    assertFalse(conn.isUsingCompression()); // Default is false

    conn.setUsingCompression(true);
    assertTrue(conn.isUsingCompression());

    conn.setUsingCompression(false);
    assertFalse(conn.isUsingCompression());
  }

  @Test
  void testRollbackAllChangesOnError() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(mockLog, "https://login.salesforce.com", "user", "pass");

    assertFalse(conn.isRollbackAllChangesOnError()); // Default is false

    conn.setRollbackAllChangesOnError(true);
    assertTrue(conn.isRollbackAllChangesOnError());

    conn.setRollbackAllChangesOnError(false);
    assertFalse(conn.isRollbackAllChangesOnError());
  }

  @Test
  void testQueryAll() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(mockLog, "https://login.salesforce.com", "user", "pass");

    assertFalse(conn.isQueryAll()); // Default is false

    conn.setQueryAll(true);
    assertTrue(conn.isQueryAll());

    conn.setQueryAll(false);
    assertFalse(conn.isQueryAll());
  }

  @Test
  void testModuleSetting() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(mockLog, "https://login.salesforce.com", "user", "pass");

    conn.setModule("Account");
    assertEquals("Account", conn.getModule());

    conn.setModule("Contact");
    assertEquals("Contact", conn.getModule());
  }

  @Test
  void testSqlSetting() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(mockLog, "https://login.salesforce.com", "user", "pass");

    String sql = "SELECT Id, Name FROM Account WHERE Active__c = true";
    conn.setSql(sql);
    assertEquals(sql, conn.getSql());
  }

  @Test
  void testFieldsListSetting() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(mockLog, "https://login.salesforce.com", "user", "pass");

    String fields = "Id,Name,Email,Phone";
    conn.setFieldsList(fields);

    // There's no getter, but we can verify it doesn't throw
    assertNotNull(conn);
  }

  @Test
  void testServerTimestamp_initiallyNull() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(mockLog, "https://login.salesforce.com", "user", "pass");

    assertNull(conn.getServerTimestamp());
  }

  @Test
  void testQueryResult_initiallyNull() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(mockLog, "https://login.salesforce.com", "user", "pass");

    assertNull(conn.getQueryResult());
  }

  @Test
  void testQueryResultSize_initiallyZero() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(mockLog, "https://login.salesforce.com", "user", "pass");

    assertEquals(0, conn.getQueryResultSize());
  }

  @Test
  void testRecordsCount_initiallyZero() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(mockLog, "https://login.salesforce.com", "user", "pass");

    assertEquals(0, conn.getRecordsCount());
  }

  @Test
  void testUrlGetterSetter() throws HopException {
    String url = "https://test.salesforce.com/services/Soap/u/64.0";
    SalesforceConnection conn = new SalesforceConnection(mockLog, url, "user", "pass");

    assertEquals(url, conn.getUrl());
  }

  @Test
  void testUsernamePasswordGetters() throws HopException {
    String username = "test.user@example.com";
    String password = "testPass123";

    SalesforceConnection conn =
        new SalesforceConnection(mockLog, "https://test.com", username, password);

    assertEquals(username, conn.getUsername());
    assertEquals(password, conn.getPassword());
  }

  @Test
  void testUsernamePasswordSetters() throws HopException {
    SalesforceConnection conn =
        new SalesforceConnection(mockLog, "https://test.com", "user", "pass");

    conn.setUsername("newUser");
    assertEquals("newUser", conn.getUsername());

    conn.setPassword("newPass");
    assertEquals("newPass", conn.getPassword());
  }
}
