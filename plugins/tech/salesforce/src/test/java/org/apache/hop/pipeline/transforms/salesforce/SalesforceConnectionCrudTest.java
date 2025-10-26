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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import java.util.HashMap;
import java.util.Map;
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

/** Unit tests for Salesforce CRUD operations */
class SalesforceConnectionCrudTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private ILogChannel mockLog;
  private PartnerConnection mockBinding;
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
    when(mockLog.isDetailed()).thenReturn(false);

    mockBinding = mock(PartnerConnection.class);
    connection = new SalesforceConnection(mockLog, "https://test.com", "user", "pass");

    // Inject mock binding
    java.lang.reflect.Field bindingField = SalesforceConnection.class.getDeclaredField("binding");
    bindingField.setAccessible(true);
    bindingField.set(connection, mockBinding);
  }

  @Test
  void testInsert_success() throws Exception {
    // Setup mock response
    SaveResult[] mockResults = new SaveResult[2];
    for (int i = 0; i < 2; i++) {
      mockResults[i] = mock(SaveResult.class);
      when(mockResults[i].isSuccess()).thenReturn(true);
      when(mockResults[i].getId()).thenReturn("id_" + i);
    }

    when(mockBinding.create(any(SObject[].class))).thenReturn(mockResults);

    // Create test SObjects
    Map<String, Object> fields1 = new HashMap<>();
    fields1.put("Name", "Test Account 1");
    Map<String, Object> fields2 = new HashMap<>();
    fields2.put("Name", "Test Account 2");

    SObject[] buffer =
        new SObject[] {
          SalesforceTestUtils.createMockSObject("Account", fields1),
          SalesforceTestUtils.createMockSObject("Account", fields2)
        };

    SaveResult[] results = connection.insert(buffer);

    assertNotNull(results);
    assertEquals(2, results.length);
    verify(mockBinding, times(1)).create(any(SObject[].class));
  }

  @Test
  void testInsert_withNullElements() throws Exception {
    SaveResult[] mockResults = new SaveResult[1];
    mockResults[0] = mock(SaveResult.class);
    when(mockResults[0].isSuccess()).thenReturn(true);

    when(mockBinding.create(any(SObject[].class))).thenReturn(mockResults);

    // Buffer with null elements (should be filtered out)
    SObject[] buffer = new SObject[3];
    buffer[0] = null;
    Map<String, Object> fields = new HashMap<>();
    fields.put("Name", "Test");
    buffer[1] = SalesforceTestUtils.createMockSObject("Account", fields);
    buffer[2] = null;

    SaveResult[] results = connection.insert(buffer);

    assertNotNull(results);
    verify(mockBinding, times(1)).create(any(SObject[].class));
  }

  @Test
  void testUpdate_success() throws Exception {
    SaveResult[] mockResults = new SaveResult[1];
    mockResults[0] = mock(SaveResult.class);
    when(mockResults[0].isSuccess()).thenReturn(true);

    when(mockBinding.update(any(SObject[].class))).thenReturn(mockResults);

    Map<String, Object> fields = new HashMap<>();
    fields.put("Id", "001xx000003DHP0");
    fields.put("Name", "Updated Name");
    SObject[] buffer = new SObject[] {SalesforceTestUtils.createMockSObject("Account", fields)};

    SaveResult[] results = connection.update(buffer);

    assertNotNull(results);
    assertEquals(1, results.length);
    verify(mockBinding, times(1)).update(any(SObject[].class));
  }

  @Test
  void testUpsert_success() throws Exception {
    UpsertResult[] mockResults = new UpsertResult[1];
    mockResults[0] = mock(UpsertResult.class);
    when(mockResults[0].isSuccess()).thenReturn(true);
    when(mockResults[0].isCreated()).thenReturn(true);

    when(mockBinding.upsert(anyString(), any(SObject[].class))).thenReturn(mockResults);

    Map<String, Object> fields = new HashMap<>();
    fields.put("ExternalId__c", "EXT123");
    fields.put("Name", "Test Account");
    SObject[] buffer = new SObject[] {SalesforceTestUtils.createMockSObject("Account", fields)};

    UpsertResult[] results = connection.upsert("ExternalId__c", buffer);

    assertNotNull(results);
    assertEquals(1, results.length);
    verify(mockBinding, times(1)).upsert(eq("ExternalId__c"), any(SObject[].class));
  }

  @Test
  void testDelete_success() throws Exception {
    DeleteResult[] mockResults = new DeleteResult[2];
    for (int i = 0; i < 2; i++) {
      mockResults[i] = mock(DeleteResult.class);
      when(mockResults[i].isSuccess()).thenReturn(true);
    }

    when(mockBinding.delete(any(String[].class))).thenReturn(mockResults);

    String[] ids = new String[] {"001xx000003DHP0", "001xx000003DHP1"};

    DeleteResult[] results = connection.delete(ids);

    assertNotNull(results);
    assertEquals(2, results.length);
    verify(mockBinding, times(1)).delete(any(String[].class));
  }

  @Test
  void testInsert_withOAuthTokenExpiry() throws Exception {
    // First call fails with INVALID_SESSION_ID, second succeeds
    when(mockBinding.create(any(SObject[].class)))
        .thenThrow(new ConnectionException("INVALID_SESSION_ID: Session expired"))
        .thenReturn(new SaveResult[] {mock(SaveResult.class)});

    // Setup for OAuth with refresh token
    connection.setAuthenticationType("OAUTH");
    connection.setOauthRefreshToken("refreshToken123");

    Map<String, Object> fields = new HashMap<>();
    fields.put("Name", "Test");
    SObject[] buffer = new SObject[] {SalesforceTestUtils.createMockSObject("Account", fields)};

    // This should NOT auto-refresh in unit tests since we can't mock the HTTP call
    // But we can verify the exception is thrown
    assertThrows(HopException.class, () -> connection.insert(buffer));
  }

  @Test
  void testUpdate_connectionException() throws Exception {
    when(mockBinding.update(any(SObject[].class)))
        .thenThrow(new ConnectionException("Network error"));

    Map<String, Object> fields = new HashMap<>();
    fields.put("Id", "001xx000003DHP0");
    SObject[] buffer = new SObject[] {SalesforceTestUtils.createMockSObject("Account", fields)};

    HopException exception = assertThrows(HopException.class, () -> connection.update(buffer));

    assertTrue(exception.getMessage().contains("update") || exception.getCause() != null);
  }

  @Test
  void testUpsert_connectionException() throws Exception {
    when(mockBinding.upsert(anyString(), any(SObject[].class)))
        .thenThrow(new ConnectionException("Network error"));

    Map<String, Object> fields = new HashMap<>();
    fields.put("ExternalId__c", "EXT123");
    SObject[] buffer = new SObject[] {SalesforceTestUtils.createMockSObject("Account", fields)};

    HopException exception =
        assertThrows(HopException.class, () -> connection.upsert("ExternalId__c", buffer));

    assertTrue(exception.getMessage().contains("upsert") || exception.getCause() != null);
  }

  @Test
  void testDelete_connectionException() throws Exception {
    when(mockBinding.delete(any(String[].class)))
        .thenThrow(new ConnectionException("Network error"));

    String[] ids = new String[] {"001xx000003DHP0"};

    HopException exception = assertThrows(HopException.class, () -> connection.delete(ids));

    assertTrue(exception.getMessage().contains("delete") || exception.getCause() != null);
  }
}
