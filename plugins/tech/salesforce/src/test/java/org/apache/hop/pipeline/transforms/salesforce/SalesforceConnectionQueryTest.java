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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import java.util.Calendar;
import java.util.GregorianCalendar;
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

/** Unit tests for Salesforce query operations */
class SalesforceConnectionQueryTest {

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
    when(mockLog.isDebug()).thenReturn(false);

    mockBinding = mock(PartnerConnection.class);
    connection = new SalesforceConnection(mockLog, "https://test.com", "user", "pass");
  }

  @Test
  void testSetCalendar_validRange() throws HopException {
    GregorianCalendar startDate = new GregorianCalendar(2024, Calendar.JANUARY, 1);
    GregorianCalendar endDate = new GregorianCalendar(2024, Calendar.JANUARY, 15);

    connection.setCalendar(SalesforceConnectionUtils.RECORDS_FILTER_UPDATED, startDate, endDate);

    // Should not throw exception - dates are valid
    assertNotNull(connection);
  }

  @Test
  void testSetCalendar_nullStartDate() {
    GregorianCalendar endDate = new GregorianCalendar(2024, Calendar.JANUARY, 15);

    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                connection.setCalendar(
                    SalesforceConnectionUtils.RECORDS_FILTER_UPDATED, null, endDate));

    assertTrue(
        exception.getMessage().contains("StartDate") || exception.getMessage().contains("EndDate"));
  }

  @Test
  void testSetCalendar_nullEndDate() {
    GregorianCalendar startDate = new GregorianCalendar(2024, Calendar.JANUARY, 1);

    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                connection.setCalendar(
                    SalesforceConnectionUtils.RECORDS_FILTER_UPDATED, startDate, null));

    assertTrue(
        exception.getMessage().contains("StartDate") || exception.getMessage().contains("EndDate"));
  }

  @Test
  void testSetCalendar_reversedDates() {
    GregorianCalendar startDate = new GregorianCalendar(2024, Calendar.JANUARY, 20);
    GregorianCalendar endDate = new GregorianCalendar(2024, Calendar.JANUARY, 10);

    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                connection.setCalendar(
                    SalesforceConnectionUtils.RECORDS_FILTER_UPDATED, startDate, endDate));

    assertTrue(exception.getMessage().toLowerCase().contains("date"));
  }

  @Test
  void testSetCalendar_tooFarApart() {
    GregorianCalendar startDate = new GregorianCalendar(2024, Calendar.JANUARY, 1);
    GregorianCalendar endDate = new GregorianCalendar(2024, Calendar.MARCH, 1); // > 30 days

    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                connection.setCalendar(
                    SalesforceConnectionUtils.RECORDS_FILTER_UPDATED, startDate, endDate));

    assertTrue(exception.getMessage().contains("30") || exception.getMessage().contains("older"));
  }

  @Test
  void testQueryMore_withMoreRecords() throws Exception {
    // Setup query result with more records available
    QueryResult mockResult = mock(QueryResult.class);
    when(mockResult.isDone()).thenReturn(false);
    when(mockResult.getQueryLocator()).thenReturn("queryLocator123");
    when(mockResult.getSize()).thenReturn(10);

    SObject[] records = new SObject[5];
    for (int i = 0; i < 5; i++) {
      records[i] = SalesforceTestUtils.createSimpleSObject("id_" + i);
    }
    when(mockResult.getRecords()).thenReturn(records);

    // Mock queryMore call
    when(mockBinding.queryMore(anyString())).thenReturn(mockResult);

    // Set up connection with mock binding
    java.lang.reflect.Field bindingField = SalesforceConnection.class.getDeclaredField("binding");
    bindingField.setAccessible(true);
    bindingField.set(connection, mockBinding);

    java.lang.reflect.Field qrField = SalesforceConnection.class.getDeclaredField("qr");
    qrField.setAccessible(true);
    qrField.set(connection, mockResult);

    boolean hasMore = connection.queryMore();

    assertTrue(hasMore);
    assertEquals(5, connection.getRecordsCount());
    assertEquals(10, connection.getQueryResultSize());

    verify(mockBinding, times(1)).queryMore("queryLocator123");
  }

  @Test
  void testQueryMore_noMoreRecords() throws Exception {
    // Setup query result with no more records
    QueryResult mockResult = mock(QueryResult.class);
    when(mockResult.isDone()).thenReturn(true);

    java.lang.reflect.Field qrField = SalesforceConnection.class.getDeclaredField("qr");
    qrField.setAccessible(true);
    qrField.set(connection, mockResult);

    boolean hasMore = connection.queryMore();

    assertFalse(hasMore);
  }

  @Test
  void testGetRecord_simpleIndex() throws Exception {
    // Setup connection with mock SObjects
    SObject[] mockSObjects = new SObject[3];
    for (int i = 0; i < 3; i++) {
      Map<String, Object> fields = new HashMap<>();
      fields.put("Id", "id_" + i);
      fields.put("Name", "Name_" + i);
      mockSObjects[i] = SalesforceTestUtils.createMockSObject("Account", fields);
    }

    java.lang.reflect.Field sObjectsField = SalesforceConnection.class.getDeclaredField("sObjects");
    sObjectsField.setAccessible(true);
    sObjectsField.set(connection, mockSObjects);

    java.lang.reflect.Field recordsCountField =
        SalesforceConnection.class.getDeclaredField("recordsCount");
    recordsCountField.setAccessible(true);
    recordsCountField.set(connection, 3);

    java.lang.reflect.Field recordsFilterField =
        SalesforceConnection.class.getDeclaredField("recordsFilter");
    recordsFilterField.setAccessible(true);
    recordsFilterField.set(connection, SalesforceConnectionUtils.RECORDS_FILTER_ALL);

    SalesforceRecordValue record = connection.getRecord(0);

    assertNotNull(record);
    assertEquals(0, record.getRecordIndex());
    assertNotNull(record.getRecordValue());
  }

  @Test
  void testGetRecordValue_simpleField() throws Exception {
    Map<String, Object> fields = new HashMap<>();
    fields.put("Name", "Test Account");
    fields.put("Email", "test@example.com");
    SObject mockSObject = SalesforceTestUtils.createMockSObject("Account", fields);

    String nameValue = connection.getRecordValue(mockSObject, "Name");
    String emailValue = connection.getRecordValue(mockSObject, "Email");

    assertEquals("Test Account", nameValue);
    assertEquals("test@example.com", emailValue);
  }

  @Test
  void testGetRecordValue_nullSObject() throws HopException {
    String value = connection.getRecordValue(null, "Name");
    assertNull(value);
  }

  @Test
  void testClose_cleansUpResources() throws Exception {
    // Setup query result
    QueryResult mockResult = mock(QueryResult.class);
    when(mockResult.isDone()).thenReturn(false);

    java.lang.reflect.Field qrField = SalesforceConnection.class.getDeclaredField("qr");
    qrField.setAccessible(true);
    qrField.set(connection, mockResult);

    // Setup sObjects
    SObject[] mockSObjects = new SObject[1];
    mockSObjects[0] = SalesforceTestUtils.createSimpleSObject("id1");

    java.lang.reflect.Field sObjectsField = SalesforceConnection.class.getDeclaredField("sObjects");
    sObjectsField.setAccessible(true);
    sObjectsField.set(connection, mockSObjects);

    // Close should clean up
    connection.close();

    // Verify cleanup (via reflection since fields are private)
    Object qrAfter = qrField.get(connection);
    Object sObjectsAfter = sObjectsField.get(connection);

    // Should be nulled or marked as done
    assertTrue(qrAfter == null || ((QueryResult) qrAfter).isDone());
    assertTrue(sObjectsAfter == null);
  }
}
