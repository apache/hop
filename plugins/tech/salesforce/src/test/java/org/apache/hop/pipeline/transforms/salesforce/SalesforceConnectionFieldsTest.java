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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;
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

/** Unit tests for Salesforce field and object metadata operations */
class SalesforceConnectionFieldsTest {

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
    mockBinding = mock(PartnerConnection.class);
    connection = new SalesforceConnection(mockLog, "https://test.com", "user", "pass");

    // Inject mock binding
    java.lang.reflect.Field bindingField = SalesforceConnection.class.getDeclaredField("binding");
    bindingField.setAccessible(true);
    bindingField.set(connection, mockBinding);
  }

  @Test
  void testGetAllAvailableObjects_onlyQueryable() throws Exception {
    DescribeGlobalResult mockResult = mock(DescribeGlobalResult.class);
    DescribeGlobalSObjectResult[] sobjects = new DescribeGlobalSObjectResult[3];

    for (int i = 0; i < 3; i++) {
      sobjects[i] = mock(DescribeGlobalSObjectResult.class);
      when(sobjects[i].getName()).thenReturn("Object" + i);
      when(sobjects[i].isQueryable()).thenReturn(i % 2 == 0); // 0 and 2 are queryable
    }

    when(mockResult.getSobjects()).thenReturn(sobjects);
    when(mockBinding.describeGlobal()).thenReturn(mockResult);

    String[] objects = connection.getAllAvailableObjects(true);

    assertNotNull(objects);
    assertEquals(2, objects.length); // Only queryable objects
    assertEquals("Object0", objects[0]);
    assertEquals("Object2", objects[1]);

    verify(mockBinding, times(1)).describeGlobal();
  }

  @Test
  void testGetAllAvailableObjects_all() throws Exception {
    DescribeGlobalResult mockResult = mock(DescribeGlobalResult.class);
    DescribeGlobalSObjectResult[] sobjects = new DescribeGlobalSObjectResult[2];

    sobjects[0] = mock(DescribeGlobalSObjectResult.class);
    when(sobjects[0].getName()).thenReturn("Account");
    when(sobjects[0].isQueryable()).thenReturn(true);

    sobjects[1] = mock(DescribeGlobalSObjectResult.class);
    when(sobjects[1].getName()).thenReturn("NonQueryableObject");
    when(sobjects[1].isQueryable()).thenReturn(false);

    when(mockResult.getSobjects()).thenReturn(sobjects);
    when(mockBinding.describeGlobal()).thenReturn(mockResult);

    String[] objects = connection.getAllAvailableObjects(false);

    assertNotNull(objects);
    assertEquals(2, objects.length); // All objects
  }

  @Test
  void testGetObjectFields_success() throws Exception {
    DescribeSObjectResult mockDescribe = mock(DescribeSObjectResult.class);
    when(mockDescribe.isQueryable()).thenReturn(true);

    Field[] fields = createMockFields(3);
    when(mockDescribe.getFields()).thenReturn(fields);
    when(mockBinding.describeSObject("Account")).thenReturn(mockDescribe);

    Field[] result = connection.getObjectFields("Account");

    assertNotNull(result);
    assertEquals(3, result.length);
    verify(mockBinding, times(1)).describeSObject("Account");
  }

  @Test
  void testGetObjectFields_notQueryable() throws Exception {
    DescribeSObjectResult mockDescribe = mock(DescribeSObjectResult.class);
    when(mockDescribe.isQueryable()).thenReturn(false);
    when(mockBinding.describeSObject("NonQueryableObject")).thenReturn(mockDescribe);

    HopException exception =
        assertThrows(HopException.class, () -> connection.getObjectFields("NonQueryableObject"));

    assertTrue(exception.getMessage().toLowerCase().contains("queryable"));
  }

  @Test
  void testGetObjectFields_excludeNonUpdatable() throws Exception {
    DescribeSObjectResult mockDescribe = mock(DescribeSObjectResult.class);
    when(mockDescribe.isQueryable()).thenReturn(true);

    Field[] allFields = new Field[4];

    // ID field (updatable ID field - should be included)
    allFields[0] = mock(Field.class);
    when(allFields[0].getName()).thenReturn("Id");
    when(allFields[0].getType()).thenReturn(FieldType.id);
    when(allFields[0].isUpdateable()).thenReturn(false);
    when(allFields[0].isCalculated()).thenReturn(false);

    // Regular updatable field
    allFields[1] = mock(Field.class);
    when(allFields[1].getName()).thenReturn("Name");
    when(allFields[1].getType()).thenReturn(FieldType.string);
    when(allFields[1].isUpdateable()).thenReturn(true);
    when(allFields[1].isCalculated()).thenReturn(false);

    // Calculated field (should be excluded)
    allFields[2] = mock(Field.class);
    when(allFields[2].getName()).thenReturn("CalculatedField");
    when(allFields[2].getType()).thenReturn(FieldType.string);
    when(allFields[2].isUpdateable()).thenReturn(true);
    when(allFields[2].isCalculated()).thenReturn(true);

    // Non-updatable field (should be excluded)
    allFields[3] = mock(Field.class);
    when(allFields[3].getName()).thenReturn("ReadOnlyField");
    when(allFields[3].getType()).thenReturn(FieldType.string);
    when(allFields[3].isUpdateable()).thenReturn(false);
    when(allFields[3].isCalculated()).thenReturn(false);

    when(mockDescribe.getFields()).thenReturn(allFields);
    when(mockBinding.describeSObject("Account")).thenReturn(mockDescribe);

    Field[] result = connection.getObjectFields("Account", true);

    assertNotNull(result);
    assertEquals(2, result.length); // Only Id and Name
    assertEquals("Id", result[0].getName());
    assertEquals("Name", result[1].getName());
  }

  @Test
  void testGetFields_fromFieldArray() throws Exception {
    Field[] fields = createMockFields(3);

    String[] fieldNames = connection.getFields(fields);

    assertNotNull(fieldNames);
    assertEquals(3, fieldNames.length);
    assertEquals("Field0", fieldNames[0]);
    assertEquals("Field1", fieldNames[1]);
    assertEquals("Field2", fieldNames[2]);
  }

  @Test
  void testGetFields_nullArray() throws Exception {
    String[] result = connection.getFields((Field[]) null);
    assertNull(result);
  }

  @Test
  void testGetFields_fromObjectName() throws Exception {
    DescribeSObjectResult mockDescribe = mock(DescribeSObjectResult.class);
    when(mockDescribe.isQueryable()).thenReturn(true);

    Field[] fields = createMockFields(2);
    when(mockDescribe.getFields()).thenReturn(fields);
    when(mockBinding.describeSObject("Contact")).thenReturn(mockDescribe);

    String[] fieldNames = connection.getFields("Contact");

    assertNotNull(fieldNames);
    assertEquals(2, fieldNames.length);
  }

  @Test
  void testGetFields_withReferenceFields() throws Exception {
    DescribeSObjectResult mockDescribe = mock(DescribeSObjectResult.class);
    when(mockDescribe.isQueryable()).thenReturn(true);

    Field[] fields = new Field[2];

    // Regular field
    fields[0] = mock(Field.class);
    when(fields[0].getName()).thenReturn("Name");
    when(fields[0].getType()).thenReturn(FieldType.string);

    // Reference field
    fields[1] = mock(Field.class);
    when(fields[1].getName()).thenReturn("AccountId");
    when(fields[1].getType()).thenReturn(FieldType.reference);
    when(fields[1].getReferenceTo()).thenReturn(new String[] {"Account"});
    when(fields[1].getRelationshipName()).thenReturn("Account");

    // Mock referenced object fields
    DescribeSObjectResult mockAccountDescribe = mock(DescribeSObjectResult.class);
    when(mockAccountDescribe.isQueryable()).thenReturn(true);

    Field[] accountFields = new Field[2];
    accountFields[0] = mock(Field.class);
    when(accountFields[0].getName()).thenReturn("Id");
    when(accountFields[0].getType()).thenReturn(FieldType.id);
    when(accountFields[0].isIdLookup()).thenReturn(false);

    accountFields[1] = mock(Field.class);
    when(accountFields[1].getName()).thenReturn("ExternalId__c");
    when(accountFields[1].getType()).thenReturn(FieldType.string);
    when(accountFields[1].isIdLookup()).thenReturn(true);

    when(mockAccountDescribe.getFields()).thenReturn(accountFields);

    when(mockBinding.describeSObject("Contact")).thenReturn(mockDescribe);
    when(mockBinding.describeSObject("Account")).thenReturn(mockAccountDescribe);
    when(mockDescribe.getFields()).thenReturn(fields);

    String[] fieldNames = connection.getFields(fields, true);

    assertNotNull(fieldNames);
    // Should include: Name, AccountId, and Account:ExternalId__c/Account
    assertTrue(fieldNames.length >= 2);
    assertEquals("Name", fieldNames[0]);
    assertEquals("AccountId", fieldNames[1]);
  }

  private Field[] createMockFields(int count) {
    Field[] fields = new Field[count];
    for (int i = 0; i < count; i++) {
      fields[i] = mock(Field.class);
      when(fields[i].getName()).thenReturn("Field" + i);
      when(fields[i].getType()).thenReturn(FieldType.string);
      when(fields[i].isUpdateable()).thenReturn(true);
      when(fields[i].isCalculated()).thenReturn(false);
    }
    return fields;
  }
}
