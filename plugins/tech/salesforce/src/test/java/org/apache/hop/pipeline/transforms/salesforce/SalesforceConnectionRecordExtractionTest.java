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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XmlObject;
import com.sforce.ws.wsdl.Constants;
import java.util.HashMap;
import java.util.Map;
import javax.xml.namespace.QName;
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

/** Unit tests for Salesforce record value extraction and transformation */
class SalesforceConnectionRecordExtractionTest {

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
  void testGetRecordValue_stringField() throws HopException {
    Map<String, Object> fields = new HashMap<>();
    fields.put("Name", "Test Account");
    fields.put("Type", "Customer");
    SObject sObject = SalesforceTestUtils.createMockSObject("Account", fields);

    String name = connection.getRecordValue(sObject, "Name");
    String type = connection.getRecordValue(sObject, "Type");

    assertEquals("Test Account", name);
    assertEquals("Customer", type);
  }

  @Test
  void testGetRecordValue_numericField() throws HopException {
    Map<String, Object> fields = new HashMap<>();
    fields.put("NumberOfEmployees", 500);
    fields.put("AnnualRevenue", 1000000.50);
    SObject sObject = SalesforceTestUtils.createMockSObject("Account", fields);

    String employees = connection.getRecordValue(sObject, "NumberOfEmployees");
    String revenue = connection.getRecordValue(sObject, "AnnualRevenue");

    assertEquals("500", employees);
    assertEquals("1000000.5", revenue);
  }

  @Test
  void testGetRecordValue_booleanField() throws HopException {
    Map<String, Object> fields = new HashMap<>();
    fields.put("IsActive", true);
    fields.put("IsDeleted", false);
    SObject sObject = SalesforceTestUtils.createMockSObject("Account", fields);

    String active = connection.getRecordValue(sObject, "IsActive");
    String deleted = connection.getRecordValue(sObject, "IsDeleted");

    assertEquals("true", active);
    assertEquals("false", deleted);
  }

  @Test
  void testGetRecordValue_nullField() throws HopException {
    Map<String, Object> fields = new HashMap<>();
    fields.put("Description", null);
    SObject sObject = SalesforceTestUtils.createMockSObject("Account", fields);

    String value = connection.getRecordValue(sObject, "Description");

    assertNull(value);
  }

  @Test
  void testGetRecordValue_nonExistentField() throws HopException {
    Map<String, Object> fields = new HashMap<>();
    fields.put("Name", "Test");
    SObject sObject = SalesforceTestUtils.createMockSObject("Account", fields);

    String value = connection.getRecordValue(sObject, "NonExistentField");

    assertNull(value);
  }

  @Test
  void testGetRecordValue_hierarchicalField() throws HopException {
    // Create parent object
    SObject parentObject = new SObject();
    parentObject.setType("Account");
    parentObject.setName(new QName(Constants.PARTNER_SOBJECT_NS, "Account"));

    // Add field to parent
    XmlObject nameField = new XmlObject();
    nameField.setName(new QName("Name"));
    nameField.setValue("Parent Account");
    parentObject.addField("Name", nameField);

    // Create child object with parent reference
    SObject childObject = new SObject();
    childObject.setType("Contact");
    childObject.setName(new QName(Constants.PARTNER_SOBJECT_NS, "Contact"));

    // Add parent as a field - parentObject itself is the nested SObject
    parentObject.setName(new QName("Account"));
    childObject.addField("Account", parentObject);

    // Extract hierarchical value
    String value = connection.getRecordValue(childObject, "Account.Name");

    assertEquals("Parent Account", value);
  }

  @Test
  void testGetRecordValue_deepHierarchy() throws HopException {
    // Level 3: Grandparent
    SObject grandparent = new SObject();
    grandparent.setName(new QName("Owner"));
    XmlObject gpName = new XmlObject();
    gpName.setName(new QName("Name"));
    gpName.setValue("Manager Name");
    grandparent.addField("Name", gpName);

    // Level 2: Parent with grandparent
    SObject parent = new SObject();
    parent.setName(new QName("Account"));
    parent.addField("Owner", grandparent);

    // Level 1: Child with parent
    SObject child = new SObject();
    child.setName(new QName(Constants.PARTNER_SOBJECT_NS, "Opportunity"));
    child.addField("Account", parent);

    // Extract deep hierarchy
    String value = connection.getRecordValue(child, "Account.Owner.Name");

    assertEquals("Manager Name", value);
  }

  @Test
  void testGetRecordValue_hierarchyWithNullIntermediate() throws HopException {
    SObject sObject = new SObject();
    sObject.setName(new QName(Constants.PARTNER_SOBJECT_NS, "Contact"));

    // Add null parent reference
    XmlObject accountField = new XmlObject();
    accountField.setName(new QName("Account"));
    accountField.setValue(null);
    sObject.addField("Account", accountField);

    // Try to get sub-field of null parent
    String value = connection.getRecordValue(sObject, "Account.Name");

    assertNull(value);
  }

  @Test
  void testGetRecordValue_queryResultField() throws HopException {
    // Create a QueryResult as a field value (for subqueries)
    QueryResult subQueryResult = new QueryResult();
    SObject[] subRecords = new SObject[1];

    Map<String, Object> contactFields = new HashMap<>();
    contactFields.put("Name", "Contact Name");
    subRecords[0] = SalesforceTestUtils.createMockSObject("Contact", contactFields);

    subQueryResult.setRecords(subRecords);
    subQueryResult.setSize(1);

    SObject accountObject = new SObject();
    accountObject.setName(new QName(Constants.PARTNER_SOBJECT_NS, "Account"));

    XmlObject contactsField = new XmlObject();
    contactsField.setName(new QName("Contacts"));
    contactsField.setValue(subQueryResult);
    accountObject.addField("Contacts", contactsField);

    // Extract QueryResult field
    String value = connection.getRecordValue(accountObject, "Contacts");

    assertNotNull(value);
    // Should return JSON representation
    assertTrue(value.contains("[") && value.contains("]")); // JSON array
    assertTrue(value.contains("Contact Name"));
  }

  @Test
  void testBuildJSONSObject() throws Exception {
    Map<String, Object> fields = new HashMap<>();
    fields.put("Id", "001xx000003DHP0");
    fields.put("Name", "Test Account");
    fields.put("Active", true);
    SObject sObject = SalesforceTestUtils.createMockSObject("Account", fields);

    // Use reflection to call private buildJSONSObject
    java.lang.reflect.Method method =
        SalesforceConnection.class.getDeclaredMethod(
            "buildJSONSObject", com.sforce.soap.partner.sobject.SObject.class);
    method.setAccessible(true);
    org.json.simple.JSONObject result =
        (org.json.simple.JSONObject) method.invoke(connection, sObject);

    assertNotNull(result);
    assertEquals(3, result.size());
    // The JSONObject keys are QName objects, need to find by matching localPart
    boolean foundId = false;
    boolean foundName = false;
    for (Object key : result.keySet()) {
      if (key instanceof QName qname) {
        if ("Id".equals(qname.getLocalPart())) {
          assertEquals("001xx000003DHP0", result.get(key));
          foundId = true;
        } else if ("Name".equals(qname.getLocalPart())) {
          assertEquals("Test Account", result.get(key));
          foundName = true;
        }
      }
    }
    assertTrue(foundId, "Id field not found");
    assertTrue(foundName, "Name field not found");
  }

  @Test
  void testGetChildren_multipleFields() {
    SObject sObject = new SObject();
    sObject.setName(new QName(Constants.PARTNER_SOBJECT_NS, "Account"));

    for (int i = 0; i < 5; i++) {
      XmlObject field = new XmlObject();
      field.setName(new QName("Field" + i));
      field.setValue("Value" + i);
      sObject.addField("Field" + i, field);
    }

    XmlObject[] children = SalesforceConnection.getChildren(sObject);

    assertNotNull(children);
    assertEquals(5, children.length);
  }
}
