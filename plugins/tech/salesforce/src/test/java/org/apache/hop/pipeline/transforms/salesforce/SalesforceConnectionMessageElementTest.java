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
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit tests for Salesforce SOAP message element creation and manipulation */
class SalesforceConnectionMessageElementTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @Test
  void testFromTemplateElement_withValue() throws Exception {
    XmlObject element = SalesforceConnection.fromTemplateElement("TestField", "TestValue", true);

    assertNotNull(element);
    assertEquals("TestField", element.getName().getLocalPart());
    assertEquals("TestValue", element.getValue());
  }

  @Test
  void testFromTemplateElement_withoutValue() throws Exception {
    XmlObject element = SalesforceConnection.fromTemplateElement("TestField", 123, false);

    assertNotNull(element);
    assertEquals("TestField", element.getName().getLocalPart());
    assertNull(element.getValue()); // setValue=false
  }

  @Test
  void testFromTemplateElement_withIntegerValue() throws Exception {
    XmlObject element = SalesforceConnection.fromTemplateElement("NumberField", 42, true);

    assertNotNull(element);
    assertEquals("NumberField", element.getName().getLocalPart());
    assertEquals(42, element.getValue());
  }

  @Test
  void testCreateMessageElement_simpleField() throws Exception {
    XmlObject element = SalesforceConnection.createMessageElement("Name", "Test Account", false);

    assertNotNull(element);
    assertEquals("Name", element.getName().getLocalPart());
    assertEquals("Test Account", element.getValue());
  }

  @Test
  void testCreateMessageElement_externalKey() throws Exception {
    // Format: objectType:externalIdField/lookupField
    XmlObject element =
        SalesforceConnection.createMessageElement("Account:ExternalId__c/Account", "EXT123", true);

    assertNotNull(element);
    assertEquals("Account", element.getName().getLocalPart());
    assertEquals("Account", element.getField("type"));
    assertEquals("EXT123", element.getField("ExternalId__c"));
  }

  @Test
  void testCreateMessageElement_externalKeyWithSlash() throws Exception {
    // Full format with explicit lookup field
    XmlObject element =
        SalesforceConnection.createMessageElement(
            "Contact:Email/AccountId", "test@example.com", true);

    assertNotNull(element);
    assertEquals("AccountId", element.getName().getLocalPart());
    assertEquals("Contact", element.getField("type"));
    assertEquals("test@example.com", element.getField("Email"));
  }

  @Test
  void testCreateMessageElement_externalKeyMissingColon() {
    // Invalid format - missing colon
    assertThrows(
        HopException.class,
        () -> SalesforceConnection.createMessageElement("InvalidFormat", "value", true));
  }

  @Test
  void testGetChildren_withValidSObject() {
    Map<String, Object> fields = new HashMap<>();
    fields.put("Id", "001xx000003DHP0");
    fields.put("Name", "Test Account");
    fields.put("Email", "test@example.com");

    SObject sObject = SalesforceTestUtils.createMockSObject("Account", fields);

    XmlObject[] children = SalesforceConnection.getChildren(sObject);

    assertNotNull(children);
    assertEquals(3, children.length);
  }

  @Test
  void testGetChildren_nullSObject() {
    XmlObject[] children = SalesforceConnection.getChildren(null);
    assertNull(children);
  }

  @Test
  void testGetChildren_filtersReservedFields() {
    SObject sObject = new SObject();
    sObject.setType("Account");
    sObject.setName(new QName(Constants.PARTNER_SOBJECT_NS, "Account"));

    // Add reserved field 'type'
    XmlObject typeField = new XmlObject();
    typeField.setName(new QName(Constants.PARTNER_SOBJECT_NS, "type"));
    typeField.setValue("Account");
    sObject.addField("type", typeField);

    // Add reserved field 'fieldsToNull'
    XmlObject fieldsToNullField = new XmlObject();
    fieldsToNullField.setName(new QName(Constants.PARTNER_SOBJECT_NS, "fieldsToNull"));
    sObject.addField("fieldsToNull", fieldsToNullField);

    // Add regular field
    XmlObject regularField = new XmlObject();
    regularField.setName(new QName(Constants.PARTNER_SOBJECT_NS, "Name"));
    regularField.setValue("Test");
    sObject.addField("Name", regularField);

    XmlObject[] children = SalesforceConnection.getChildren(sObject);

    // Should only return non-reserved fields
    assertNotNull(children);
    assertEquals(1, children.length);
    assertEquals("Name", children[0].getName().getLocalPart());
  }

  @Test
  void testGetChildren_emptyObject() {
    SObject sObject = new SObject();
    sObject.setType("Account");

    XmlObject[] children = SalesforceConnection.getChildren(sObject);

    // Empty object should return null
    assertNull(children);
  }
}
