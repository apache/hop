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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.bind.XmlObject;
import com.sforce.ws.wsdl.Constants;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;
import javax.xml.namespace.QName;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

class SalesforceConnectionTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private ILogChannel logInterface = mock(ILogChannel.class);
  private String url = "url";
  private String username = "username";
  private String password = "password";
  private int recordsFilter = 0;

  @BeforeAll
  static void setUpClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @Test
  void testConstructor_emptyUrl() throws HopException {
    try {
      new SalesforceConnection(logInterface, null, username, password);
      fail();
    } catch (HopException expected) {
      // OK
    }
  }

  @Test
  void testConstructor_emptyUserName() throws HopException {
    try {
      new SalesforceConnection(logInterface, url, null, password);
      fail();
    } catch (HopException expected) {
      // OK
    }
  }

  @Test
  void testSetCalendarStartNull() throws HopException {
    SalesforceConnection connection =
        new SalesforceConnection(logInterface, url, username, password);
    GregorianCalendar endDate = new GregorianCalendar(2000, 2, 10);
    try {
      connection.setCalendar(recordsFilter, null, endDate);
      fail();
    } catch (HopException expected) {
      // OK
    }
  }

  @Test
  void testSetCalendarEndNull() throws HopException {
    SalesforceConnection connection =
        new SalesforceConnection(logInterface, url, username, password);
    GregorianCalendar startDate = new GregorianCalendar(2000, 2, 10);
    try {
      connection.setCalendar(recordsFilter, startDate, null);
      fail();
    } catch (HopException expected) {
      // OK
    }
  }

  @Test
  void testSetCalendarStartDateTooOlder() throws HopException {
    SalesforceConnection connection =
        new SalesforceConnection(logInterface, url, username, password);
    GregorianCalendar startDate = new GregorianCalendar(2000, 3, 20);
    GregorianCalendar endDate = new GregorianCalendar(2000, 2, 10);
    try {
      connection.setCalendar(recordsFilter, startDate, endDate);
      fail();
    } catch (HopException expected) {
      // OK
    }
  }

  @Test
  void testSetCalendarDatesTooFarApart() throws HopException {
    SalesforceConnection connection =
        new SalesforceConnection(logInterface, url, username, password);
    GregorianCalendar startDate = new GregorianCalendar(2000, 1, 1);
    GregorianCalendar endDate = new GregorianCalendar(2000, 2, 11);
    try {
      connection.setCalendar(recordsFilter, startDate, endDate);
      fail();
    } catch (HopException expected) {
      // OK
    }
  }

  @Test
  void testConstructor() {
    SalesforceConnection conn;

    // Test all-invalid parameters
    try {
      conn = null;
      conn = new SalesforceConnection(null, null, null, null);
      fail();
    } catch (HopException expected) {
      // Ignore, expected result
    }

    // Test null Log Interface
    try {
      conn = null;
      conn = new SalesforceConnection(null, "http://localhost:1234", "anonymous", "mypwd");
      assertFalse(conn.getURL().isEmpty());
      assertFalse(conn.getUsername().isEmpty());
      assertFalse(conn.getPassword().isEmpty());
    } catch (HopException e) {
      fail();
    }

    // Test valid Log Interface
    try {
      conn = null;
      conn =
          new SalesforceConnection(
              new LogChannel(this), "http://localhost:1234", "anonymous", "mypwd");
      assertFalse(conn.getURL().isEmpty());
      assertFalse(conn.getUsername().isEmpty());
      assertFalse(conn.getPassword().isEmpty());
    } catch (HopException e) {
      fail();
    }

    // Test missing target URL (should fail)
    try {
      conn = null;
      conn = new SalesforceConnection(null, null, "anonymous", "mypwd");
      fail();
    } catch (HopException expected) {
      // Ignore, expected result
    }

    // Test missing username (should fail)
    try {
      conn = null;
      conn = new SalesforceConnection(null, "http://localhost:1234", null, "mypwd");
      fail();
    } catch (HopException expected) {
      // Ignore, expected result
    }

    // Test missing password (should fail)
    try {
      conn = null;
      conn = new SalesforceConnection(null, "http://localhost:1234", "anonymous", null);
      assertFalse(conn.getURL().isEmpty());
      assertEquals("anonymous", conn.getUsername());
    } catch (HopException e) {
      fail();
    }
  }

  @Test
  void testSetCalendar() {
    SalesforceConnection conn = mock(SalesforceConnection.class, Mockito.CALLS_REAL_METHODS);

    // Test valid data
    try {
      conn.setCalendar(
          new Random().nextInt(SalesforceConnectionUtils.recordsFilterDesc.length),
          new GregorianCalendar(2016, Calendar.JANUARY, 1),
          new GregorianCalendar(2016, Calendar.JANUARY, 31));
      // No errors detected
    } catch (HopException e) {
      fail();
    }

    // Test reversed dates (should fail)
    try {
      conn.setCalendar(
          new Random().nextInt(SalesforceConnectionUtils.recordsFilterDesc.length),
          new GregorianCalendar(2016, Calendar.JANUARY, 31),
          new GregorianCalendar(2016, Calendar.JANUARY, 1));
      fail();
    } catch (HopException expected) {
      // Ignore, expected result
    }

    // Test null start date (should fail)
    try {
      conn.setCalendar(
          new Random().nextInt(SalesforceConnectionUtils.recordsFilterDesc.length),
          null,
          new GregorianCalendar(2016, Calendar.JANUARY, 31));
      fail();
    } catch (HopException expected) {
      // Ignore, expected result
    }

    // Test null end date (should fail)
    try {
      conn.setCalendar(
          new Random().nextInt(SalesforceConnectionUtils.recordsFilterDesc.length),
          new GregorianCalendar(2016, Calendar.JANUARY, 1),
          null);
      fail();
    } catch (HopException expected) {
      // Ignore, expected result
    }
  }

  @Test
  void testMessageElements() throws Exception {
    XmlObject me = SalesforceConnection.fromTemplateElement("myName", 123, false);
    assertNotNull(me);
    assertEquals("myName", me.getName().getLocalPart());
    assertNull(me.getValue());

    me = null;
    me = SalesforceConnection.fromTemplateElement("myName", 123, true);
    assertNotNull(me);
    assertEquals("myName", me.getName().getLocalPart());
    assertEquals(123, me.getValue());

    me = null;
    me = SalesforceConnection.createMessageElement("myName", 123, false);
    assertNotNull(me);
    assertEquals("myName", me.getName().getLocalPart());
    assertEquals(123, me.getValue());

    me = null;
    try {
      me = SalesforceConnection.createMessageElement("myName", 123, true);
      fail();
    } catch (Exception expected) {
      // Ignore, name was expected to have a colon ':'
    }

    me = null;
    try {
      me = SalesforceConnection.createMessageElement("myType:Name", "123", true);
      assertNotNull(me);
      assertEquals("Name", me.getName().getLocalPart());
      assertEquals("myType", me.getField("type"));
      assertEquals("123", me.getField("Name"));
    } catch (Exception expected) {
      fail();
    }

    me = null;
    try {
      me = SalesforceConnection.createMessageElement("myType:Name/MyLookupField", 123, true);
      assertNotNull(me);
      assertEquals("MyLookupField", me.getName().getLocalPart());
      // assertEquals( "myType", me.getField( "type" ) );
      assertEquals(123, me.getField("Name"));
    } catch (Exception expected) {
      fail();
    }
  }

  @Test
  void testCreateBinding() throws HopException, ConnectionException {
    SalesforceConnection conn =
        new SalesforceConnection(null, "http://localhost:1234", "aUser", "aPass");
    ConnectorConfig config = new ConnectorConfig();
    config.setAuthEndpoint(Connector.END_POINT);
    config.setManualLogin(true); // Required to prevent connection attempt during test

    assertNull(conn.getBinding());

    conn.createBinding(config);
    PartnerConnection binding1 = conn.getBinding();
    conn.createBinding(config);
    PartnerConnection binding2 = conn.getBinding();
    assertSame(binding1, binding2);
  }

  @Test // Hop-15973
  void testGetRecordValue() throws Exception { // Hop-15973
    SalesforceConnection conn = mock(SalesforceConnection.class, Mockito.CALLS_REAL_METHODS);
    SObject sObject = new SObject();
    sObject.setName(new QName(Constants.PARTNER_SOBJECT_NS, "sObject"));

    SObject testObject = createObject("field", "value");
    sObject.addField("field", testObject);
    assertEquals("value", conn.getRecordValue(sObject, "field"), "Get value of simple record");

    SObject parentObject = createObject("parentField", null);
    sObject.addField("parentField", parentObject);
    SObject childObject = createObject("subField", "subValue");
    parentObject.addField("subField", childObject);
    assertEquals(
        "subValue",
        conn.getRecordValue(sObject, "parentField.subField"),
        "Get value of record with hierarchy");

    XmlObject nullObject = new XmlObject(new QName("nullField"));
    sObject.addField("nullField", nullObject);
    assertEquals(
        null,
        conn.getRecordValue(sObject, "nullField.childField"),
        "Get null value when relational query id is null");
  }

  private SObject createObject(String fieldName, String value) {
    SObject result = new SObject();
    result.setName(new QName(Constants.PARTNER_SOBJECT_NS, fieldName));
    result.setValue(value);
    return result;
  }

  @Test // Hop-16459
  void getFieldsTest() throws HopException {
    String name = "name";
    SalesforceConnection conn =
        new SalesforceConnection(null, "http://localhost:1234", "aUser", "aPass");
    Field[] fields = new Field[1];
    Field field = new Field();
    field.setRelationshipName("Parent");
    field.setName(name);
    fields[0] = field;
    String[] names = conn.getFields(fields);
    assertEquals(name, names[0]);
  }
}
