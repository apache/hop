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

package org.apache.hop.mongo.wrapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.mongodb.BasicDBObject;
import java.math.BigDecimal;
import java.util.Date;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.mongo.wrapper.field.MongoField;
import org.bson.types.Binary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

class MongoFieldTest {

  @Mock IVariables variables;
  private MongoField field;

  @BeforeEach
  void before() throws HopPluginException {
    MockitoAnnotations.openMocks(this);
    when(variables.resolve(any(String.class)))
        .thenAnswer(
            (Answer<String>) invocationOnMock -> (String) invocationOnMock.getArguments()[0]);
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.init();
  }

  @Test
  void testGetPath() {
    MongoField mongoField = new MongoField();

    mongoField.fieldPath = "$.parent[0].child[0]";
    assertEquals("parent.0.child.0", mongoField.getPath());

    mongoField.fieldPath = "$.field[*]";
    assertEquals("field", mongoField.getPath());

    mongoField.fieldPath = "$.parent.child";
    assertEquals("parent.child", mongoField.getPath());
  }

  // "Number", "String", "Date", "Boolean", "Integer", "BigNumber", "Serializable",
  // "Binary", "Timestamp", "Internet Address"
  @Test
  void testDatatypes() throws HopException {
    initField("Number");
    assertEquals(1.1, field.getHopValue(1.1));
    assertEquals(1.1, field.getHopValue("1.1"));
    assertEquals(1.1, field.getHopValue(new Binary(new byte[] {'1', '.', '1'})));

    initField("BigNumber");
    Date date = new Date();
    assertEquals(BigDecimal.valueOf(date.getTime()), field.getHopValue(date));
    assertEquals(BigDecimal.valueOf(12341234), field.getHopValue(12341234));
    assertEquals(BigDecimal.valueOf(12341234), field.getHopValue("12341234"));

    initField("Boolean");
    assertTrue((Boolean) field.getHopValue(1));
    assertTrue((Boolean) field.getHopValue("Y"));
    assertFalse((Boolean) field.getHopValue(0));
    assertTrue((Boolean) field.getHopValue(true));

    initField("Binary");
    byte[] data = new byte[] {'a', 'b', 'c'};
    assertEquals(data, field.getHopValue(data));

    initField("Date");
    assertEquals(date, field.getHopValue(date));
    assertEquals(date, field.getHopValue(date.getTime()));
    try {
      field.getHopValue("Not a date value");
      fail("expected exception");
    } catch (Exception e) {
      assertTrue(e instanceof HopException);
    }

    initField("Integer");
    assertEquals(123L, field.getHopValue(123));
    assertEquals(123L, field.getHopValue("123"));
    assertEquals(123L, field.getHopValue(new Binary(new byte[] {'1', '2', '3'})));

    initField("String");
    assertEquals("foo", field.getHopValue("foo"));
    assertEquals("123", field.getHopValue(123));

    initField("UUID");
    java.util.UUID uuid = java.util.UUID.fromString("4d0e4aee-d845-4f5e-8c7d-9d5cff1c2a4d");
    assertEquals(uuid, field.getHopValue(uuid));
    assertEquals(uuid, field.getHopValue("4d0e4aee-d845-4f5e-8c7d-9d5cff1c2a4d"));
  }

  @Test
  void testConvertArrayIndicesToHopValue() throws HopException {
    BasicDBObject dbObj =
        (BasicDBObject) BasicDBObject.parse("{ parent : { fieldName : ['valA', 'valB'] } } ");

    initField("fieldName", "$.parent.fieldName[0]", "String");
    assertEquals("valA", field.convertToHopValue(dbObj));
    initField("fieldName", "$.parent.fieldName[1]", "String");
    assertEquals("valB", field.convertToHopValue(dbObj));
  }

  @Test
  void testConvertUndefinedOrNullToHopValue() throws HopException {
    BasicDBObject dbObj = BasicDBObject.parse("{ test1 : undefined, test2 : null } ");
    initField("fieldName", "$.test1", "String");
    assertNull(field.convertToHopValue(dbObj), "Undefined should be interpreted as null ");
    initField("fieldName", "$.test2", "String");
    assertNull(field.convertToHopValue(dbObj));
    initField("fieldName", "$.test3", "String");
    assertNull(field.convertToHopValue(dbObj));
  }

  private void initField(String type) throws HopException {
    initField("fieldName", "$.parent.child.fieldName", type);
  }

  private void initField(String name, String path, String type) throws HopException {
    field = new MongoField();
    field.fieldName = name;
    field.fieldPath = path;
    field.hopType = type;
    field.init(0);
    field.reset(variables);
  }
}
