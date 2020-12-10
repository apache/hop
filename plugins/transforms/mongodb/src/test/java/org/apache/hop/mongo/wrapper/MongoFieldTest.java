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

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.mongo.wrapper.field.MongoField;
import org.bson.types.Binary;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.math.BigDecimal;
import java.util.Date;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class MongoFieldTest {

  @Mock IVariables variables;
  private MongoField field;

  @Before
  public void before() throws HopPluginException {
    MockitoAnnotations.initMocks(this);
    when(variables.resolve(any(String.class)))
        .thenAnswer(
            new Answer<String>() {
              @Override
              public String answer(InvocationOnMock invocationOnMock) throws Throwable {
                return (String) invocationOnMock.getArguments()[0];
              }
            });
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.init();
  }

  @Test
  public void testGetPath() throws Exception {
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
  public void testDatatypes() throws HopException {
    initField("Number");
    assertThat(field.getHopValue(1.1), equalTo((Object) 1.1));
    assertThat(field.getHopValue("1.1"), equalTo((Object) 1.1));
    assertThat(field.getHopValue(new Binary(new byte[] {'1', '.', '1'})), equalTo((Object) 1.1));

    initField("BigNumber");
    Date date = new Date();
    assertThat(field.getHopValue(date), equalTo((Object) BigDecimal.valueOf(date.getTime())));
    assertThat(field.getHopValue(12341234), equalTo((Object) BigDecimal.valueOf(12341234)));
    assertThat(field.getHopValue("12341234"), equalTo((Object) BigDecimal.valueOf(12341234)));

    initField("Boolean");
    assertTrue((Boolean) field.getHopValue(1));
    assertTrue((Boolean) field.getHopValue("Y"));
    assertFalse((Boolean) field.getHopValue(0));
    assertTrue((Boolean) field.getHopValue(true));

    initField("Binary");
    byte[] data = new byte[] {'a', 'b', 'c'};
    assertThat(field.getHopValue(new Binary(data)), equalTo((Object) data));
    assertThat((byte[]) field.getHopValue(data), equalTo(data));
    assertThat(field.getHopValue("abc"), equalTo((Object) data));

    initField("Date");
    assertThat(field.getHopValue(date), equalTo((Object) date));
    assertThat(field.getHopValue(date.getTime()), equalTo((Object) date));
    try {
      field.getHopValue("Not a date value");
      fail("expected exception");
    } catch (Exception e) {
      assertThat(e, instanceOf(HopException.class));
    }

    initField("Integer");
    assertThat(field.getHopValue(123), equalTo((Object) 123l));
    assertThat(field.getHopValue("123"), equalTo((Object) 123l));
    assertThat(field.getHopValue(new Binary(new byte[] {'1', '2', '3'})), equalTo((Object) 123l));

    initField("String");
    assertThat(field.getHopValue("foo"), equalTo((Object) "foo"));
    assertThat(field.getHopValue(123), equalTo((Object) "123"));
  }

  @Test
  public void testConvertArrayIndicesToHopValue() throws HopException {
    BasicDBObject dbObj =
        (BasicDBObject) JSON.parse("{ parent : { fieldName : ['valA', 'valB'] } } ");

    initField("fieldName", "$.parent.fieldName[0]", "String");
    assertThat(field.convertToHopValue(dbObj), equalTo((Object) "valA"));
    initField("fieldName", "$.parent.fieldName[1]", "String");
    assertThat(field.convertToHopValue(dbObj), equalTo((Object) "valB"));
  }

  @Test
  public void testConvertUndefinedOrNullToHopValue() throws HopException {
    BasicDBObject dbObj = BasicDBObject.parse("{ test1 : undefined, test2 : null } ");
    initField("fieldName", "$.test1", "String");
    // PDI-16090S
    assertNull("Undefined should be interpreted as null ", field.convertToHopValue(dbObj));
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
