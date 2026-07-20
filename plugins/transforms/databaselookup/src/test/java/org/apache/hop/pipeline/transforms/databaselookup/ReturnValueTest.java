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

package org.apache.hop.pipeline.transforms.databaselookup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

class ReturnValueTest {

  @Test
  void defaultConstructor_LeavesFieldsNull() {
    ReturnValue value = new ReturnValue();
    assertNull(value.getTableField());
    assertNull(value.getNewName());
    assertNull(value.getDefaultValue());
    assertNull(value.getDefaultType());
    assertNull(value.getTrimType());
  }

  @Test
  void allArgsConstructor_SetsFields() {
    String trim = ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_BOTH);
    ReturnValue value = new ReturnValue("col", "rename", "?", "String", trim);

    assertEquals("col", value.getTableField());
    assertEquals("rename", value.getNewName());
    assertEquals("?", value.getDefaultValue());
    assertEquals("String", value.getDefaultType());
    assertEquals(trim, value.getTrimType());
  }

  @Test
  void copyConstructor_CopiesAllFieldsIndependently() {
    ReturnValue original =
        new ReturnValue(
            "col",
            "rename",
            "?",
            "String",
            ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_BOTH));
    ReturnValue copy = new ReturnValue(original);

    assertNotSame(original, copy);
    assertEquals(original.getTableField(), copy.getTableField());
    assertEquals(original.getNewName(), copy.getNewName());
    assertEquals(original.getDefaultValue(), copy.getDefaultValue());
    assertEquals(original.getDefaultType(), copy.getDefaultType());
    assertEquals(original.getTrimType(), copy.getTrimType());

    copy.setTableField("t2");
    copy.setNewName("other");
    copy.setDefaultValue("x");
    copy.setDefaultType("Integer");
    copy.setTrimType(ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_LEFT));
    assertEquals("col", original.getTableField());
    assertEquals("rename", original.getNewName());
    assertEquals("?", original.getDefaultValue());
    assertEquals("String", original.getDefaultType());
    assertEquals(
        ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_BOTH), original.getTrimType());
  }

  @Test
  void settersAndGetters() {
    ReturnValue value = new ReturnValue();
    value.setTableField("t");
    value.setNewName("n");
    value.setDefaultValue("d");
    value.setDefaultType("Integer");
    value.setTrimType(ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_LEFT));

    assertEquals("t", value.getTableField());
    assertEquals("n", value.getNewName());
    assertEquals("d", value.getDefaultValue());
    assertEquals("Integer", value.getDefaultType());
    assertEquals(ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_LEFT), value.getTrimType());
  }
}
