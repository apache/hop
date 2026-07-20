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

import org.junit.jupiter.api.Test;

class KeyFieldTest {

  @Test
  void defaultConstructor_LeavesFieldsNull() {
    KeyField key = new KeyField();
    assertNull(key.getStreamField1());
    assertNull(key.getStreamField2());
    assertNull(key.getCondition());
    assertNull(key.getTableField());
  }

  @Test
  void allArgsConstructor_SetsFields() {
    KeyField key = new KeyField("stream1", "stream2", "=", "table_col");
    assertEquals("stream1", key.getStreamField1());
    assertEquals("stream2", key.getStreamField2());
    assertEquals("=", key.getCondition());
    assertEquals("table_col", key.getTableField());
  }

  @Test
  void copyConstructor_CopiesAllFieldsIndependently() {
    KeyField original = new KeyField("stream1", "stream2", "=", "table_col");
    KeyField copy = new KeyField(original);

    assertNotSame(original, copy);
    assertEquals(original.getStreamField1(), copy.getStreamField1());
    assertEquals(original.getStreamField2(), copy.getStreamField2());
    assertEquals(original.getCondition(), copy.getCondition());
    assertEquals(original.getTableField(), copy.getTableField());

    copy.setStreamField1("changed");
    copy.setStreamField2("changed2");
    copy.setCondition("<>");
    copy.setTableField("other");
    assertEquals("stream1", original.getStreamField1());
    assertEquals("stream2", original.getStreamField2());
    assertEquals("=", original.getCondition());
    assertEquals("table_col", original.getTableField());
  }

  @Test
  void settersAndGetters() {
    KeyField key = new KeyField();
    key.setStreamField1("a");
    key.setStreamField2("b");
    key.setCondition("LIKE");
    key.setTableField("c");

    assertEquals("a", key.getStreamField1());
    assertEquals("b", key.getStreamField2());
    assertEquals("LIKE", key.getCondition());
    assertEquals("c", key.getTableField());
  }
}
