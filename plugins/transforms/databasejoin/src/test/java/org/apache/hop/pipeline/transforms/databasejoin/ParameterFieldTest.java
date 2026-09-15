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

package org.apache.hop.pipeline.transforms.databasejoin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Unit test for {@link ParameterField} */
class ParameterFieldTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void defaultTypeIsNone() {
    ParameterField field = new ParameterField();
    assertNull(field.getName());
    assertEquals(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_NONE), field.getType());
  }

  @Test
  void setNameStripsBlankToNull() {
    ParameterField field = new ParameterField();
    field.setName("  id  ");
    assertEquals("id", field.getName());
    field.setName("   ");
    assertNull(field.getName());
    field.setName(null);
    assertNull(field.getName());
  }

  @Test
  void setTypeAcceptsNameAndId() {
    ParameterField field = new ParameterField();
    field.setType("Integer");
    assertEquals("Integer", field.getType());
    field.setType(IValueMeta.TYPE_STRING);
    assertEquals(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING), field.getType());
  }

  @Test
  void cloneCopiesValuesIndependently() {
    ParameterField original = new ParameterField();
    original.setName("id");
    original.setType(IValueMeta.TYPE_INTEGER);

    ParameterField copy = (ParameterField) original.clone();
    assertNotSame(original, copy);
    assertEquals(original.getName(), copy.getName());
    assertEquals(original.getType(), copy.getType());

    copy.setName("other");
    copy.setType(IValueMeta.TYPE_STRING);
    assertEquals("id", original.getName());
    assertEquals(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_INTEGER), original.getType());
  }

  @Test
  void copyConstructorCopiesValues() {
    ParameterField original = new ParameterField();
    original.setName("code");
    original.setType("String");
    ParameterField copy = new ParameterField(original);
    assertEquals("code", copy.getName());
    assertEquals("String", copy.getType());
  }

  @Test
  void toStringIncludesNameAndType() {
    ParameterField field = new ParameterField();
    field.setName("id");
    field.setType("Integer");
    assertEquals("id:Integer", field.toString());
  }
}
