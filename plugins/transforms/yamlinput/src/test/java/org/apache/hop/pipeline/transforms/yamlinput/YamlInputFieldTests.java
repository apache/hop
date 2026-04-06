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

package org.apache.hop.pipeline.transforms.yamlinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.apache.hop.core.row.IValueMeta;
import org.junit.jupiter.api.Test;

/** Unit test for {@link YamlInputField} */
class YamlInputFieldTests {

  @Test
  void testDefaultConstructor() {
    YamlInputField field = new YamlInputField();

    assertEquals("", field.getName());
    assertEquals("", field.getPath());
    assertEquals(-1, field.getLength());
    assertEquals(-1, field.getPrecision());
    assertEquals("", field.getFormat());
    assertEquals("", field.getGroupSymbol());
    assertEquals("", field.getDecimalSymbol());
    assertEquals("", field.getCurrencySymbol());

    assertEquals(IValueMeta.TYPE_STRING, field.getType());
    assertEquals(YamlInputField.TYPE_TRIM_NONE, field.getTrimType());
  }

  @Test
  void testConstructorWithName() {
    YamlInputField field = new YamlInputField("testField");

    assertEquals("testField", field.getName());
  }

  @Test
  void testCopyConstructor() {
    YamlInputField original = new YamlInputField("name");
    original.setPath("$.data.name");
    original.setType(IValueMeta.TYPE_INTEGER);
    original.setLength(10);
    original.setFormat("###");
    original.setTrimType(YamlInputField.TYPE_TRIM_BOTH);
    original.setPrecision(2);
    original.setCurrencySymbol("$");
    original.setDecimalSymbol(".");
    original.setGroupSymbol(",");

    YamlInputField copy = new YamlInputField(original);

    assertEquals(original.getName(), copy.getName());
    assertEquals(original.getPath(), copy.getPath());
    assertEquals(original.getType(), copy.getType());
    assertEquals(original.getLength(), copy.getLength());
    assertEquals(original.getFormat(), copy.getFormat());
    assertEquals(original.getTrimType(), copy.getTrimType());
    assertEquals(original.getPrecision(), copy.getPrecision());
    assertEquals(original.getCurrencySymbol(), copy.getCurrencySymbol());
    assertEquals(original.getDecimalSymbol(), copy.getDecimalSymbol());
    assertEquals(original.getGroupSymbol(), copy.getGroupSymbol());
  }

  @Test
  void testClone() {
    YamlInputField original = new YamlInputField("cloneField");
    original.setPath("$.path");

    YamlInputField cloned = (YamlInputField) original.clone();

    assertNotSame(original, cloned);
    assertEquals(original.getName(), cloned.getName());
    assertEquals(original.getPath(), cloned.getPath());
  }

  @Test
  void testTrimTypeDesc() {
    YamlInputField field = new YamlInputField();
    field.setTrimType(YamlInputField.TYPE_TRIM_BOTH);

    String desc = field.getTrimTypeDesc();
    assertNotNull(desc);
  }

  @Test
  void testStaticTrimTypeMethods() {
    String desc = YamlInputField.getTrimTypeDesc(YamlInputField.TYPE_TRIM_LEFT);
    assertNotNull(desc);

    int type = YamlInputField.getTrimTypeByDesc(desc);
    assertEquals(YamlInputField.TYPE_TRIM_LEFT, type);
  }

  @Test
  void testGetTypeDesc() {
    YamlInputField field = new YamlInputField();
    field.setType(IValueMeta.TYPE_INTEGER);

    String desc = field.getTypeDesc();
    assertNotNull(desc);
  }
}
