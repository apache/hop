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
package org.apache.hop.pipeline.transforms.janino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.row.IValueMeta;
import org.junit.jupiter.api.Test;

class JaninoMetaFunctionTest {

  private static JaninoMetaFunction build(String fieldName) {
    JaninoMetaFunction f = new JaninoMetaFunction();
    f.setFieldName(fieldName);
    f.setFormula("1+1");
    f.setValueType(IValueMeta.TYPE_INTEGER);
    f.setValueLength(9);
    f.setValuePrecision(0);
    f.setReplaceField("rep");
    return f;
  }

  // ------------------------------------------------------------------ equals

  @Test
  void equals_sameFieldName_returnsTrue() {
    JaninoMetaFunction a = build("x");
    JaninoMetaFunction b = build("x");
    assertTrue(a.equals(b));
  }

  @Test
  void equals_differentFieldName_returnsFalse() {
    assertFalse(build("a").equals(build("b")));
  }

  @Test
  void equals_null_returnsFalse() {
    assertFalse(build("x").equals(null));
  }

  @Test
  void equals_differentClass_returnsFalse() {
    assertFalse(build("x").equals("x"));
  }

  @Test
  void equals_reflexive() {
    JaninoMetaFunction f = build("y");
    assertTrue(f.equals(f));
  }

  // ------------------------------------------------------------------ hashCode

  @Test
  void hashCode_equalObjects_sameHashCode() {
    assertEquals(build("x").hashCode(), build("x").hashCode());
  }

  // ------------------------------------------------------------------ clone / copy constructor

  @Test
  void copyConstructor_copiesAllFields() {
    JaninoMetaFunction original = build("myField");
    JaninoMetaFunction copy = new JaninoMetaFunction(original);

    assertNotSame(original, copy);
    assertEquals("myField", copy.getFieldName());
    assertEquals("1+1", copy.getFormula());
    assertEquals(IValueMeta.TYPE_INTEGER, copy.getValueType());
    assertEquals(9, copy.getValueLength());
    assertEquals(0, copy.getValuePrecision());
    assertEquals("rep", copy.getReplaceField());
  }

  @Test
  void clone_returnsEqualButDistinctInstance() {
    JaninoMetaFunction original = build("z");
    JaninoMetaFunction cloned = (JaninoMetaFunction) original.clone();

    assertNotNull(cloned);
    assertNotSame(original, cloned);
    assertEquals(original.getFieldName(), cloned.getFieldName());
    assertEquals(original.getFormula(), cloned.getFormula());
  }

  // ------------------------------------------------------------------ default constructor

  @Test
  void defaultConstructor_fieldsAreNullOrZero() {
    JaninoMetaFunction f = new JaninoMetaFunction();
    assertFalse(f.equals(null)); // doesn't throw
    assertEquals(0, f.getValueType());
    assertEquals(0, f.getValueLength());
    assertEquals(0, f.getValuePrecision());
  }
}
