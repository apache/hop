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

package org.apache.hop.pipeline.transforms.formula;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.row.value.ValueMetaFactory;
import org.junit.jupiter.api.Test;

class FormulaMetaFunctionTest {

  @Test
  void testDefaultConstructor() {
    FormulaMetaFunction function = new FormulaMetaFunction();
    assertEquals(-1, function.getValueLength());
    assertEquals(-1, function.getValuePrecision());
    assertFalse(function.isNeedDataConversion());
  }

  @Test
  void testParameterizedConstructor() {
    FormulaMetaFunction function =
        new FormulaMetaFunction(
            "test_field",
            "1+1",
            ValueMetaFactory.getIdForValueMeta("Number"),
            10,
            2,
            "replace_field",
            true);

    assertEquals("test_field", function.getFieldName());
    assertEquals("1+1", function.getFormula());
    assertEquals(ValueMetaFactory.getIdForValueMeta("Number"), function.getValueType());
    assertEquals(10, function.getValueLength());
    assertEquals(2, function.getValuePrecision());
    assertEquals("replace_field", function.getReplaceField());
    assertTrue(function.isSetNa());
  }

  @Test
  void testGettersAndSetters() {
    FormulaMetaFunction function = new FormulaMetaFunction();

    function.setFieldName("my_field");
    assertEquals("my_field", function.getFieldName());

    function.setFormula("2*3");
    assertEquals("2*3", function.getFormula());

    function.setValueType(ValueMetaFactory.getIdForValueMeta("String"));
    assertEquals(ValueMetaFactory.getIdForValueMeta("String"), function.getValueType());

    function.setValueLength(50);
    assertEquals(50, function.getValueLength());

    function.setValuePrecision(3);
    assertEquals(3, function.getValuePrecision());

    function.setReplaceField("old_field");
    assertEquals("old_field", function.getReplaceField());

    function.setSetNa(true);
    assertTrue(function.isSetNa());

    function.setNeedDataConversion(true);
    assertTrue(function.isNeedDataConversion());
  }

  @Test
  void testHashCode() {
    FormulaMetaFunction function1 =
        new FormulaMetaFunction("field1", "formula1", 1, 10, 2, "replace1", false);
    FormulaMetaFunction function2 =
        new FormulaMetaFunction("field1", "formula1", 1, 10, 2, "replace1", false);
    FormulaMetaFunction function3 =
        new FormulaMetaFunction("field2", "formula2", 2, 20, 3, "replace2", true);

    assertEquals(function1.hashCode(), function2.hashCode());
    assertNotEquals(function1.hashCode(), function3.hashCode());
  }

  @Test
  void testXmlTag() {
    assertEquals("formula", FormulaMetaFunction.XML_TAG);
  }
}
