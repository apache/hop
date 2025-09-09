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

package org.apache.hop.pipeline.transforms.formula.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class FormulaFieldsExtractorTest {

  @Test
  void testSimpleFieldExtraction() {
    String formula = "[field1] + [field2]";
    List<String> fields = FormulaFieldsExtractor.getFormulaFieldList(formula);

    assertNotNull(fields);
    assertEquals(2, fields.size());
    assertTrue(fields.contains("field1"));
    assertTrue(fields.contains("field2"));
  }

  @Test
  void testSingleFieldExtraction() {
    String formula = "UPPER([name])";
    List<String> fields = FormulaFieldsExtractor.getFormulaFieldList(formula);

    assertNotNull(fields);
    assertEquals(1, fields.size());
    assertEquals("name", fields.get(0));
  }

  @Test
  void testNoFieldsInFormula() {
    String formula = "1 + 2 * 3";
    List<String> fields = FormulaFieldsExtractor.getFormulaFieldList(formula);

    assertNotNull(fields);
    assertTrue(fields.isEmpty());
  }

  @Test
  void testComplexFormula() {
    String formula = "IF([status] = \"active\", [amount] * [rate], 0)";
    List<String> fields = FormulaFieldsExtractor.getFormulaFieldList(formula);

    assertNotNull(fields);
    assertEquals(3, fields.size());
    assertTrue(fields.contains("status"));
    assertTrue(fields.contains("amount"));
    assertTrue(fields.contains("rate"));
  }

  @Test
  void testDuplicateFields() {
    String formula = "[value] + [value] * 2";
    List<String> fields = FormulaFieldsExtractor.getFormulaFieldList(formula);

    assertNotNull(fields);
    assertEquals(2, fields.size()); // Duplicates are included
    assertEquals("value", fields.get(0));
    assertEquals("value", fields.get(1));
  }

  @Test
  void testEmptyFormula() {
    String formula = "";
    List<String> fields = FormulaFieldsExtractor.getFormulaFieldList(formula);

    assertNotNull(fields);
    assertTrue(fields.isEmpty());
  }

  @Test
  void testFormulaWithSpaces() {
    String formula = "[ field with spaces ] + [another_field]";
    List<String> fields = FormulaFieldsExtractor.getFormulaFieldList(formula);

    assertNotNull(fields);
    assertEquals(2, fields.size());
    assertTrue(fields.contains(" field with spaces "));
    assertTrue(fields.contains("another_field"));
  }

  @Test
  void testUnmatchedBrackets() {
    String formula = "[incomplete + [complete]";
    List<String> fields = FormulaFieldsExtractor.getFormulaFieldList(formula);

    assertNotNull(fields);
    assertEquals(1, fields.size());
    assertEquals("incomplete + [complete", fields.get(0));
  }

  @Test
  void testEmptyBrackets() {
    String formula = "[] + [field]";
    List<String> fields = FormulaFieldsExtractor.getFormulaFieldList(formula);

    assertNotNull(fields);
    assertEquals(2, fields.size());
    assertEquals("", fields.get(0)); // Empty bracket is also captured
    assertEquals("field", fields.get(1));
  }

  @Test
  void testNestedFunctions() {
    String formula = "ROUND(SQRT([value1] + [value2]), 2)";
    List<String> fields = FormulaFieldsExtractor.getFormulaFieldList(formula);

    assertNotNull(fields);
    assertEquals(2, fields.size());
    assertTrue(fields.contains("value1"));
    assertTrue(fields.contains("value2"));
  }

  @Test
  void testSpecialCharactersInFieldNames() {
    String formula = "[field-with-dashes] + [field_with_underscores] + [field.with.dots]";
    List<String> fields = FormulaFieldsExtractor.getFormulaFieldList(formula);

    assertNotNull(fields);
    assertEquals(3, fields.size());
    assertTrue(fields.contains("field-with-dashes"));
    assertTrue(fields.contains("field_with_underscores"));
    assertTrue(fields.contains("field.with.dots"));
  }
}
