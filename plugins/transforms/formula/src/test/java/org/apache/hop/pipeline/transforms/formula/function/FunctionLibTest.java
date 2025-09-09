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

package org.apache.hop.pipeline.transforms.formula.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopXmlException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FunctionLibTest {

  private FunctionLib functionLib;

  @BeforeEach
  void setUp() throws HopXmlException {
    functionLib =
        new FunctionLib("/org/apache/hop/pipeline/transforms/formula/function/functions.xml");
  }

  @Test
  void testConstructor() {
    assertNotNull(functionLib);
    assertNotNull(functionLib.getFunctions());
  }

  @Test
  void testGetFunctions() {
    List<FunctionDescription> functions = functionLib.getFunctions();
    assertNotNull(functions);
    assertFalse(functions.isEmpty());

    // Verify we have some common functions
    boolean hasAbs = functions.stream().anyMatch(f -> "ABS".equals(f.getName()));
    boolean hasSum = functions.stream().anyMatch(f -> "SUM".equals(f.getName()));
    assertTrue(hasAbs || hasSum, "Should contain at least one common function like ABS or SUM");
  }

  @Test
  void testGetFunctionNames() {
    String[] functionNames = functionLib.getFunctionNames();
    assertNotNull(functionNames);
    assertTrue(functionNames.length > 0);

    // Function names should be sorted
    for (int i = 1; i < functionNames.length; i++) {
      assertTrue(
          functionNames[i - 1].compareTo(functionNames[i]) <= 0,
          "Function names should be sorted alphabetically");
    }
  }

  @Test
  void testGetFunctionCategories() {
    String[] categories = functionLib.getFunctionCategories();
    assertNotNull(categories);
    assertTrue(categories.length > 0);

    // Categories should be sorted
    for (int i = 1; i < categories.length; i++) {
      assertTrue(
          categories[i - 1].compareTo(categories[i]) <= 0,
          "Categories should be sorted alphabetically");
    }
  }

  @Test
  void testGetFunctionsForACategory() {
    String[] categories = functionLib.getFunctionCategories();
    if (categories.length > 0) {
      String firstCategory = categories[0];
      String[] functions = functionLib.getFunctionsForACategory(firstCategory);
      assertNotNull(functions);

      for (String functionName : functions) {
        FunctionDescription desc = functionLib.getFunctionDescription(functionName);
        assertEquals(firstCategory, desc.getCategory());
      }
    }
  }

  @Test
  void testGetFunctionsForNonExistentCategory() {
    String[] functions = functionLib.getFunctionsForACategory("NON_EXISTENT_CATEGORY");
    assertNotNull(functions);
    assertEquals(0, functions.length);
  }

  @Test
  void testGetFunctionDescription() {
    String[] functionNames = functionLib.getFunctionNames();
    if (functionNames.length > 0) {
      String firstFunction = functionNames[0];
      FunctionDescription desc = functionLib.getFunctionDescription(firstFunction);
      assertNotNull(desc);
      assertEquals(firstFunction, desc.getName());
    }
  }

  @Test
  void testGetFunctionDescriptionNotFound() {
    FunctionDescription desc = functionLib.getFunctionDescription("NON_EXISTENT_FUNCTION");
    assertNull(desc);
  }

  @Test
  void testInvalidXmlFile() {
    assertThrows(
        HopXmlException.class,
        () -> {
          new FunctionLib("/non_existent_file.xml");
        });
  }

  @Test
  void testFunctionDescriptionProperties() {
    List<FunctionDescription> functions = functionLib.getFunctions();
    if (!functions.isEmpty()) {
      FunctionDescription firstFunction = functions.get(0);
      assertNotNull(firstFunction.getName());
      assertNotNull(firstFunction.getCategory());
      // Description can be null/empty for some functions
    }
  }

  @Test
  void testSetFunctions() {
    List<FunctionDescription> originalFunctions = functionLib.getFunctions();
    int originalSize = originalFunctions.size();

    // Test setter
    functionLib.setFunctions(originalFunctions);
    assertEquals(originalSize, functionLib.getFunctions().size());
  }
}
