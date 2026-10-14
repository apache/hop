/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.openformula.function.text;

import org.apache.hop.openformula.common.FormulaTestBase;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.junit.jupiter.api.Test;

public class DollarFunctionTest extends FormulaTestBase {

  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"DOLLAR(102.35;2)", "$102.35"},
      {"DOLLAR(102.354;2)", "$102.35"},
      {"DOLLAR(102.357;2)", "$102.36"},
      {"DOLLAR(102.354;3)", "$102.354"},
      {"DOLLAR(102.354;4)", "$102.3540"},
      {"DOLLAR(102.354)", "$102.35"},
      {"DOLLAR(102.357)", "$102.36"},
      {"DOLLAR(102)", "$102.00"},
      {"DOLLAR(0)", "$0.00"},
      {"DOLLAR(\"102.354\";3)", "$102.354"},
      {"DOLLAR(102.354;\"4\")", "$102.3540"},
    };
  }

  @Test
  public void testWrongParameterNumber_Zero() throws Exception {
    performTest("DOLLAR()", FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
  }

  @Test
  public void testWrongParameterNumber_Three() throws Exception {
    performTest("DOLLAR(1; 0; 10)", FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
  }

  @Test
  public void testWrongParameterType_One() throws Exception {
    performTest("DOLLAR(\"error\"; 1)", FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
  }

  @Test
  public void testWrongParameterType_Two() throws Exception {
    performTest("DOLLAR(10; \"error\")", FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
  }
}
