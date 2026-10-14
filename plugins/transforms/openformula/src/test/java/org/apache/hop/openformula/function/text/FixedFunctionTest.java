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

public class FixedFunctionTest extends FormulaTestBase {

  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"FIXED(1234.567; 1)", "1,234.6"},
      {"FIXED(1234.567; -1)", "1,230"},
      {"FIXED(-1234.567; -1; FALSE())", "-1,230"},
      {"FIXED(1234.567; -1; TRUE())", "1230"},
      {"FIXED(1234.567)", "1,234.57"},
      {"FIXED(1234.567; 2; TRUE())", "1234.57"},
      {"FIXED(1234.567; 0; TRUE())", "1235"},
    };
  }

  @Test
  public void testWrongParameterNumber_Zero() throws Exception {
    performTest("FIXED()", FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
  }

  @Test
  public void testWrongParameterNumber_Four() throws Exception {
    performTest("FIXED(1; 2; TRUE(); 4)", FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
  }

  @Test
  public void testWrongParameterType_One() throws Exception {
    performTest("FIXED(\"error\"; 1)", FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
  }

  @Test
  public void testWrongParameterType_Two() throws Exception {
    performTest("FIXED(1; \"error\")", FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
  }

  @Test
  public void testWrongParameterType_Three() throws Exception {
    performTest("FIXED(1; 2; \"error\")", FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
  }
}
