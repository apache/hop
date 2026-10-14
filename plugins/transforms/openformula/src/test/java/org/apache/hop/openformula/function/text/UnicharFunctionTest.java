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

/**
 * @author Pawan Ponugupati
 */
public class UnicharFunctionTest extends FormulaTestBase {
  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"UNICHAR(134071)", "\uD842\uDFB7"},
      {"UNICHAR(3114)", "ప"},
      {"UNICHAR(2346)", "प"},
      {"UNICHAR(2999)", "ஷ"},
      {"UNICHAR(65)", "A"},
      {"UNICHAR(97)", "a"},
      {"UNICHAR(198)", "Æ"},
      {"UNICHAR(1)", "\u0001"},

      // Edge Cases
      // Max Value of Unicode is 1114111 and Min Value is 1.
      {"UNICHAR(1114111)", "\uDBFF\uDFFF"},
      {"UNICHAR(-1)", new FormulaErrorValue(502)},
      {"UNICHAR(987654321)", new FormulaErrorValue(502)},
      {"UNICHAR(0)", new FormulaErrorValue(502)},
    };
  }
}
