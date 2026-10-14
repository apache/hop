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
 * @author Cedric Pronzato
 */
public class MidFunctionTest extends FormulaTestBase {
  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"MID(\"123456789\";5;3)", "567"},
      {"MID(\"123456789\";20;3)", ""},
      {"MID(\"123456789\";-1;0)", FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE},
      {"MID(\"123456789\";1;0)", ""},
      {"MID(\"123456789\";2.9;1)", "2"},
      {"MID(\"123456789\";2;2.9)", "23"},

      // custom tests
      {"MID(\"123456789\";5;10)", "56789"},
      {"MID(\"123456789\";1;9)", "123456789"},
      {"MID(\"text\";2;2)", "ex"},
      {"MID(123456789;\"3\";4)", "3456"},
    };
  }
}
