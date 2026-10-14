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

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.junit.jupiter.api.Test;

/**
 * @author Cedric Pronzato
 */
public class SearchFunctionTest extends FormulaTestBase {
  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"SEARCH(\"b\";\"abcabc\")", new BigDecimal(2)},
      {"SEARCH(\"b\";\"abcabcabc\"; 3)", new BigDecimal(5)},
      {"SEARCH(\"d\";\"ABC\";1)", FormulaErrorValue.ERROR_NOT_FOUND_VALUE},
      {"SEARCH(\"b\";\"ABC\";1)", new BigDecimal(2)},
      {"SEARCH(\"c?a\";\"abcabcda\")", new BigDecimal(6)},
      {"SEARCH(\"e*o\";\"yes and no\")", new BigDecimal(2)},
      {"SEARCH(\"b*c\";\"abcabcabc\")", new BigDecimal(2)},
    };
  }
}
