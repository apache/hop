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

package org.apache.hop.openformula.function.logical;

import java.math.BigDecimal;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.junit.jupiter.api.Test;

/**
 * @author Cedric Pronzato
 */
public class IfFunctionTest extends FormulaTestBase {
  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"IF(FALSE();7;8)", new BigDecimal(8)},
      {"IF(TRUE();7;8)", new BigDecimal(7)},
      {"IF(TRUE();\"HI\";8)", "HI"},
      {"IF(1;7;8)", new BigDecimal(7)},
      {"IF(5;7;8)", new BigDecimal(7)},
      {"IF(0;7;8)", new BigDecimal(8)},
      {"IF(TRUE();[.B4];8)", new BigDecimal(2)},
      {"IF(TRUE();[.B4]+5;8)", new BigDecimal(7)},
      {"IF(\"x\";7;8)", FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE},
      {"IF(\"1\";7;8)", FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE},
      {"IF(\"\";7;8)", FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE},
      {"IF(FALSE();7)", Boolean.FALSE},
      {"IF(FALSE();7;)", Boolean.FALSE},
      {"IF(FALSE();;7)", new BigDecimal(7)},
      // TODO { "IF(FALSE();7;)", new BigDecimal(0) }, we will not allow this syntax
      {"IF(TRUE();4;1/0)", new BigDecimal(4)},
      {"IF(FALSE();1/0;5)", new BigDecimal(5)},
    };
  }
}
