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

import java.math.BigDecimal;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.junit.jupiter.api.Test;

/**
 * @author Cedric Pronzato
 */
public class LeftFunctionTest extends FormulaTestBase {
  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"LEFT(\"Hello\";2)", "He"},
      {"LEFT(\"Hello\")", "H"},
      {"LEFT(\"Hello\";20)", "Hello"},
      {"LEFT(\"Hello\";0)", ""},
      {"LEFT(\"\";4)", ""},
      {"LEN(LEFT(\"xxx\";-0.1))", FormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE},
      {"LEN(LEFT(\"xxx\";2^15-1))", new BigDecimal(3)},
      {"LEFT(\"Hello\";2.9)", "He"},
    };
  }
}
