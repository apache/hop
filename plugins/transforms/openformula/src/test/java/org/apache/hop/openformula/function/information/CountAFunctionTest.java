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

package org.apache.hop.openformula.function.information;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.junit.jupiter.api.Test;

/**
 * @author Cedric Pronzato
 */
public class CountAFunctionTest extends FormulaTestBase {
  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"COUNTA(\"1\";2;TRUE())", new BigDecimal(3)},
      {"COUNTA([.B3:.B5])", new BigDecimal(3)},
      {"COUNTA([.B3:.B5];[.B3:.B5])", new BigDecimal(6)},
      {"COUNTA([.B3:.B9])", new BigDecimal(6)},
      {"COUNTA(\"1\";2;1/0)", new BigDecimal(3)},
      {"COUNTA(\"1\";2;SUM([.B3:.B9]))", new BigDecimal(3)},
      {"COUNTA(\"1\";2;[.B3:.B9])", new BigDecimal(8)},
      {"COUNTA({\"1\";2;[.B3:.B9]})", new BigDecimal(8)},
      {"COUNTA({[.B3:.B9]})", new BigDecimal(6)},
    };
  }
}
