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

package org.apache.hop.openformula.function.math;

import java.math.BigDecimal;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.junit.jupiter.api.Test;

/**
 * @author Cedric Pronzato
 */
public class OddFunctionTest extends FormulaTestBase {
  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"ODD(5)", new BigDecimal(5)},
      {"ODD(-5)", new BigDecimal(-5)},
      {"ODD(2)", new BigDecimal(3)},
      {"ODD(0.3)", new BigDecimal(1)},
      {"ODD(-2)", new BigDecimal(-3)},
      {"ODD(-0.3)", new BigDecimal(-1)},
      {"ODD(0)", new BigDecimal(1)},
      {"ODD(0.0)", new BigDecimal(1)},
      {"ODD(0.05)", new BigDecimal(1)},
      {"ODD(0.95)", new BigDecimal(1)},
      {"ODD(1.0)", new BigDecimal(1)},
      {"ODD(1.05)", new BigDecimal(3)},
      {"ODD(1.9)", new BigDecimal(3)},
      {"ODD(2.0)", new BigDecimal(3)},
      {"ODD(2.05)", new BigDecimal(3)},
      {"ODD(2.95)", new BigDecimal(3)},
      {"ODD(3.0)", new BigDecimal(3)},
      {"ODD(3.05)", new BigDecimal(5)},
      {"ODD(-0.05)", new BigDecimal(-1)},
      {"ODD(-0.95)", new BigDecimal(-1)},
      {"ODD(-1.0)", new BigDecimal(-1)},
      {"ODD(-1.05)", new BigDecimal(-3)},
      {"ODD(-1.9)", new BigDecimal(-3)},
      {"ODD(-2.0)", new BigDecimal(-3)},
      {"ODD(-2.05)", new BigDecimal(-3)},
      {"ODD(-2.95)", new BigDecimal(-3)},
      {"ODD(-3.0)", new BigDecimal(-3)},
      {"ODD(-3.05)", new BigDecimal(-5)},
    };
  }
}
