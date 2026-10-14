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

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.junit.jupiter.api.Test;

/**
 * @author Cedric Pronzato
 */
public class EvenFunctionTest extends FormulaTestBase {
  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"EVEN(6)", new BigDecimal(6)},
      {"EVEN(-4)", new BigDecimal(-4)},
      {"EVEN(1)", new BigDecimal(2)},
      {"EVEN(0.3)", new BigDecimal(2)},
      {"EVEN(-1)", new BigDecimal(-2)},
      {"EVEN(-0.3)", new BigDecimal(-2)},
      {"EVEN(0)", new BigDecimal(0)},

      // test with border cases
      {"EVEN(0.05)", new BigDecimal(2)},
      {"EVEN(2.05)", new BigDecimal(4)},
      {"EVEN(3.05)", new BigDecimal(4)},
      {"EVEN(4.05)", new BigDecimal(6)},
      {"EVEN(5.0)", new BigDecimal(6)},
      {"EVEN(6.0)", new BigDecimal(6)},
      {"EVEN(7.95)", new BigDecimal(8)},
      {"EVEN(8.95)", new BigDecimal(10)},
      {"EVEN(-0.05)", new BigDecimal(-2)},
      {"EVEN(-2.05)", new BigDecimal(-4)},
      {"EVEN(-3.05)", new BigDecimal(-4)},
      {"EVEN(-4.05)", new BigDecimal(-6)},
      {"EVEN(-5.0)", new BigDecimal(-6)},
      {"EVEN(-6.0)", new BigDecimal(-6)},
      {"EVEN(-7.95)", new BigDecimal(-8)},
      {"EVEN(-8.95)", new BigDecimal(-10)},
    };
  }
}
