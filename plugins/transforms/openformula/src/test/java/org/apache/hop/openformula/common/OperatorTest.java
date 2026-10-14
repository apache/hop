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

package org.apache.hop.openformula.common;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import org.apache.hop.openformula.EvaluationException;
import org.junit.jupiter.api.Test;

public class OperatorTest extends FormulaTestBase {
  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"1 + 1", new BigDecimal(2)},
      {"1 - 1", new BigDecimal(0)},
      {"1 = 1", Boolean.TRUE},
      {"1 <> 1", Boolean.FALSE},
      {"1 < 1", Boolean.FALSE},
      {"1 > 1", Boolean.FALSE},
      {"1 >= 1", Boolean.TRUE},
      {"1 <= 1", Boolean.TRUE},
      {"1 * 1", new BigDecimal(1)},
      {"1 ^ 1", new BigDecimal(1.0)},
      {"1 / 1", new BigDecimal(1)},
      {"1%", new BigDecimal("0.01")},
      {"0.1%", new BigDecimal("0.001")}
    };
  }

  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Test
  public void testRangeOperator() throws EvaluationException {
    final Object resolveReference = getContext().resolveReference(".B4:.B5");
    assertNotNull(resolveReference, "Reference should not be null");
  }
}
