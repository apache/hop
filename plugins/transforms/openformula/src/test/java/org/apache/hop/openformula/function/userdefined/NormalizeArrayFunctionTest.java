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

package org.apache.hop.openformula.function.userdefined;

import java.math.BigDecimal;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.junit.jupiter.api.Test;

public class NormalizeArrayFunctionTest extends FormulaTestBase {
  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {
        "NORMALIZEARRAY({11 | 21 | [.B18] | [.C19]})",
        new Object[] {
          new BigDecimal(11),
          new BigDecimal(21),
          new BigDecimal(1),
          new BigDecimal(2),
          new BigDecimal(3),
          new BigDecimal(42),
          new BigDecimal(43)
        }
      },
      {
        "NORMALIZEARRAY({11 | 21 | [.B18] | [.B19]})",
        new Object[] {
          new BigDecimal(11),
          new BigDecimal(21),
          new BigDecimal(1),
          new BigDecimal(2),
          new BigDecimal(3)
        }
      },
      {
        "NORMALIZEARRAY({11 | 21 | [.B18]})",
        new Object[] {
          new BigDecimal(11),
          new BigDecimal(21),
          new BigDecimal(1),
          new BigDecimal(2),
          new BigDecimal(3)
        }
      },
    };
  }
}
