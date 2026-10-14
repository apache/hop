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

package org.apache.hop.openformula.function.datetime;

import java.math.BigDecimal;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.junit.jupiter.api.Test;

/**
 * @author Cedric Pronzato
 */
public class TimeFunctionTest extends FormulaTestBase {
  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"0.9999884259259259259259259259259259259*27", new BigDecimal("26.9996875000000000000")},
      {"TIME(0;0;0)+0", new BigDecimal(0)},
      {"TIME(23;59;59)*60*60*24", new BigDecimal(86399)},
      /*{ "", Boolean.TRUE },
              { "", Boolean.TRUE },
              { "", Boolean.TRUE },
              { "", Boolean.TRUE },
              { "", Boolean.TRUE },
              { "", Boolean.TRUE },
              { "", Boolean.TRUE },
              { "", Boolean.TRUE },
              { "", Boolean.TRUE },
      */
    };
  }
}
