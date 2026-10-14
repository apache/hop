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

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.junit.jupiter.api.Test;

/**
 * @author Cedric Pronzato
 */
public class MinuteFunctionTest extends FormulaTestBase {
  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {
        "MINUTE(1/(24*60))", new BigDecimal(1)
      }, // Changed as a result of PRD-5734 - should be the same as =MINUTE("00:01:00")
      {
        "MINUTE(TODAY()+1/(24*60))", new BigDecimal(1)
      }, // Changed as a result of PRD-5734 - should be the same as =MINUTE("00:01:00")
      {"MINUTE(1/24)", new BigDecimal(0)},
      {"MINUTE(TIME(11;37;05))", new BigDecimal(37)},
      {"MINUTE(TIME(11;37;52))", new BigDecimal(37)}, // No rounding ... PRD-5499
      {"MINUTE(TIME(00;00;59))", new BigDecimal(0)},
      {"MINUTE(TIME(00;01;00))", new BigDecimal(1)},
      {"MINUTE(TIME(00;01;59))", new BigDecimal(1)},
      {"MINUTE(\"00:00:59\")", new BigDecimal(0)},
      {"MINUTE(\"00:01:00\")", new BigDecimal(1)},
      {"MINUTE(\"00:01:59\")", new BigDecimal(1)},
      {"MINUTE(\"00:29:59\")", new BigDecimal(29)},
      {"MINUTE(\"00:30:00\")", new BigDecimal(30)},
      {"MINUTE(\"00:30:59\")", new BigDecimal(30)},
      {"MINUTE(TIMEVALUE(\"00:00:59\"))", new BigDecimal(0)},
      {"MINUTE(TIMEVALUE(\"00:01:00\"))", new BigDecimal(1)},
      {"MINUTE(TIMEVALUE(\"00:01:59\"))", new BigDecimal(1)},
      {"MINUTE(TIMEVALUE(\"00:29:59\"))", new BigDecimal(29)},
      {"MINUTE(TIMEVALUE(\"00:30:00\"))", new BigDecimal(30)},
      {"MINUTE(TIMEVALUE(\"00:30:59\"))", new BigDecimal(30)},
      {"MINUTE(15/24/60/60+timevalue(\"00:30:00\"))", new BigDecimal(30)}
    };
  }

  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }
}
