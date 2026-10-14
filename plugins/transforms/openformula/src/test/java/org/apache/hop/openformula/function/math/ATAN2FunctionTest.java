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
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.apache.hop.openformula.util.NumberUtil;
import org.junit.jupiter.api.Test;

/**
 * @author Shiva Krishna Kurremula
 */
public class ATAN2FunctionTest extends FormulaTestBase {
  private FormulaContext context;

  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {
        "ATAN2(5;10)",
        NumberUtil.performTuneRounding(new BigDecimal(1.1071487177940904089723517245147377))
      },
      {
        "ATAN2(10;5)",
        NumberUtil.performTuneRounding(new BigDecimal(0.4636476090008060935154787784995278))
      },
      {
        "ATAN2(10;10)",
        NumberUtil.performTuneRounding(new BigDecimal(0.7853981633974482789994908671360463))
      },
      {
        "ATAN2(30;45)",
        NumberUtil.performTuneRounding(new BigDecimal(0.9827937232473290540823995797836687))
      },
      {
        "ATAN2(45;30)",
        NumberUtil.performTuneRounding(new BigDecimal(0.5880026035475675039165821544884238))
      },
      {
        "ATAN2(90;30)",
        NumberUtil.performTuneRounding(new BigDecimal(0.3217505543966421854840120886365184))
      },
      {
        "ATAN2(30;90)",
        NumberUtil.performTuneRounding(new BigDecimal(1.2490457723982544280261208768934011))
      },
      {
        "ATAN2(90;45)",
        NumberUtil.performTuneRounding(new BigDecimal(0.4636476090008060935154787784995278))
      },
      {
        "ATAN2(45;90)",
        NumberUtil.performTuneRounding(new BigDecimal(1.1071487177940904089723517245147377))
      },
      {
        "ATAN2(90;90)",
        NumberUtil.performTuneRounding(new BigDecimal(0.7853981633974482789994908671360462))
      },
    };
  }
}
