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

package org.apache.hop.openformula.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

public class NumberUtilTest {

  @Test
  public void testPerformMinuteRounding() {
    BigDecimal[][] test = {
      // =MINUTE(timevalue("00:00:59"))
      new BigDecimal[] {
        new BigDecimal(0.9833333333333333333333333333333333333760), new BigDecimal(0)
      },
      // =MINUTE(timevalue("00:01:00")) =MINUTE("00:01:00") =MINUTE(1/(24*60))
      new BigDecimal[] {
        new BigDecimal(0.9999999999999999999999999999999999999360), new BigDecimal(1)
      },
      // =MINUTE(timevalue("00:01:59"))
      new BigDecimal[] {
        new BigDecimal(1.9833333333333333333333333333333333333120), new BigDecimal(1)
      },
      // =MINUTE(timevalue("00:07:00"))
      new BigDecimal[] {
        new BigDecimal(6.9999999999999999999999999999999999999840), new BigDecimal(7)
      },
      // =MINUTE(timevalue("00:09:00"))
      new BigDecimal[] {
        new BigDecimal(9.0000000000000000000000000000000000000000), new BigDecimal(9)
      },
      // =MINUTE(timevalue("00:11:00"))
      new BigDecimal[] {
        new BigDecimal(11.9833333333333333333333333333333333333920), new BigDecimal(11)
      },
      // =MINUTE(timevalue("00:23:00"))
      new BigDecimal[] {
        new BigDecimal(22.9999999999999999999999999999999999999680), new BigDecimal(23)
      },
      // =MINUTE(timevalue("00:28:59"))
      new BigDecimal[] {
        new BigDecimal(28.9833333333333333333333333333333333333120), new BigDecimal(28)
      }
    };

    for (BigDecimal[] bigDecimal : test) {
      BigDecimal result = NumberUtil.performMinuteRounding(bigDecimal[0]);
      assertEquals(bigDecimal[1], result);
    }
  }
}
