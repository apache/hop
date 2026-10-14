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
public class SecondFunctionTest extends FormulaTestBase {
  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"SECOND(1/(24*60*60))", new BigDecimal(1)},
      // TODO this testcase seems to be wrong as the definition is: round to the nearest integer
      // which should be 0
      // {"SECOND(1/(24*60*60*2))", new BigDecimal(1)},
      {"SECOND(1/(24*60*60*4))", new BigDecimal(0)},
    };
  }

  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }
}
