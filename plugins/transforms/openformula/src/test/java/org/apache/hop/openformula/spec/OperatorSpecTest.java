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

package org.apache.hop.openformula.spec;

import org.apache.hop.openformula.DefaultFormulaContext;
import org.junit.jupiter.api.Test;

/** Part 2 sections 5 and 6.4, plus the pipeline field binding. */
class OperatorSpecTest {
  @Test
  void infixOperators() throws Exception {
    // 6.4.1 addition, 6.4.2 subtraction, 6.4.4 multiplication, 6.4.5 division
    Spec.number("1+2", "3");
    Spec.number("5-2", "3");
    Spec.number("2*3", "6");
    Spec.number("8/2", "4");
    // 6.4.6 power
    Spec.number("2^3", "8");
    // 6.4.9 percent
    Spec.number("50%", "0.5");
    // 6.4.10 concatenation
    Spec.text("\"a\"&\"b\"", "ab");
    // 6.4.7 equality and 6.4.8 inequality, 6.4.11 through 6.4.14 ordered comparison
    Spec.logical("2=2", true);
    Spec.logical("2<>3", true);
    Spec.logical("1<2", true);
    Spec.logical("2<=2", true);
    Spec.logical("3>1", true);
    Spec.logical("3>=3", true);
    // 6.4.15 unary minus
    Spec.number("-4", "-4");
  }

  /** Fields are bound with [name]. That is the pipeline binding, not a spreadsheet cell. */
  @Test
  void fieldReference() throws Exception {
    DefaultFormulaContext context = new DefaultFormulaContext();
    context.defineReference("qty", 2);
    context.defineReference("price", 4);
    Object actual = Spec.value("[qty]*[price]", context);
    org.junit.jupiter.api.Assertions.assertEquals(
        0, new java.math.BigDecimal("8").compareTo(new java.math.BigDecimal(actual.toString())));
  }
}
