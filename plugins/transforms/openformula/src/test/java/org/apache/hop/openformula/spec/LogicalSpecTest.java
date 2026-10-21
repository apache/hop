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

import org.apache.hop.openformula.error.FormulaErrorValue;
import org.junit.jupiter.api.Test;

/** Part 2 section 6.15. */
class LogicalSpecTest {
  @Test
  void logicalFunctions() throws Exception {
    // 6.15.2 AND is true only when every parameter is true.
    Spec.logical("AND(TRUE();TRUE())", true);
    Spec.logical("AND(TRUE();FALSE())", false);
    // 6.15.8 OR is false only when every parameter is false.
    Spec.logical("OR(FALSE();TRUE())", true);
    Spec.logical("OR(FALSE();FALSE())", false);
    // 6.15.7 NOT
    Spec.logical("NOT(FALSE())", true);
    Spec.logical("NOT(TRUE())", false);
    // 6.15.10 XOR is true when an odd number of parameters is true.
    Spec.logical("XOR(TRUE();TRUE())", false);
    Spec.logical("XOR(TRUE();FALSE())", true);
    // 6.15.9 TRUE and 6.15.3 FALSE
    Spec.logical("TRUE()", true);
    Spec.logical("FALSE()", false);
    // 6.15.4 IF returns the second argument when the condition is true.
    Spec.text("IF(TRUE();\"Y\";\"N\")", "Y");
    Spec.text("IF(FALSE();\"Y\";\"N\")", "N");
    // 6.15.6 IFNA returns the alternative only for NA.
    Spec.text("IFNA(NA();\"alt\")", "alt");
    Spec.number("IFNA(1;\"alt\")", "1");
    Spec.error("NA()", FormulaErrorValue.ERROR_NA);
  }
}
