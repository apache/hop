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
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.junit.jupiter.api.Test;

/** Part 2 sections 6.13 and 6.14. */
class InformationSpecTest {
  @Test
  void informationFunctions() throws Exception {
    // 6.14.3 CHOOSE is 1-based.
    Spec.text("CHOOSE(2;\"a\";\"b\";\"c\")", "b");
    // 6.13.6 COUNT counts numbers. 6.13.7 COUNTA counts non-blank values.
    Spec.number("COUNT(1;2;\"a\")", "2");
    Spec.number("COUNTA(1;\"a\")", "2");
    // 6.13.8 COUNTBLANK. A number is not blank.
    Spec.number("COUNTBLANK(1)", "0");
    // 6.14.6 INDEX is 1-based from the top left. Columns in an inline array are separated by ";".
    Spec.number("INDEX({10;20;30};1;2)", "20");
    // 6.14.8 LOOKUP prefers an exact match in an ascending vector.
    Spec.text("LOOKUP(2;{1;2;3};{\"a\";\"b\";\"c\"})", "b");
    // 6.13.14 a number is not blank.
    Spec.logical("ISBLANK(1)", false);
    // 6.13.15 ISERR is false for NA. 6.13.16 ISERROR is true for NA. 6.13.20 ISNA.
    Spec.logical("ISERR(NA())", false);
    Spec.logical("ISERROR(NA())", true);
    Spec.logical("ISNA(NA())", true);
    Spec.logical("ISNA(1)", false);
    // 6.13.17 ISEVEN, 6.13.23 ISODD, 6.13.19 ISLOGICAL, 6.13.21 ISNONTEXT, 6.13.22 ISNUMBER,
    // 6.13.25 ISTEXT.
    Spec.logical("ISEVEN(2)", true);
    Spec.logical("ISEVEN(3)", false);
    Spec.logical("ISODD(3)", true);
    Spec.logical("ISLOGICAL(TRUE())", true);
    Spec.logical("ISLOGICAL(1)", false);
    Spec.logical("ISNONTEXT(1)", true);
    Spec.logical("ISNONTEXT(\"a\")", false);
    Spec.logical("ISNUMBER(1)", true);
    Spec.logical("ISNUMBER(\"a\")", false);
    Spec.logical("ISTEXT(\"a\")", true);
    Spec.logical("ISTEXT(1)", false);
    // 6.13.27 NA
    Spec.error("NA()", FormulaErrorValue.ERROR_NA);
    // 6.13.34 VALUE converts plain integer text.
    Spec.number("VALUE(\"42\")", "42");
  }

  /** 6.13.24 ISREF is true for a reference. The pipeline writes a reference as [name]. */
  @Test
  void isRefOnAField() throws Exception {
    DefaultFormulaContext context = new DefaultFormulaContext();
    context.defineReference("qty", 1);
    Object actual = Spec.value("ISREF([qty])", context);
    org.junit.jupiter.api.Assertions.assertEquals(true, actual);
  }
}
