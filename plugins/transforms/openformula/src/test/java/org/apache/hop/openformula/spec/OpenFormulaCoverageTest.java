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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.TreeSet;
import org.apache.hop.openformula.extra.ExtraFunctionTest;
import org.apache.hop.openformula.function.DefaultFunctionRegistry;
import org.junit.jupiter.api.Test;

/**
 * Every function the engine registers is either a Part 2 function exercised above or a Kettle/PDI
 * extra exercised in {@link ExtraFunctionTest}.
 */
class OpenFormulaCoverageTest {
  /** Names that have a section in OpenDocument v1.2 Part 2 and a case in this package. */
  static final Set<String> PART2 =
      Set.of(
          "ABS",
          "ACOS",
          "ACOSH",
          "AND",
          "ASIN",
          "ATAN",
          "ATAN2",
          "AVERAGE",
          "AVERAGEA",
          "CHAR",
          "CHOOSE",
          "CLEAN",
          "CODE",
          "CONCATENATE",
          "COS",
          "COUNT",
          "COUNTA",
          "COUNTBLANK",
          "DATE",
          "DATEDIF",
          "DATEVALUE",
          "DAY",
          "DAYS",
          "DOLLAR",
          "EVEN",
          "EXACT",
          "EXP",
          "FALSE",
          "FIND",
          "FIXED",
          "HOUR",
          "IF",
          "IFNA",
          "INDEX",
          "INT",
          "ISBLANK",
          "ISERR",
          "ISERROR",
          "ISEVEN",
          "ISLOGICAL",
          "ISNA",
          "ISNONTEXT",
          "ISNUMBER",
          "ISODD",
          "ISREF",
          "ISTEXT",
          "LEFT",
          "LEN",
          "LN",
          "LOG",
          "LOG10",
          "LOOKUP",
          "LOWER",
          "MAX",
          "MAXA",
          "MID",
          "MIN",
          "MINA",
          "MINUTE",
          "MOD",
          "MONTH",
          "N",
          "NA",
          "NOT",
          "NOW",
          "ODD",
          "OR",
          "PI",
          "POWER",
          "PROPER",
          "REPLACE",
          "REPT",
          "RIGHT",
          "SEARCH",
          "SECOND",
          "SIN",
          "SQRT",
          "SUBSTITUTE",
          "SUM",
          "T",
          "TEXT",
          "TIME",
          "TIMEVALUE",
          "TODAY",
          "TRIM",
          "TRUE",
          "UNICHAR",
          "UNICODE",
          "UPPER",
          "VALUE",
          "VAR",
          "WEEKDAY",
          "XOR",
          "YEAR",
          "ASC");

  @Test
  void everyRegisteredFunctionIsClassified() {
    Set<String> registered = new TreeSet<>();
    for (String name : new DefaultFunctionRegistry().getFunctionNames()) {
      registered.add(name.toUpperCase());
    }
    Set<String> classified = new TreeSet<>();
    classified.addAll(PART2);
    classified.addAll(ExtraFunctionTest.EXTRAS);
    assertEquals(classified, registered);
  }
}
