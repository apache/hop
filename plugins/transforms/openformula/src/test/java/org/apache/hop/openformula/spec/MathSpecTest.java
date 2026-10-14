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

import org.junit.jupiter.api.Test;

/** Part 2 sections 6.16 and 6.18, plus INT from 6.17.2. Angles are radians. */
class MathSpecTest {
  @Test
  void mathFunctions() throws Exception {
    // 6.16.2 absolute value
    Spec.number("ABS(-2)", "2");
    // 6.16.3 arccosine of 1 is 0
    Spec.approx("ACOS(1)", 0);
    // 6.16.4 hyperbolic arccosine of 1 is 0
    Spec.approx("ACOSH(1)", 0);
    // 6.16.7 arcsine of 0 is 0
    Spec.approx("ASIN(0)", 0);
    // 6.16.9 arctangent of 0 is 0
    Spec.approx("ATAN(0)", 0);
    // 6.16.10 ATAN2(x; y). ATAN2(1; 1) is pi/4.
    Spec.approx("ATAN2(1;1)", Math.PI / 4);
    // 6.16.19 cosine of 0 is 1. 6.16.55 sine of 0 is 0.
    Spec.approx("COS(0)", 1);
    Spec.approx("SIN(0)", 0);
    // 6.16.31 EXP(0) is 1. 6.16.39 LN(1) is 0.
    Spec.approx("EXP(0)", 1);
    Spec.approx("LN(1)", 0);
    // 6.16.40 LOG(number; base). 6.16.41 LOG10.
    Spec.number("LOG(100;10)", "2");
    Spec.number("LOG10(1000)", "3");
    // 6.16.45 PI
    Spec.approx("PI()", Math.PI);
    // 6.16.46 POWER. 6.16.58 SQRT.
    Spec.number("POWER(2;3)", "8");
    Spec.number("SQRT(9)", "3");
    // 6.16.30 EVEN rounds away from zero to an even integer. 6.16.44 ODD likewise to odd.
    Spec.number("EVEN(1)", "2");
    Spec.number("ODD(2)", "3");
    // 6.16.42 remainder has the sign of the divisor.
    Spec.number("MOD(5;2)", "1");
    // 6.16.61 SUM. 6.18.3 AVERAGE. 6.18.45 MAX. 6.18.48 MIN.
    Spec.number("SUM(1;2;3)", "6");
    Spec.number("AVERAGE(2;4;6)", "4");
    Spec.number("MAX(1;9;3)", "9");
    Spec.number("MIN(1;9;3)", "1");
    // 6.18.4 AVERAGEA, 6.18.46 MAXA, 6.18.49 MINA treat the same numbers the same way.
    Spec.number("AVERAGEA(2;4)", "3");
    Spec.number("MAXA(1;4)", "4");
    Spec.number("MINA(1;4)", "1");
    // 6.18.82 sample variance of 1, 2, 3 is 1.
    Spec.number("VAR(1;2;3)", "1");
    // 6.17.2 INT rounds toward negative infinity.
    Spec.number("INT(1.9)", "1");
    Spec.number("INT(-1.2)", "-2");
    // 6.13.26 N converts logical true to 1.
    Spec.number("N(TRUE())", "1");
  }
}
