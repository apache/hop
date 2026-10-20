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

/** Part 2 section 6.20. */
class TextSpecTest {
  @Test
  void textFunctions() throws Exception {
    // 6.20.2 characters that are not full-width are copied through.
    Spec.text("ASC(\"A\")", "A");
    // 6.20.3 CHAR and 6.20.5 CODE are code-point pairs for ASCII.
    Spec.text("CHAR(65)", "A");
    Spec.number("CODE(\"A\")", "65");
    // 6.20.4 CLEAN copies printable text.
    Spec.text("CLEAN(\"A\")", "A");
    // 6.20.6 CONCATENATE
    Spec.text("CONCATENATE(\"a\";\"b\")", "ab");
    // 6.20.8 EXACT is case-sensitive.
    Spec.logical("EXACT(\"A\";\"A\")", true);
    Spec.logical("EXACT(\"A\";\"a\")", false);
    // 6.20.9 FIND is 1-based and case-sensitive. 6.20.20 SEARCH is not case-sensitive.
    Spec.number("FIND(\"l\";\"Hello\")", "3");
    Spec.number("SEARCH(\"L\";\"Hello\")", "3");
    // 6.20.12 LEFT returns Length characters from the start. Omitted Length is 1.
    Spec.text("LEFT(\"Hello\";2)", "He");
    Spec.text("LEFT(\"Hello\")", "H");
    // 6.20.13 LEN counts characters.
    Spec.number("LEN(\"Hello\")", "5");
    // 6.20.14 LOWER and 6.20.27 UPPER convert A-Z.
    Spec.text("LOWER(\"AB\")", "ab");
    Spec.text("UPPER(\"ab\")", "AB");
    // 6.20.15 MID is 1-based.
    Spec.text("MID(\"Hello\";2;3)", "ell");
    // 6.20.16 PROPER capitalizes the first letter of each word.
    Spec.text("PROPER(\"hello world\")", "Hello World");
    // 6.20.17 REPLACE replaces a counted span. 6.20.21 SUBSTITUTE replaces matching text.
    Spec.text("REPLACE(\"Hello\";2;2;\"i\")", "Hilo");
    Spec.text("SUBSTITUTE(\"ab\";\"a\";\"x\")", "xb");
    // 6.20.18 REPT. 6.20.19 RIGHT.
    Spec.text("REPT(\"ab\";2)", "abab");
    Spec.text("RIGHT(\"Hello\";2)", "lo");
    // 6.20.22 T returns text unchanged and empty text for a number.
    Spec.text("T(\"a\")", "a");
    Spec.text("T(1)", "");
    // 6.20.24 TRIM drops leading and trailing space and collapses inner runs.
    Spec.text("TRIM(\" a  b \")", "a b");
    // 6.20.25 UNICHAR and 6.20.26 UNICODE.
    Spec.text("UNICHAR(65)", "A");
    Spec.number("UNICODE(\"A\")", "65");
    // 6.20.10 FIXED with separators omitted is the rounded digits.
    Spec.text("FIXED(1;0;TRUE())", "1");
    // 6.20.7 DOLLAR returns currency text. The symbol is locale-specific.
    Spec.textContains("DOLLAR(3;0)", "3");
    // 6.20.23 TEXT formats a number. "0" has no locale grouping.
    Spec.text("TEXT(4;\"0\")", "4");
  }
}
