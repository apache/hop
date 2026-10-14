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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.openformula.parser.FormulaLexer;
import org.apache.hop.openformula.parser.ParseException;
import org.apache.hop.openformula.parser.Token;
import org.apache.hop.openformula.parser.TokenType;

public final class FormulaUtil {
  private static final Pattern CONTEXT_PATTERN = Pattern.compile("^([a-zA-Z_0-9]+):(.*)$");

  private FormulaUtil() {}

  public static String[] extractFormulaContext(String formula) {
    if (formula == null) {
      return new String[] {null, null};
    }
    String trimmed = formula.trim();
    if (trimmed.startsWith("=")) {
      return new String[] {"report", trimmed.substring(1)};
    }
    Matcher m = CONTEXT_PATTERN.matcher(trimmed);
    if (m.matches()) {
      return new String[] {m.group(1), m.group(2)};
    }
    return new String[] {null, null};
  }

  public static String[] getReferences(String formulaText) throws ParseException {
    if (formulaText == null || formulaText.isBlank()) {
      return new String[0];
    }
    String cleaned = formulaText.trim();
    if (cleaned.startsWith("=")) {
      cleaned = cleaned.substring(1);
    }
    FormulaLexer lexer = new FormulaLexer(cleaned);
    List<Token> tokens = lexer.tokenize();
    Set<String> refs = new LinkedHashSet<>();
    for (Token t : tokens) {
      if (t.type() == TokenType.COLUMN_LOOKUP) {
        refs.add(t.text());
      }
    }
    return refs.toArray(new String[0]);
  }
}
