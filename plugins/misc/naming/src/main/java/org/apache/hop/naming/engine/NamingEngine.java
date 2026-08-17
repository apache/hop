/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.naming.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.naming.metadata.NamingCaseStyle;
import org.apache.hop.naming.metadata.NamingScheme;
import org.apache.hop.naming.metadata.NamingWordSeparator;

/**
 * Pure string transform that applies a {@link NamingScheme} to an input name. No UI or metadata
 * provider dependencies — safe to unit-test and reuse from transforms later.
 */
public final class NamingEngine {

  private NamingEngine() {
    // utility
  }

  /**
   * Apply the scheme to {@code input}. Null input returns null; empty input returns empty.
   *
   * @param scheme naming rules (null is treated as default hop-field snake_case-ish scheme)
   * @param input original name
   * @return transformed name
   */
  /**
   * Names that must not be rewritten: empty, table null markers, and values that already contain a
   * variable expression.
   */
  public static boolean shouldSkip(String value) {
    if (StringUtils.isEmpty(value) || "<null>".equals(value)) {
      return true;
    }
    return value.contains("${");
  }

  public static String apply(NamingScheme scheme, String input) {
    if (input == null) {
      return null;
    }
    if (input.isEmpty()) {
      return input;
    }

    NamingScheme effective = scheme != null ? scheme : new NamingScheme();
    NamingCaseStyle caseStyle = NamingCaseStyle.fromCode(effective.getCaseStyle());
    NamingWordSeparator wordSeparator = NamingWordSeparator.fromCode(effective.getWordSeparator());

    List<String> words = splitWords(input, effective.getExtraDelimiters());
    if (effective.isRemoveSpecialCharacters()) {
      words = removeSpecialCharacters(words);
    }
    if (words.isEmpty()) {
      return applyAffixes(effective, "");
    }

    String joined = joinWords(words, caseStyle, wordSeparator);
    joined = postProcessSeparators(joined, wordSeparator, effective);
    return applyAffixes(effective, joined);
  }

  static List<String> splitWords(String input, String extraDelimiters) {
    String prepared = insertCamelCaseBoundaries(input.trim());
    StringBuilder delimiters = new StringBuilder(" \t\n\r_-");
    if (StringUtils.isNotEmpty(extraDelimiters)) {
      delimiters.append(extraDelimiters);
    }

    List<String> words = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    for (int i = 0; i < prepared.length(); i++) {
      char c = prepared.charAt(i);
      if (delimiters.indexOf(String.valueOf(c)) >= 0) {
        if (!current.isEmpty()) {
          words.add(current.toString());
          current.setLength(0);
        }
      } else {
        current.append(c);
      }
    }
    if (!current.isEmpty()) {
      words.add(current.toString());
    }
    return words;
  }

  /**
   * Insert spaces at camelCase and letter/digit boundaries so later delimiter splitting sees
   * separate words. Acronyms: {@code XMLParser} → {@code XML Parser}.
   */
  static String insertCamelCaseBoundaries(String input) {
    if (input.length() < 2) {
      return input;
    }
    StringBuilder sb = new StringBuilder(input.length() + 8);
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      if (i > 0) {
        char prev = input.charAt(i - 1);
        char next = i + 1 < input.length() ? input.charAt(i + 1) : 0;
        boolean lowerThenUpper = Character.isLowerCase(prev) && Character.isUpperCase(c);
        boolean acronymBoundary =
            Character.isUpperCase(prev)
                && Character.isUpperCase(c)
                && next != 0
                && Character.isLowerCase(next);
        boolean letterDigit =
            (Character.isLetter(prev) && Character.isDigit(c))
                || (Character.isDigit(prev) && Character.isLetter(c));
        if (lowerThenUpper || acronymBoundary || letterDigit) {
          sb.append(' ');
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }

  private static List<String> removeSpecialCharacters(List<String> words) {
    List<String> cleaned = new ArrayList<>();
    for (String word : words) {
      StringBuilder sb = new StringBuilder(word.length());
      for (int i = 0; i < word.length(); i++) {
        char c = word.charAt(i);
        if (Character.isLetterOrDigit(c)) {
          sb.append(c);
        }
      }
      if (!sb.isEmpty()) {
        cleaned.add(sb.toString());
      }
    }
    return cleaned;
  }

  private static String joinWords(
      List<String> words, NamingCaseStyle caseStyle, NamingWordSeparator wordSeparator) {
    if (caseStyle == NamingCaseStyle.CAMEL || caseStyle == NamingCaseStyle.PASCAL) {
      return joinCamel(words, caseStyle == NamingCaseStyle.PASCAL);
    }

    String sep = wordSeparator.getValue();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < words.size(); i++) {
      if (i > 0 && !sep.isEmpty()) {
        sb.append(sep);
      }
      sb.append(applyCaseToWord(words.get(i), caseStyle));
    }
    return sb.toString();
  }

  private static String joinCamel(List<String> words, boolean pascal) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < words.size(); i++) {
      String word = words.get(i);
      if (word.isEmpty()) {
        continue;
      }
      String lower = word.toLowerCase(Locale.ROOT);
      if (i == 0 && !pascal) {
        sb.append(lower);
      } else {
        sb.append(Character.toUpperCase(lower.charAt(0)));
        if (lower.length() > 1) {
          sb.append(lower.substring(1));
        }
      }
    }
    return sb.toString();
  }

  private static String applyCaseToWord(String word, NamingCaseStyle caseStyle) {
    return switch (caseStyle) {
      case LOWER -> word.toLowerCase(Locale.ROOT);
      case UPPER -> word.toUpperCase(Locale.ROOT);
      default -> word; // AS_IS
    };
  }

  private static String postProcessSeparators(
      String joined, NamingWordSeparator wordSeparator, NamingScheme scheme) {
    String result = joined;
    String sep = wordSeparator.getValue();
    if (StringUtils.isNotEmpty(sep)) {
      if (scheme.isCollapseRepeatedSeparators()) {
        String doubled = sep + sep;
        while (result.contains(doubled)) {
          result = result.replace(doubled, sep);
        }
      }
      if (scheme.isTrimEdgeSeparators()) {
        while (result.startsWith(sep)) {
          result = result.substring(sep.length());
        }
        while (result.endsWith(sep)) {
          result = result.substring(0, result.length() - sep.length());
        }
      }
    }
    return result;
  }

  private static String applyAffixes(NamingScheme scheme, String value) {
    String prefix = StringUtils.defaultString(scheme.getPrefix());
    String suffix = StringUtils.defaultString(scheme.getSuffix());
    return prefix + value + suffix;
  }
}
