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
package org.apache.hop.databases.cassandra.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** @author Tatsiana_Kasiankova */
/** Utilities for CQL processing. */
public class CQLUtils {

  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("(\\s){2,}");
  private static final Pattern WHITESPACE_IN_FUNCTION_OPEN_BRACKET_PATTERN =
      Pattern.compile("(\\s)*(\\()(\\s)*");
  private static final Pattern WHITESPACE_IN_FUNCTION_CLOSE_BRACKET_PATTERN =
      Pattern.compile("(\\s)*(\\))");
  private static final Pattern UNNECESSARY_WHITESPACE_BEFORE_COMMA_PATTERN =
      Pattern.compile("(\\s)+(,)");
  private static final Pattern ADD_WHITESPACE_AFTER_COMMA_PATTERN =
      Pattern.compile("(,)(?=[\\da-zA-Z])");
  private static final Pattern DOUBLE_QUOTES_PATTERN = Pattern.compile("\"");
  private static final Pattern QUOTE_PATTERN = Pattern.compile("\'");
  private static final Pattern NUMERIC_PATTERN = Pattern.compile("\\d+");

  private static final String DISTINCT = "distinct";
  private static final String FIRST = "first";
  private static final String OPEN_BRACKET = "(";
  private static final String CLOSE_BRACKET = ")";
  private static final String ALIAS_INDICATOR = " AS ";
  private static final String COMMA = ",";
  private static final String SELECT = "select";
  private static final String FROM = "from";
  private static final String WHITESPACE = " ";

  private enum SpecificNames {
    COUNT("count", "count(*)", "count(1)");

    List<String> variants;

    private SpecificNames(String... variants) {
      this.variants = Arrays.asList(variants);
    }

    private List<String> getVariants() {
      return variants;
    }

    private static final Map<String, String> SPECIFIC_NAMES = new HashMap<>();

    static {
      for (SpecificNames sName : SpecificNames.values()) {
        for (String variants : sName.getVariants()) {
          SPECIFIC_NAMES.put(variants.toUpperCase(), sName.name());
        }
      }
    }

    private static String getName(String value) {
      return SPECIFIC_NAMES.get(value.toUpperCase());
    }
  };

  private static ArrayList<Selector> getSelectors(String selectExpression, boolean isCql3) {
    ArrayList<Selector> sList = new ArrayList<Selector>();
    selectExpression = clean(selectExpression);
    while (selectExpression.length() > 0) {
      int possibleEnd = selectExpression.indexOf(COMMA);
      String possibleSelectorElement =
          (possibleEnd != -1) ? selectExpression.substring(0, possibleEnd) : selectExpression;
      selectExpression =
          (possibleEnd != -1)
              ? selectExpression.substring(possibleEnd + 1, selectExpression.length()).trim()
              : "";

      Selector realSelectorElement = buildSelector(possibleSelectorElement, isCql3);

      if (!isPartOfFunction(possibleSelectorElement) || isFunction(possibleSelectorElement)) {
        realSelectorElement = buildSelector(possibleSelectorElement, isCql3);
      } else {
        // The part of the function, we need one more part
        StringBuffer sb = new StringBuffer();
        sb.append(possibleSelectorElement).append(COMMA).append(WHITESPACE);
        int indexFunctionEnd = selectExpression.indexOf(CLOSE_BRACKET);
        possibleEnd = selectExpression.indexOf(COMMA, indexFunctionEnd);
        possibleSelectorElement =
            (possibleEnd != -1) ? selectExpression.substring(0, possibleEnd) : selectExpression;
        sb.append(possibleSelectorElement);
        possibleSelectorElement = sb.toString();
        selectExpression =
            (possibleEnd != -1)
                ? selectExpression.substring(possibleEnd + 1, selectExpression.length()).trim()
                : "";
        realSelectorElement = buildSelector(possibleSelectorElement, isCql3);
      }
      sList.add(realSelectorElement);
    }
    return sList;
  }

  /**
   * Extract select expression from the select clause. Assumes that any kettle variables have been
   * already substituted in the query.
   *
   * @param cqlExpresssion the select clause
   * @return the select expression. Null if select clause is empty or null.
   */
  public static String getSelectExpression(String cqlExpresssion) {
    String selectExpression = null;
    if (cqlExpresssion != null && !cqlExpresssion.isEmpty()) {
      cqlExpresssion = clean(cqlExpresssion);
      int end = cqlExpresssion.toLowerCase().indexOf(FROM);
      int start = cqlExpresssion.toLowerCase().indexOf(SELECT) + SELECT.length();
      if (start > -1 && end > -1) {
        selectExpression = cqlExpresssion.substring(start, end);
        int firstInd = selectExpression.toLowerCase().indexOf(FIRST);
        if (firstInd > -1) {
          selectExpression = selectExpression.substring(firstInd + FIRST.length()).trim();
          int nearWsIndex = selectExpression.indexOf(WHITESPACE);
          String numberPart = selectExpression.substring(0, nearWsIndex);
          selectExpression =
              isNumeric(numberPart) ? selectExpression.substring(nearWsIndex) : selectExpression;
        }
        int distInd = selectExpression.toLowerCase().indexOf(DISTINCT);
        if (distInd > -1) {
          selectExpression = selectExpression.substring(distInd + DISTINCT.length()).trim();
        }
        selectExpression = selectExpression.trim();
      }
    }
    return selectExpression;
  }

  /**
   * Returns the array of selectors
   *
   * @param selectExpression the CQL select expression
   * @param isCql3 the indicator of CQL3 version used
   * @return the array of selectors
   */
  public static Selector[] getColumnsInSelect(String selectExpression, boolean isCql3) {
    if (selectExpression != null && !selectExpression.isEmpty()) {
      ArrayList<Selector> selectors = getSelectors(selectExpression.trim(), isCql3);
      return selectors.toArray(new Selector[selectors.size()]);
    }
    return null;
  }

  static Selector buildSelector(String selector, boolean isCql3) {
    String columnName = getColumnName(selector, isCql3);
    return new Selector(
        getGeneralVariantForSpecificNames(columnName),
        getAlias(selector, isCql3),
        getFunction(selector));
  }

  private static String getFunction(String selector) {
    String f = null;
    if (isFunction(selector)) {
      f = selector.substring(0, selector.indexOf(OPEN_BRACKET)).trim().toLowerCase();
    }
    return f;
  }

  private static boolean isPartOfFunction(String element) {
    // there is at least one left or right bracket
    return element.indexOf(OPEN_BRACKET) > 0 || element.indexOf(CLOSE_BRACKET) > 0;
  }

  private static boolean isFunction(String element) {
    // there are both brackets
    return element.indexOf(OPEN_BRACKET) > 0 && element.indexOf(CLOSE_BRACKET) > 0;
  }

  private static final String getColumnName(String element, boolean isCql3) {
    String name = element.trim();
    int idx = element.toUpperCase().indexOf(ALIAS_INDICATOR);
    if (idx > -1) {
      name = name.substring(0, idx).trim();
    }
    if (!isFunction(element)) {
      name = getNormalizedForCql3Name(name, isCql3);
    }
    name = cleanQuotes(name);
    return name;
  }

  private static String getAlias(String element, boolean isCql3) {
    String alias = null;
    int idx = element.toUpperCase().indexOf(ALIAS_INDICATOR);
    if (idx > -1) {
      alias = getNormalizedForCql3Name(element.substring(idx + ALIAS_INDICATOR.length()), isCql3);
      alias = cleanQuotes(alias);
    }
    return alias;
  }

  private static String cleanUnnecessaryWhitespaces(String input) {
    String cleaned = null;
    if (input != null) {
      cleaned = WHITESPACE_PATTERN.matcher(input.trim()).replaceAll(WHITESPACE);
      cleaned =
          WHITESPACE_IN_FUNCTION_OPEN_BRACKET_PATTERN.matcher(cleaned.trim()).replaceAll("$2");
      cleaned =
          WHITESPACE_IN_FUNCTION_CLOSE_BRACKET_PATTERN.matcher(cleaned.trim()).replaceAll("$2");
      cleaned =
          UNNECESSARY_WHITESPACE_BEFORE_COMMA_PATTERN.matcher(cleaned.trim()).replaceAll("$2");
    }
    return cleaned;
  }

  private static String addWhitespaceAfterComa(String input) {
    String cleaned = null;
    if (input != null) {
      cleaned = ADD_WHITESPACE_AFTER_COMMA_PATTERN.matcher(input.trim()).replaceAll("$1 ");
    }
    return cleaned;
  }

  /**
   * Clean input from all unnecessary whitespaces: double and etc., before comma, inside functions
   * and etc. Add whitespace after comma if it is absent.
   *
   * @param input the input string
   * @return cleaned string
   */
  public static String clean(String input) {
    String cleaned = null;
    if (input != null) {
      cleaned = cleanUnnecessaryWhitespaces(input);
      cleaned = addWhitespaceAfterComa(cleaned);
    }
    return cleaned;
  }

  /**
   * Clean input of quotes and double quotes
   *
   * @param input the input string
   * @return cleaned string
   */
  public static String cleanQuotes(String input) {
    String cleaned = null;
    if (input != null) {
      cleaned = DOUBLE_QUOTES_PATTERN.matcher(input).replaceAll("");
      cleaned = QUOTE_PATTERN.matcher(cleaned).replaceAll("");
    }
    return cleaned;
  }

  private static boolean isQuoted(String input) {
    return DOUBLE_QUOTES_PATTERN.matcher(input).find() || QUOTE_PATTERN.matcher(input).find();
  }

  private static String getGeneralVariantForSpecificNames(String sName) {
    String gName = null;
    if (sName != null) {
      String name = SpecificNames.getName(sName);
      gName = name != null ? name : sName;
    }
    return gName;
  }

  private static String getNormalizedForCql3Name(String input, boolean isCql3) {
    String normalizedName = input;
    if (isCql3 && !isQuoted(input)) {
      normalizedName = input.toLowerCase();
    }
    return normalizedName;
  }

  private static boolean isNumeric(String input) {
    boolean result = false;
    if (input != null) {
      result = NUMERIC_PATTERN.matcher(input).matches();
    }
    return result;
  }
}
