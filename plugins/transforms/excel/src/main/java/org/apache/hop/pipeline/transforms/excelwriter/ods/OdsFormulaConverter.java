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

package org.apache.hop.pipeline.transforms.excelwriter.ods;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.util.Utils;

/** Converts Excel-style formula strings to OpenFormula syntax for ODS output. */
public final class OdsFormulaConverter {

  private static final Pattern SHEET_CELL =
      Pattern.compile(
          "((?:'[^']+'|[A-Za-z0-9_]+))!(\\$?)([A-Za-z]{1,3})(\\$?)(\\d+)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern CELL_RANGE =
      Pattern.compile(
          "(\\$?)([A-Za-z]{1,3})(\\$?)(\\d+):(\\$?)([A-Za-z]{1,3})(\\$?)(\\d+)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern CELL_REFERENCE =
      Pattern.compile("(?<![\\[\\.])\\$?([A-Za-z]{1,3})\\$?(\\d+)", Pattern.CASE_INSENSITIVE);

  private OdsFormulaConverter() {}

  public static String toOdfFormula(String excelFormula) {
    if (Utils.isEmpty(excelFormula)) {
      return excelFormula;
    }
    String trimmed = excelFormula.trim();
    if (trimmed.regionMatches(true, 0, "of:", 0, 3)) {
      return trimmed;
    }

    String body = trimmed.startsWith("=") ? trimmed.substring(1) : trimmed;
    body = convertSheetCellReferences(body);
    body = convertCellRanges(body);
    body = convertCellReferences(body);
    return "of:=" + body;
  }

  private static String convertSheetCellReferences(String body) {
    Matcher matcher = SHEET_CELL.matcher(body);
    StringBuilder result = new StringBuilder();
    while (matcher.find()) {
      String sheet = matcher.group(1);
      String column = matcher.group(3).toUpperCase();
      String row = matcher.group(5);
      matcher.appendReplacement(result, sheet + ".[" + column + row + "]");
    }
    matcher.appendTail(result);
    return result.toString();
  }

  private static String convertCellRanges(String body) {
    Matcher matcher = CELL_RANGE.matcher(body);
    StringBuilder result = new StringBuilder();
    while (matcher.find()) {
      String startCol = matcher.group(2).toUpperCase();
      String startRow = matcher.group(4);
      String endCol = matcher.group(6).toUpperCase();
      String endRow = matcher.group(8);
      matcher.appendReplacement(result, "[." + startCol + startRow + ":." + endCol + endRow + "]");
    }
    matcher.appendTail(result);
    return result.toString();
  }

  private static String convertCellReferences(String body) {
    Matcher matcher = CELL_REFERENCE.matcher(body);
    StringBuilder result = new StringBuilder();
    while (matcher.find()) {
      String column = matcher.group(1).toUpperCase();
      String row = matcher.group(2);
      matcher.appendReplacement(result, ".[" + column + row + "]");
    }
    matcher.appendTail(result);
    return result.toString();
  }
}
