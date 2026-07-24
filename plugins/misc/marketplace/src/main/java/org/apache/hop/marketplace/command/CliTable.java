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

package org.apache.hop.marketplace.command;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/** Portable ASCII table and CSV printers for marketplace CLI output. */
public final class CliTable {

  private CliTable() {}

  /**
   * Print a bordered ASCII table. Column widths are the max of header and cell content (after
   * optional truncation of individual cells by the caller).
   */
  public static void printTable(PrintStream out, List<String> headers, List<List<String>> rows) {
    if (headers == null || headers.isEmpty()) {
      return;
    }
    int cols = headers.size();
    int[] widths = new int[cols];
    for (int c = 0; c < cols; c++) {
      widths[c] = displayWidth(headers.get(c));
    }
    if (rows != null) {
      for (List<String> row : rows) {
        if (row == null) {
          continue;
        }
        for (int c = 0; c < cols; c++) {
          String cell = c < row.size() ? nullToEmpty(row.get(c)) : "";
          widths[c] = Math.max(widths[c], displayWidth(cell));
        }
      }
    }

    String separator = buildSeparator(widths);
    out.println(separator);
    out.println(buildRow(headers, widths));
    out.println(separator);
    if (rows != null) {
      for (List<String> row : rows) {
        if (row == null) {
          continue;
        }
        List<String> padded = new ArrayList<>(cols);
        for (int c = 0; c < cols; c++) {
          padded.add(c < row.size() ? nullToEmpty(row.get(c)) : "");
        }
        out.println(buildRow(padded, widths));
      }
    }
    out.println(separator);
  }

  /** Print CSV with a header row. Fields are quoted when needed (comma, quote, CR/LF). */
  public static void printCsv(PrintStream out, List<String> headers, List<List<String>> rows) {
    if (headers == null || headers.isEmpty()) {
      return;
    }
    out.println(toCsvLine(headers));
    if (rows == null) {
      return;
    }
    int cols = headers.size();
    for (List<String> row : rows) {
      if (row == null) {
        continue;
      }
      List<String> cells = new ArrayList<>(cols);
      for (int c = 0; c < cols; c++) {
        cells.add(c < row.size() ? nullToEmpty(row.get(c)) : "");
      }
      out.println(toCsvLine(cells));
    }
  }

  static String toCsvLine(List<String> fields) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(escapeCsv(fields.get(i)));
    }
    return sb.toString();
  }

  static String escapeCsv(String value) {
    String v = nullToEmpty(value);
    boolean needsQuotes =
        v.indexOf(',') >= 0 || v.indexOf('"') >= 0 || v.indexOf('\n') >= 0 || v.indexOf('\r') >= 0;
    if (!needsQuotes) {
      return v;
    }
    return '"' + v.replace("\"", "\"\"") + '"';
  }

  /**
   * Truncate for display, appending {@code ...} when shortened. {@code maxLen} is the maximum
   * output length including the ellipsis.
   */
  public static String truncate(String value, int maxLen) {
    String v = nullToEmpty(value).replace('\n', ' ').replace('\r', ' ');
    if (maxLen <= 0 || v.length() <= maxLen) {
      return v;
    }
    if (maxLen <= 3) {
      return v.substring(0, maxLen);
    }
    return v.substring(0, maxLen - 3) + "...";
  }

  private static String buildSeparator(int[] widths) {
    StringBuilder sb = new StringBuilder();
    sb.append('+');
    for (int w : widths) {
      sb.append("-".repeat(w + 2));
      sb.append('+');
    }
    return sb.toString();
  }

  private static String buildRow(List<String> cells, int[] widths) {
    StringBuilder sb = new StringBuilder();
    sb.append('|');
    for (int c = 0; c < widths.length; c++) {
      String cell = c < cells.size() ? nullToEmpty(cells.get(c)) : "";
      sb.append(' ');
      sb.append(padRight(cell, widths[c]));
      sb.append(' ');
      sb.append('|');
    }
    return sb.toString();
  }

  private static String padRight(String s, int width) {
    int pad = width - displayWidth(s);
    if (pad <= 0) {
      return s;
    }
    return s + " ".repeat(pad);
  }

  private static int displayWidth(String s) {
    return nullToEmpty(s).length();
  }

  private static String nullToEmpty(String s) {
    return s == null ? "" : s;
  }
}
