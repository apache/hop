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

package org.apache.hop.ui.core.widget;

import java.util.Locale;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Finds a cell in a table grid. The scan is row by row and, within a row, left to right in the
 * visual column order. Unchecked columns are skipped. There is no wrap.
 */
public final class TableViewFind {

  private TableViewFind() {}

  /** One cell, with {@code dataColumn} the data-column index (the {@code #} column is not one). */
  public record Hit(int row, int dataColumn) {}

  /** A grid captured from a {@link TableView} for one search click. */
  public record Grid(
      String[][] cellValues,
      int[] visualDataColumns,
      String[] columnNames,
      int activeRow,
      int activeDataColumn) {}

  public enum Status {
    FOUND,
    NOT_FOUND,
    EMPTY_QUERY,
    INVALID_REGEX
  }

  /** Outcome of one scan. {@code hit} is set only for {@link Status#FOUND}. */
  public record Result(Status status, Hit hit, String regexMessage) {
    public static Result found(Hit hit) {
      return new Result(Status.FOUND, hit, null);
    }

    public static Result notFound() {
      return new Result(Status.NOT_FOUND, null, null);
    }

    public static Result emptyQuery() {
      return new Result(Status.EMPTY_QUERY, null, null);
    }

    public static Result invalidRegex(String message) {
      return new Result(Status.INVALID_REGEX, null, message);
    }
  }

  /**
   * Scan {@code rows} for {@code query}.
   *
   * @param rows full cell text, {@code [row][dataColumn]}
   * @param visualDataColumns data-column indexes in visual order
   * @param included one flag per data column; unchecked columns are skipped
   * @param startRow row to start from
   * @param startDataColumn data column to start from, or {@code -1} to start at the first checked
   *     column of {@code startRow}
   * @param inclusive when false, the start cell itself is skipped
   */
  public static Result find(
      String[][] rows,
      int[] visualDataColumns,
      boolean[] included,
      String query,
      boolean caseSensitive,
      boolean regex,
      int startRow,
      int startDataColumn,
      boolean inclusive) {
    if (query == null || query.isEmpty()) {
      return Result.emptyQuery();
    }

    Pattern pattern = null;
    String needle = null;
    if (regex) {
      int flags = Pattern.DOTALL;
      if (!caseSensitive) {
        flags |= Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
      }
      try {
        pattern = Pattern.compile(query, flags);
      } catch (PatternSyntaxException e) {
        String message = e.getDescription();
        if (message == null || message.isEmpty()) {
          message = e.getMessage();
        }
        return Result.invalidRegex(message == null ? "" : message);
      }
    } else {
      needle = caseSensitive ? query : query.toLowerCase(Locale.ROOT);
    }

    if (rows == null || visualDataColumns == null || included == null || rows.length == 0) {
      return Result.notFound();
    }

    int rowStart = Math.max(0, startRow);
    boolean seeking = startDataColumn >= 0 && startRow >= 0;
    for (int row = rowStart; row < rows.length; row++) {
      String[] cells = rows[row];
      boolean sawStart = false;
      for (int dataColumn : visualDataColumns) {
        boolean selected = dataColumn >= 0 && dataColumn < included.length && included[dataColumn];
        if (seeking && row == rowStart && dataColumn == startDataColumn) {
          sawStart = true;
          seeking = false;
          if (!inclusive || !selected) {
            continue;
          }
        } else if (seeking && row == rowStart) {
          continue;
        }
        if (!selected) {
          continue;
        }
        String text = "";
        if (cells != null && dataColumn < cells.length && cells[dataColumn] != null) {
          text = cells[dataColumn];
        }
        if (pattern == null && !caseSensitive) {
          text = text.toLowerCase(Locale.ROOT);
        }
        if (matches(text, needle, pattern)) {
          return Result.found(new Hit(row, dataColumn));
        }
      }
      if (seeking && row == rowStart && !sawStart) {
        seeking = false;
      }
    }
    return Result.notFound();
  }

  private static boolean matches(String text, String needle, Pattern pattern) {
    if (pattern != null) {
      return pattern.matcher(text).find();
    }
    return text.contains(needle);
  }
}
