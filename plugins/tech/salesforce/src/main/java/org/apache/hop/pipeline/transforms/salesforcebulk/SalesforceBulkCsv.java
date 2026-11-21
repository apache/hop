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

package org.apache.hop.pipeline.transforms.salesforcebulk;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;

/** CSV rules for Bulk API 2.0: quote when required, and {@code #N/A} for a null on update. */
public final class SalesforceBulkCsv {
  /** Value written for a null on update and upsert. An empty field does not clear a value. */
  public static final String NULL_TOKEN = "#N/A";

  public static final String SUCCESS_ID = "sf__Id";
  public static final String SUCCESS_CREATED = "sf__Created";
  public static final String FAILURE_ERROR = "sf__Error";

  private SalesforceBulkCsv() {}

  public static String encode(String value, String delimiter) {
    if (value == null) {
      return "";
    }
    boolean quote =
        value.indexOf(delimiter) >= 0
            || value.indexOf('"') >= 0
            || value.indexOf('\n') >= 0
            || value.indexOf('\r') >= 0
            || value.startsWith(" ")
            || value.endsWith(" ");
    String escaped = value.replace("\"", "\"\"");
    if (quote) {
      return "\"" + escaped + "\"";
    }
    return escaped;
  }

  public static byte[] toBytes(String csv) {
    return csv.getBytes(StandardCharsets.UTF_8);
  }

  /** Parsed table. The first row is the header. */
  public static List<List<String>> parse(String csv) throws HopException {
    return parse(csv, SalesforceBulkDelimiter.COMMA);
  }

  public static List<List<String>> parse(String csv, SalesforceBulkDelimiter columnDelimiter)
      throws HopException {
    String delimiter = columnDelimiter == null ? "," : columnDelimiter.getCharacter();
    List<List<String>> rows = new ArrayList<>();
    if (csv == null || csv.isEmpty()) {
      return rows;
    }
    List<String> row = new ArrayList<>();
    StringBuilder field = new StringBuilder();
    boolean quoted = false;
    for (int i = 0; i < csv.length(); i++) {
      char current = csv.charAt(i);
      if (quoted) {
        if (current == '"') {
          if (i + 1 < csv.length() && csv.charAt(i + 1) == '"') {
            field.append('"');
            i++;
          } else {
            quoted = false;
          }
        } else {
          field.append(current);
        }
        continue;
      }
      if (current == '"' && field.length() == 0) {
        quoted = true;
        continue;
      }
      if (delimiter != null && current == delimiter.charAt(0) && delimiter.length() == 1) {
        row.add(field.toString());
        field.setLength(0);
        continue;
      }
      if (current == '\n' || current == '\r') {
        row.add(field.toString());
        field.setLength(0);
        if (!isBlankRow(row)) {
          rows.add(row);
        }
        row = new ArrayList<>();
        if (current == '\r' && i + 1 < csv.length() && csv.charAt(i + 1) == '\n') {
          i++;
        }
        continue;
      }
      field.append(current);
    }
    if (quoted) {
      throw new HopException("A Bulk API result CSV has an unterminated quote");
    }
    if (field.length() > 0 || !row.isEmpty()) {
      row.add(field.toString());
      if (!isBlankRow(row)) {
        rows.add(row);
      }
    }
    return rows;
  }

  private static boolean isBlankRow(List<String> row) {
    for (String value : row) {
      if (value != null && !value.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  /** One CSV document built with a fixed header, delimiter, and line ending. */
  public static final class Builder {
    private final List<String> headers;
    private final String delimiter;
    private final String lineEnding;
    private final StringBuilder csv = new StringBuilder();
    private int rows;

    public Builder(
        List<String> headers,
        SalesforceBulkDelimiter delimiter,
        SalesforceBulkLineEnding lineEnding) {
      this.headers = List.copyOf(headers);
      this.delimiter = delimiter.getCharacter();
      this.lineEnding = lineEnding.getCharacters();
      writeRow(this.headers);
    }

    public int sizeBytes() {
      return csv.toString().getBytes(StandardCharsets.UTF_8).length;
    }

    public int getRows() {
      return rows;
    }

    public void add(List<String> values) {
      writeRow(values);
      rows++;
    }

    public byte[] toByteArray() {
      return csv.toString().getBytes(StandardCharsets.UTF_8);
    }

    private void writeRow(List<String> values) {
      for (int i = 0; i < values.size(); i++) {
        if (i > 0) {
          csv.append(delimiter);
        }
        csv.append(encode(values.get(i) == null ? "" : values.get(i), delimiter));
      }
      csv.append(lineEnding);
    }
  }
}
