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

package org.apache.hop.pipeline.transforms.tableinput;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.i18n.BaseMessages;

/**
 * Parses Table Input SQL for named parameters {@code {fieldName}} and rewrites them to JDBC {@code
 * ?} placeholders. Hop variables of the form {@code ${variable}} are left untouched.
 */
public final class TableInputSql {
  private static final Class<?> PKG = TableInputMeta.class;

  private TableInputSql() {
    // utility
  }

  /** Result of scanning SQL for named and positional parameters. */
  @Getter
  public static final class Parsed {
    private final String jdbcSql;
    private final List<String> namedParameters;
    private final int positionalParameterCount;

    Parsed(String jdbcSql, List<String> namedParameters, int positionalParameterCount) {
      this.jdbcSql = jdbcSql;
      this.namedParameters = namedParameters;
      this.positionalParameterCount = positionalParameterCount;
    }

    public boolean hasNamedParameters() {
      return !namedParameters.isEmpty();
    }
  }

  /** JDBC SQL plus the prepared-statement parameter metadata and values. */
  @Getter
  public static final class Bound {
    private final String jdbcSql;
    private final IRowMeta parameterMeta;
    private final Object[] parameterData;

    Bound(String jdbcSql, IRowMeta parameterMeta, Object[] parameterData) {
      this.jdbcSql = jdbcSql;
      this.parameterMeta = parameterMeta;
      this.parameterData = parameterData;
    }
  }

  /**
   * Rewrite {@code {field}} tokens to {@code ?} and collect the field names in appearance order.
   *
   * @param sql SQL that may contain named parameters, Hop variables, and/or positional {@code ?}
   * @return parsed JDBC SQL and parameter names
   * @throws HopException if placeholders are mixed or a brace is unclosed
   */
  public static Parsed parse(String sql) throws HopException {
    if (sql == null) {
      sql = "";
    }

    StringBuilder jdbc = new StringBuilder(sql.length());
    List<String> names = new ArrayList<>();
    int positional = 0;
    int i = 0;
    final int n = sql.length();

    while (i < n) {
      char c = sql.charAt(i);
      char next = (i + 1 < n) ? sql.charAt(i + 1) : 0;

      if (c == '-' && next == '-') {
        int end = indexOfNewline(sql, i + 2);
        if (end < 0) {
          jdbc.append(sql, i, n);
          break;
        }
        jdbc.append(sql, i, end);
        i = end;
        continue;
      }

      if (c == '/' && next == '*') {
        int end = sql.indexOf("*/", i + 2);
        if (end < 0) {
          jdbc.append(sql, i, n);
          break;
        }
        jdbc.append(sql, i, end + 2);
        i = end + 2;
        continue;
      }

      if (c == '\'') {
        i = copyQuoted(sql, i, jdbc, '\'');
        continue;
      }

      if (c == '"') {
        i = copyQuoted(sql, i, jdbc, '"');
        continue;
      }

      if (c == '$' && next == '{') {
        int end = sql.indexOf('}', i + 2);
        if (end < 0) {
          throw new HopException(
              BaseMessages.getString(PKG, "TableInputSql.Exception.UnclosedVariable"));
        }
        jdbc.append(sql, i, end + 1);
        i = end + 1;
        continue;
      }

      if (c == '{') {
        int end = sql.indexOf('}', i + 1);
        if (end < 0) {
          throw new HopException(
              BaseMessages.getString(PKG, "TableInputSql.Exception.UnclosedNamedParameter"));
        }
        String name = sql.substring(i + 1, end).trim();
        if (name.isEmpty()) {
          throw new HopException(
              BaseMessages.getString(PKG, "TableInputSql.Exception.EmptyNamedParameter"));
        }
        names.add(name);
        jdbc.append('?');
        i = end + 1;
        continue;
      }

      if (c == '?') {
        positional++;
      }
      jdbc.append(c);
      i++;
    }

    if (!names.isEmpty() && positional > 0) {
      throw new HopException(
          BaseMessages.getString(PKG, "TableInputSql.Exception.MixedPlaceholders"));
    }

    return new Parsed(jdbc.toString(), List.copyOf(names), positional);
  }

  /**
   * Bind incoming row values to the named parameters in {@code parsed}. When the SQL has no named
   * parameters the original parameter metadata and data are passed through (positional {@code ?}).
   */
  public static Bound bind(Parsed parsed, IRowMeta parametersMeta, Object[] parameters)
      throws HopException {
    if (!parsed.hasNamedParameters()) {
      return new Bound(parsed.getJdbcSql(), parametersMeta, parameters);
    }
    if (parametersMeta == null) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "TableInput.Exception.NamedParameterFieldNotFound",
              parsed.getNamedParameters().get(0)));
    }

    IRowMeta paramMeta = new RowMeta();
    Object[] paramData = new Object[parsed.getNamedParameters().size()];
    for (int i = 0; i < parsed.getNamedParameters().size(); i++) {
      String name = parsed.getNamedParameters().get(i);
      int idx = parametersMeta.indexOfValue(name);
      if (idx < 0) {
        throw new HopException(
            BaseMessages.getString(PKG, "TableInput.Exception.NamedParameterFieldNotFound", name));
      }
      paramMeta.addValueMeta(parametersMeta.getValueMeta(idx));
      if (parameters != null && idx < parameters.length) {
        paramData[i] = parameters[idx];
      }
    }
    return new Bound(parsed.getJdbcSql(), paramMeta, paramData);
  }

  /** Parse SQL and bind incoming values in one step. */
  public static Bound prepare(String sql, IRowMeta parametersMeta, Object[] parameters)
      throws HopException {
    return bind(parse(sql), parametersMeta, parameters);
  }

  /**
   * Bind named parameters only when {@code useNamedParameters} is true. Otherwise the SQL is passed
   * through unchanged so existing {@code {braces}} in queries stay literal.
   */
  public static Bound prepare(
      boolean useNamedParameters, String sql, IRowMeta parametersMeta, Object[] parameters)
      throws HopException {
    if (!useNamedParameters) {
      return new Bound(sql, parametersMeta, parameters);
    }
    return prepare(sql, parametersMeta, parameters);
  }

  private static int indexOfNewline(String sql, int from) {
    for (int i = from; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (c == '\n' || c == '\r') {
        return i;
      }
    }
    return -1;
  }

  private static int copyQuoted(String sql, int start, StringBuilder jdbc, char quote) {
    jdbc.append(quote);
    int j = start + 1;
    final int n = sql.length();
    while (j < n) {
      char ch = sql.charAt(j);
      jdbc.append(ch);
      if (ch == quote) {
        if (j + 1 < n && sql.charAt(j + 1) == quote) {
          jdbc.append(quote);
          j += 2;
          continue;
        }
        return j + 1;
      }
      j++;
    }
    return n;
  }
}
