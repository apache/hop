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

package org.apache.hop.calcite;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.calcite.config.CalciteSqlFormatConfig;

/**
 * Pretty-print SQL with Apache Calcite. Hop variable expressions ({@code ${NAME}}) are replaced
 * with placeholders for parsing and restored afterwards.
 *
 * <p>When the whole script cannot be parsed, each {@code ;}-separated statement is tried on its own
 * so a valid {@code SELECT} next to dialect-specific DDL still formats.
 */
public final class CalciteSqlFormatter {

  private static final Pattern HOP_VARIABLE = Pattern.compile("\\$\\{[^}]+\\}");

  private static final String PLACEHOLDER_PREFIX = "HOPVAR";

  private CalciteSqlFormatter() {}

  public static String format(String sql) throws SqlParseException {
    return format(sql, null);
  }

  /**
   * @param sql SQL to format
   * @param databasePluginId Hop database plugin id, or {@code null} for ANSI
   * @return pretty-printed SQL
   * @throws SqlParseException if no statement in the script could be parsed
   */
  public static String format(String sql, String databasePluginId) throws SqlParseException {
    return format(sql, databasePluginId, CalciteSqlFormatConfig.current());
  }

  public static String format(
      String sql, String databasePluginId, CalciteSqlFormatConfig formatConfig)
      throws SqlParseException {
    if (StringUtils.isBlank(sql)) {
      return sql;
    }
    CalciteSqlStyle style = CalciteSqlDialects.of(databasePluginId);
    VariableMask mask = maskVariables(sql);
    try {
      return restoreTrailingNewline(
          sql, restoreVariables(formatParsed(mask.sql, style, formatConfig), mask));
    } catch (SqlParseException wholeScriptError) {
      List<String> statements = splitStatements(mask.sql);
      if (statements.size() <= 1) {
        throw wholeScriptError;
      }
      String formatted = formatStatements(statements, style, formatConfig, wholeScriptError);
      return restoreTrailingNewline(sql, restoreVariables(formatted, mask));
    }
  }

  private static String formatParsed(
      String sql, CalciteSqlStyle style, CalciteSqlFormatConfig formatConfig)
      throws SqlParseException {
    SqlNode node = parse(sql, style.lex());
    return formatNode(node, style, formatConfig, sql);
  }

  /**
   * Parse with the dialect lex, then retry other identifier-quoting styles. Calcite 1.42 {@link
   * Lex#JAVA} uses backticks, while Hop (PostgreSQL, ANSI, and others) emits {@code
   * "schema".table}.
   */
  private static SqlNode parse(String sql, Lex lex) throws SqlParseException {
    SqlParseException last = null;
    for (boolean ddl : new boolean[] {true, false}) {
      for (Quoting quoting : quotingAttempts(lex)) {
        try {
          return SqlParser.create(sql, parserConfig(lex, ddl, quoting)).parseStmtList();
        } catch (SqlParseException e) {
          last = e;
        }
      }
    }
    throw Objects.requireNonNull(last);
  }

  private static List<Quoting> quotingAttempts(Lex lex) {
    List<Quoting> attempts = new ArrayList<>();
    attempts.add(lex.quoting);
    for (Quoting quoting :
        List.of(
            Quoting.DOUBLE_QUOTE,
            Quoting.BACK_TICK,
            Quoting.BRACKET,
            Quoting.BACK_TICK_BACKSLASH)) {
      if (!attempts.contains(quoting)) {
        attempts.add(quoting);
      }
    }
    return attempts;
  }

  private static SqlParser.Config parserConfig(Lex lex, boolean ddl, Quoting quoting) {
    SqlParser.Config config =
        SqlParser.config()
            .withLex(lex)
            .withQuoting(quoting)
            .withQuotedCasing(Casing.UNCHANGED)
            .withUnquotedCasing(Casing.UNCHANGED)
            .withConformance(SqlConformanceEnum.LENIENT);
    if (ddl) {
      config = config.withParserFactory(SqlDdlParserImpl.FACTORY);
    }
    return config;
  }

  private static String formatNode(
      SqlNode node,
      CalciteSqlStyle style,
      CalciteSqlFormatConfig formatConfig,
      String originalSql) {
    if (node instanceof SqlNodeList list && list.size() > 1) {
      StringBuilder buffer = new StringBuilder();
      for (int i = 0; i < list.size(); i++) {
        if (i > 0) {
          buffer.append(";\n\n");
        }
        buffer.append(pretty(list.get(i), style, formatConfig).trim());
      }
      if (originalSql.trim().endsWith(";")) {
        buffer.append(';');
      }
      return buffer.toString();
    }
    SqlNode toFormat = node instanceof SqlNodeList list && list.size() == 1 ? list.get(0) : node;
    String formatted = pretty(toFormat, style, formatConfig).trim();
    if (originalSql.trim().endsWith(";")) {
      formatted = formatted + ';';
    }
    return formatted;
  }

  private static String pretty(
      SqlNode node, CalciteSqlStyle style, CalciteSqlFormatConfig formatConfig) {
    SqlPrettyWriter writer =
        new SqlPrettyWriter(SqlPrettyWriter.config().withDialect(style.dialect()));
    writer.setFormatOptions(formatConfig.toSqlFormatOptions());
    return writer.format(node);
  }

  private static String formatStatements(
      List<String> statements,
      CalciteSqlStyle style,
      CalciteSqlFormatConfig formatConfig,
      SqlParseException wholeScriptError)
      throws SqlParseException {
    List<String> formatted = new ArrayList<>(statements.size());
    boolean any = false;
    for (String statement : statements) {
      try {
        formatted.add(formatParsed(statement, style, formatConfig).trim());
        any = true;
      } catch (SqlParseException ignored) {
        formatted.add(statement.trim());
      }
    }
    if (!any) {
      throw wholeScriptError;
    }
    return String.join(";\n\n", formatted);
  }

  /**
   * Split on top-level semicolons, keeping quotes and comments intact. Empty fragments are dropped.
   */
  static List<String> splitStatements(String sql) {
    List<String> statements = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inSingle = false;
    boolean inDouble = false;
    boolean inBacktick = false;
    boolean inLineComment = false;
    boolean inBlockComment = false;
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      char next = i + 1 < sql.length() ? sql.charAt(i + 1) : 0;
      if (inLineComment) {
        current.append(c);
        if (c == '\n') {
          inLineComment = false;
        }
        continue;
      }
      if (inBlockComment) {
        current.append(c);
        if (c == '*' && next == '/') {
          current.append(next);
          i++;
          inBlockComment = false;
        }
        continue;
      }
      if (!inSingle && !inDouble && !inBacktick) {
        if (c == '-' && next == '-') {
          current.append(c);
          inLineComment = true;
          continue;
        }
        if (c == '/' && next == '*') {
          current.append(c);
          inBlockComment = true;
          continue;
        }
        if (c == ';') {
          String statement = current.toString().trim();
          if (!statement.isEmpty()) {
            statements.add(statement);
          }
          current.setLength(0);
          continue;
        }
      }
      if (c == '\'' && !inDouble && !inBacktick) {
        inSingle = !inSingle;
      } else if (c == '"' && !inSingle && !inBacktick) {
        inDouble = !inDouble;
      } else if (c == '`' && !inSingle && !inDouble) {
        inBacktick = !inBacktick;
      }
      current.append(c);
    }
    String tail = current.toString().trim();
    if (!tail.isEmpty()) {
      statements.add(tail);
    }
    return statements;
  }

  private static VariableMask maskVariables(String sql) {
    List<String> originals = new ArrayList<>();
    Matcher matcher = HOP_VARIABLE.matcher(sql);
    StringBuilder out = new StringBuilder();
    while (matcher.find()) {
      originals.add(matcher.group());
      matcher.appendReplacement(
          out, Matcher.quoteReplacement(PLACEHOLDER_PREFIX + (originals.size() - 1)));
    }
    matcher.appendTail(out);
    return new VariableMask(out.toString(), originals);
  }

  private static String restoreVariables(String sql, VariableMask mask) {
    String result = sql;
    for (int i = mask.originals.size() - 1; i >= 0; i--) {
      String original = mask.originals.get(i);
      String token = PLACEHOLDER_PREFIX + i;
      result = result.replace('"' + token + '"', original);
      result = result.replace('`' + token + '`', original);
      result = result.replace('[' + token + ']', original);
      result = result.replace(token, original);
    }
    return result;
  }

  private static String restoreTrailingNewline(String original, String formatted) {
    if (original.endsWith("\n") && !formatted.endsWith("\n")) {
      return formatted + "\n";
    }
    if (!original.endsWith("\n") && formatted.endsWith("\n")) {
      return formatted.substring(0, formatted.length() - 1);
    }
    return formatted;
  }

  private record VariableMask(String sql, List<String> originals) {}
}
