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

package org.apache.hop.calcite.config;

import java.util.function.Consumer;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.pretty.SqlFormatOptions;
import org.apache.hop.core.Const;

/**
 * Persisted Apache Calcite {@link SqlFormatOptions} used by the SQL formatter toolbar button.
 *
 * <p>Defaults match the formatter's original layout (readable SELECT lists, unquoted identifiers)
 * rather than Calcite's {@link SqlFormatOptions} no-arg constructor.
 */
@Getter
@Setter
public class CalciteSqlFormatConfig {

  public static final String HOP_CONFIG_KEY = "calciteSqlFormat";

  private boolean alwaysUseParentheses;
  private boolean caseClausesOnNewLines = true;
  private boolean clauseStartsLine = true;
  private boolean keywordsLowercase;
  private boolean quoteAllIdentifiers;
  private boolean selectListItemsOnSeparateLines = true;
  private boolean whereListItemsOnSeparateLines = true;
  private boolean windowDeclarationStartsLine = true;
  private boolean windowListItemsOnSeparateLines = true;
  private int indentation = 2;
  private int lineLength;

  public CalciteSqlFormatConfig() {}

  public CalciteSqlFormatConfig(CalciteSqlFormatConfig other) {
    this.alwaysUseParentheses = other.alwaysUseParentheses;
    this.caseClausesOnNewLines = other.caseClausesOnNewLines;
    this.clauseStartsLine = other.clauseStartsLine;
    this.keywordsLowercase = other.keywordsLowercase;
    this.quoteAllIdentifiers = other.quoteAllIdentifiers;
    this.selectListItemsOnSeparateLines = other.selectListItemsOnSeparateLines;
    this.whereListItemsOnSeparateLines = other.whereListItemsOnSeparateLines;
    this.windowDeclarationStartsLine = other.windowDeclarationStartsLine;
    this.windowListItemsOnSeparateLines = other.windowListItemsOnSeparateLines;
    this.indentation = other.indentation;
    this.lineLength = other.lineLength;
  }

  /**
   * Apply values from the configuration GUI / CLI plugin. {@code null} fields are left unchanged so
   * {@code hop conf} can set a subset of options.
   *
   * @param plugin source values
   * @return true if any field changed
   */
  public boolean applyFrom(CalciteSqlFormatConfigPlugin plugin) {
    boolean changed = false;
    changed |= applyBoolean(plugin.getAlwaysUseParentheses(), v -> alwaysUseParentheses = v);
    changed |= applyBoolean(plugin.getCaseClausesOnNewLines(), v -> caseClausesOnNewLines = v);
    changed |= applyBoolean(plugin.getClauseStartsLine(), v -> clauseStartsLine = v);
    changed |= applyBoolean(plugin.getKeywordsLowercase(), v -> keywordsLowercase = v);
    changed |= applyBoolean(plugin.getQuoteAllIdentifiers(), v -> quoteAllIdentifiers = v);
    changed |=
        applyBoolean(
            plugin.getSelectListItemsOnSeparateLines(), v -> selectListItemsOnSeparateLines = v);
    changed |=
        applyBoolean(
            plugin.getWhereListItemsOnSeparateLines(), v -> whereListItemsOnSeparateLines = v);
    changed |=
        applyBoolean(plugin.getWindowDeclarationStartsLine(), v -> windowDeclarationStartsLine = v);
    changed |=
        applyBoolean(
            plugin.getWindowListItemsOnSeparateLines(), v -> windowListItemsOnSeparateLines = v);
    if (plugin.getIndentation() != null) {
      int value = Math.max(0, Const.toInt(plugin.getIndentation(), indentation));
      if (value != indentation) {
        indentation = value;
        changed = true;
      }
    }
    if (plugin.getLineLength() != null) {
      int value = Math.max(0, Const.toInt(plugin.getLineLength(), lineLength));
      if (value != lineLength) {
        lineLength = value;
        changed = true;
      }
    }
    return changed;
  }

  public SqlFormatOptions toSqlFormatOptions() {
    return new SqlFormatOptions(
        alwaysUseParentheses,
        caseClausesOnNewLines,
        clauseStartsLine,
        keywordsLowercase,
        quoteAllIdentifiers,
        selectListItemsOnSeparateLines,
        whereListItemsOnSeparateLines,
        windowDeclarationStartsLine,
        windowListItemsOnSeparateLines,
        indentation,
        lineLength);
  }

  /**
   * Config currently in effect for formatting. Falls back to defaults when Hop config is not
   * available (unit tests).
   */
  public static CalciteSqlFormatConfig current() {
    try {
      return CalciteSqlFormatConfigSingleton.getConfig();
    } catch (Throwable t) {
      return new CalciteSqlFormatConfig();
    }
  }

  private static boolean applyBoolean(Boolean incoming, Consumer<Boolean> setter) {
    if (incoming == null) {
      return false;
    }
    setter.accept(incoming);
    return true;
  }
}
