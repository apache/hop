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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.SqlWriterConfig.LineFolding;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.hop.calcite.CalciteSqlFormatter;
import org.junit.jupiter.api.Test;

class CalciteSqlFormatConfigTest {

  @Test
  void toSqlFormatOptionsCopiesEveryField() {
    CalciteSqlFormatConfig config = new CalciteSqlFormatConfig();
    config.setAlwaysUseParentheses(true);
    config.setCaseClausesOnNewLines(false);
    config.setClauseStartsLine(false);
    config.setLeadingComma(true);
    config.setKeywordsLowercase(true);
    config.setQuoteAllIdentifiers(true);
    config.setSelectListItemsOnSeparateLines(false);
    config.setFromListItemsOnSeparateLines(false);
    config.setWhereListItemsOnSeparateLines(false);
    config.setGroupByListItemsOnSeparateLines(false);
    config.setOrderByListItemsOnSeparateLines(false);
    config.setWindowDeclarationStartsLine(false);
    config.setWindowListItemsOnSeparateLines(false);
    config.setIndentation(8);
    config.setLineLength(80);

    SqlWriterConfig writerConfig = config.applySqlFormat(SqlPrettyWriter.config());

    assertTrue(writerConfig.alwaysUseParentheses());
    assertFalse(writerConfig.caseClausesOnNewLines());
    assertFalse(writerConfig.clauseStartsLine());
    assertFalse(writerConfig.clauseEndsLine());
    assertTrue(writerConfig.leadingComma());
    assertTrue(writerConfig.keywordsLowerCase());
    assertTrue(writerConfig.quoteAllIdentifiers());
    assertEquals(LineFolding.FOLD, writerConfig.selectFolding());
    assertEquals(LineFolding.FOLD, writerConfig.fromFolding());
    assertEquals(LineFolding.FOLD, writerConfig.whereFolding());
    assertEquals(LineFolding.FOLD, writerConfig.groupByFolding());
    assertEquals(LineFolding.FOLD, writerConfig.orderByFolding());
    assertEquals(LineFolding.FOLD, writerConfig.windowFolding());
    assertEquals(LineFolding.FOLD, writerConfig.overFolding());

    assertEquals(8, writerConfig.indentation());
    assertEquals(80, writerConfig.lineLength());
  }

  @Test
  void applyFromIgnoresNullPluginFields() {
    CalciteSqlFormatConfig config = new CalciteSqlFormatConfig();
    config.setIndentation(2);
    config.setKeywordsLowercase(false);

    CalciteSqlFormatConfigPlugin plugin = new CalciteSqlFormatConfigPlugin();
    plugin.setKeywordsLowercase(true);
    plugin.setIndentation("6");

    assertTrue(config.applyFrom(plugin));
    assertTrue(config.isKeywordsLowercase());
    assertEquals(6, config.getIndentation());
    assertTrue(config.isSelectListItemsOnSeparateLines());
  }

  @Test
  void keywordsLowercaseIsAppliedWhenFormatting() throws Exception {
    CalciteSqlFormatConfig config = new CalciteSqlFormatConfig();
    config.setKeywordsLowercase(true);
    config.setSelectListItemsOnSeparateLines(false);
    String formatted = CalciteSqlFormatter.format("SELECT a FROM customers", null, config);
    assertTrue(formatted.contains("select"));
    assertFalse(formatted.contains("SELECT"));
  }

  @Test
  void pluginCopiesFromPersistedConfig() {
    CalciteSqlFormatConfig config = new CalciteSqlFormatConfig();
    config.setIndentation(8);
    config.setQuoteAllIdentifiers(true);
    CalciteSqlFormatConfigPlugin plugin = new CalciteSqlFormatConfigPlugin(config);
    assertEquals("8", plugin.getIndentation());
    assertTrue(plugin.getQuoteAllIdentifiers());
  }
}
