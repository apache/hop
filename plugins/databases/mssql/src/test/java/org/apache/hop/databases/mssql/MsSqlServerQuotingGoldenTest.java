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

package org.apache.hop.databases.mssql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Locale;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins how identifiers are recognised as reserved words and quoted for this dialect, so caching the
 * reserved-word lookup cannot change a generated statement.
 */
class MsSqlServerQuotingGoldenTest {

  /** word, isReservedWord, quoteField */
  private static final String[][] WORDS = {
    {"select", "true", "[select]"},
    {"SELECT", "true", "[SELECT]"},
    {"Select", "true", "[Select]"},
    {"user", "true", "[user]"},
    {"USER", "true", "[USER]"},
    {"order", "true", "[order]"},
    {"Order", "true", "[Order]"},
    {"value", "true", "[value]"},
    {"name", "false", "name"},
    {"customer", "false", "customer"},
    {"id", "false", "id"},
    {"first name", "false", "[first name]"},
    {"a.b", "false", "[a.b]"},
    {"date", "true", "[date]"},
    {"timestamp", "true", "[timestamp]"},
    {"key", "true", "[key]"},
    {"comment", "false", "comment"},
    {"level", "true", "[level]"},
    {"size", "true", "[size]"},
    {"rank", "false", "rank"},
    {"limit", "true", "[limit]"},
    {"Mixed_Case", "false", "Mixed_Case"},
    {"sElEcT", "true", "[sElEcT]"},
  };

  /** schema, table, getQuotedSchemaTableCombination */
  private static final String[][] SCHEMA_TABLES = {
    {"public", "order", "[public].[order]"},
    {null, "user", "[user]"},
    {"sales", "customer", "sales.customer"},
    {"db.dbo", "select", "db.dbo.[select]"},
    {"My Schema", "Group", "[My Schema].[Group]"},
  };

  private DatabaseMeta databaseMeta;

  @BeforeAll
  static void setUpOnce() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new MsSqlServerDatabaseMeta());
  }

  @Test
  void reservedWordsAndQuoting() {
    for (String[] row : WORDS) {
      assertEquals(Boolean.parseBoolean(row[1]), databaseMeta.isReservedWord(row[0]), row[0]);
      assertEquals(row[2], databaseMeta.quoteField(row[0]), row[0]);
    }
  }

  @Test
  void nullAndEmpty() {
    assertFalse(databaseMeta.isReservedWord(null));
    assertFalse(databaseMeta.isReservedWord(""));
    assertNull(databaseMeta.quoteField(null));
    assertNull(databaseMeta.quoteField(""));
  }

  @Test
  void quotedSchemaTableCombination() {
    Variables variables = new Variables();
    for (String[] row : SCHEMA_TABLES) {
      assertEquals(
          row[2], databaseMeta.getQuotedSchemaTableCombination(variables, row[0], row[1]), row[1]);
    }
  }

  @Test
  void everyListedWordIsReservedInAnyCase() {
    for (String word : databaseMeta.getReservedWords()) {
      assertTrue(databaseMeta.isReservedWord(word), word);
      assertTrue(databaseMeta.isReservedWord(word.toLowerCase(Locale.ROOT)), word);
      assertTrue(databaseMeta.isReservedWord(word.toUpperCase(Locale.ROOT)), word);
    }
  }
}
