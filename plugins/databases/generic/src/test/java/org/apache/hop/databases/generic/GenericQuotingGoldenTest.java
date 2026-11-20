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

package org.apache.hop.databases.generic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Locale;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Pins how identifiers are recognised as reserved words and quoted for this dialect, so caching the
 * reserved-word lookup cannot change a generated statement.
 */
class GenericQuotingGoldenTest {

  /** word, isReservedWord, quoteField */
  private static final String[][] WORDS = {
    {"select", "false", "select"},
    {"SELECT", "false", "SELECT"},
    {"Select", "false", "Select"},
    {"user", "false", "user"},
    {"USER", "false", "USER"},
    {"order", "false", "order"},
    {"Order", "false", "Order"},
    {"value", "false", "value"},
    {"name", "false", "name"},
    {"customer", "false", "customer"},
    {"id", "false", "id"},
    {"first name", "false", "\"first name\""},
    {"a.b", "false", "\"a.b\""},
    {"date", "false", "date"},
    {"timestamp", "false", "timestamp"},
    {"key", "false", "key"},
    {"comment", "false", "comment"},
    {"level", "false", "level"},
    {"size", "false", "size"},
    {"rank", "false", "rank"},
    {"limit", "false", "limit"},
    {"Mixed_Case", "false", "Mixed_Case"},
    {"sElEcT", "false", "sElEcT"},
  };

  /** schema, table, getQuotedSchemaTableCombination */
  private static final String[][] SCHEMA_TABLES = {
    {"public", "order", "public.order"},
    {null, "user", "user"},
    {"sales", "customer", "sales.customer"},
    {"db.dbo", "select", "db.dbo.select"},
    {"My Schema", "Group", "\"My Schema\".Group"},
  };

  private DatabaseMeta databaseMeta;

  @BeforeAll
  static void setUpOnce() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new GenericDatabaseMeta());
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

  @Test
  void reservedWordsFollowTheSelectedDialect() {
    GenericDatabaseMeta generic = new GenericDatabaseMeta();
    databaseMeta.setIDatabase(generic);
    assertFalse(databaseMeta.isReservedWord("select"));
    assertEquals("select", databaseMeta.quoteField("select"));

    IDatabase dialect =
        Mockito.mock(
            IDatabase.class, Mockito.withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));
    Mockito.doReturn("FakeDialect").when(dialect).getPluginName();
    Mockito.doReturn(new String[] {"Select", "ORDER"}).when(dialect).getReservedWords();
    Mockito.doReturn("\"").when(dialect).getStartQuote();
    Mockito.doReturn("\"").when(dialect).getEndQuote();
    Mockito.doReturn(true).when(dialect).isQuoteReservedWords();
    try (MockedStatic<DatabaseMeta> dbMetaStatic =
        Mockito.mockStatic(DatabaseMeta.class, Mockito.CALLS_REAL_METHODS)) {
      dbMetaStatic.when(DatabaseMeta::getDatabaseInterfaces).thenReturn(new IDatabase[] {dialect});
      generic.setDatabaseDialect("FakeDialect");
    }
    assertTrue(databaseMeta.isReservedWord("select"));
    assertTrue(databaseMeta.isReservedWord("order"));
    assertEquals("\"select\"", databaseMeta.quoteField("select"));

    // Back to the generic dialect: nothing is reserved any more
    generic.setDatabaseDialect(generic.getPluginName());
    assertFalse(databaseMeta.isReservedWord("select"));
    assertEquals("select", databaseMeta.quoteField("select"));
  }
}
