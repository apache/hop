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

package org.apache.hop.core.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.util.HashMap;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

/**
 * Pins how {@link DatabaseMeta} recognises reserved words and quotes identifiers, so that the
 * reserved-word lookup can be cached without changing a single quoted name.
 */
class ReservedWordQuotingTest {

  /** A dialect with a small, mixed-case reserved-word list. */
  static class MixedCaseDialect extends ConcreteBaseDatabaseMeta {
    int reservedWordCalls;

    @Override
    public String[] getReservedWords() {
      reservedWordCalls++;
      return new String[] {"SELECT", "Order", "user", "MiXeD", "GROUP"};
    }
  }

  private DatabaseMeta databaseMeta;
  private MixedCaseDialect dialect;
  private IVariables variables;

  @BeforeAll
  static void setUpOnce() throws HopException {
    DatabasePluginType.getInstance().searchPlugins();
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    databaseMeta = new DatabaseMeta();
    dialect = new MixedCaseDialect();
    databaseMeta.setIDatabase(dialect);
    variables = new Variables();
  }

  @Test
  void reservedWordLookupIgnoresCase() {
    for (String word :
        new String[] {
          "SELECT", "select", "SeLeCt", "ORDER", "order", "Order", "USER", "user", "mixed", "MIXED"
        }) {
      assertTrue(databaseMeta.isReservedWord(word), word);
    }
    for (String word : new String[] {"selects", "sel", "ORDER_ID", " user", "user ", "customer"}) {
      assertFalse(databaseMeta.isReservedWord(word), word);
    }
  }

  @Test
  void nullAndEmptyAreNotReserved() {
    assertFalse(databaseMeta.isReservedWord(null));
    assertFalse(databaseMeta.isReservedWord(""));
    assertNull(databaseMeta.quoteField(null));
    assertNull(databaseMeta.quoteField(""));
  }

  @Test
  void dialectWithoutReservedWords() {
    databaseMeta.setIDatabase(new ConcreteBaseDatabaseMeta());
    assertFalse(databaseMeta.isReservedWord("select"));
    assertEquals("select", databaseMeta.quoteField("select"));
  }

  @Test
  void dialectReturningNullReservedWords() {
    databaseMeta.setIDatabase(
        new ConcreteBaseDatabaseMeta() {
          @Override
          public String[] getReservedWords() {
            return null;
          }
        });
    assertFalse(databaseMeta.isReservedWord("select"));
    assertEquals("select", databaseMeta.quoteField("select"));
  }

  @Test
  void quoteFieldPreservesCaseOfReservedWordsByDefault() {
    assertEquals("\"select\"", databaseMeta.quoteField("select"));
    assertEquals("\"Order\"", databaseMeta.quoteField("Order"));
    assertEquals("\"MIXED\"", databaseMeta.quoteField("MIXED"));
    assertEquals("customer", databaseMeta.quoteField("customer"));
    assertEquals("Customer_Id", databaseMeta.quoteField("Customer_Id"));
  }

  @Test
  void quoteFieldFoldsReservedWordCaseWhenNotPreserving() {
    dialect.setPreserveReservedCase(false);
    // BaseDatabaseMeta defaults to upper case
    assertEquals("\"SELECT\"", databaseMeta.quoteField("select"));
    assertEquals("\"ORDER\"", databaseMeta.quoteField("Order"));
    assertEquals("customer", databaseMeta.quoteField("customer"));
  }

  @Test
  void quoteFieldOtherReasons() {
    assertEquals("\"first name\"", databaseMeta.quoteField("first name"));
    assertEquals("\"a.b\"", databaseMeta.quoteField("a.b"));
    assertEquals("\"a-b\"", databaseMeta.quoteField("a-b"));
    // already quoted: untouched
    assertEquals("\"select\"", databaseMeta.quoteField("\"select\""));
    assertEquals("x\"y", databaseMeta.quoteField("x\"y"));
  }

  @Test
  void quoteFieldWithForcedIdentifierCase() {
    dialect.setForcingIdentifiersToLowerCase(true);
    assertEquals("\"select\"", databaseMeta.quoteField("SELECT"));
    assertEquals("customer", databaseMeta.quoteField("CUSTOMER"));

    dialect.setForcingIdentifiersToLowerCase(false);
    dialect.setForcingIdentifiersToUpperCase(true);
    assertEquals("\"ORDER\"", databaseMeta.quoteField("order"));
    assertEquals("CUSTOMER", databaseMeta.quoteField("customer"));
  }

  @Test
  void quoteAllFields() {
    dialect.setQuoteAllFields(true);
    assertEquals("\"customer\"", databaseMeta.quoteField("customer"));
    assertEquals("\"select\"", databaseMeta.quoteField("select"));
  }

  @Test
  void quotedSchemaTableCombination() {
    assertEquals(
        "\"user\".\"order\"",
        databaseMeta.getQuotedSchemaTableCombination(variables, "user", "order"));
    assertEquals(
        "sales.customer",
        databaseMeta.getQuotedSchemaTableCombination(variables, "sales", "customer"));
    assertEquals(
        "\"Group\"", databaseMeta.getQuotedSchemaTableCombination(variables, null, "Group"));
    assertEquals("\"Group\"", databaseMeta.getQuotedSchemaTableCombination(variables, "", "Group"));
    assertEquals(
        "db.\"user\".\"select\"",
        databaseMeta.getQuotedSchemaTableCombination(variables, "db.user", "select"));

    variables.setVariable("SCHEMA", "user");
    variables.setVariable("TABLE", "my table");
    assertEquals(
        "\"user\".\"my table\"",
        databaseMeta.getQuotedSchemaTableCombination(variables, "${SCHEMA}", "${TABLE}"));
  }

  @Test
  void isInNeedOfQuoting() {
    assertTrue(databaseMeta.isInNeedOfQuoting("select"));
    assertTrue(databaseMeta.isInNeedOfQuoting("first name"));
    assertFalse(databaseMeta.isInNeedOfQuoting("customer"));
  }

  @Test
  void reservedWordsOfNonBaseDialectAreHonoured() {
    // An IDatabase that does not extend BaseDatabaseMeta only provides getReservedWords().
    IDatabase plain =
        mock(IDatabase.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));
    doReturn(new String[] {"Select", "FROM"}).when(plain).getReservedWords();
    doReturn("\"").when(plain).getStartQuote();
    doReturn("\"").when(plain).getEndQuote();
    doReturn(true).when(plain).isQuoteReservedWords();
    doReturn(true).when(plain).isPreserveReservedCase();
    doReturn(new HashMap<String, String>()).when(plain).getAttributes();
    databaseMeta.setIDatabase(plain);

    assertTrue(databaseMeta.isReservedWord("select"));
    assertTrue(databaseMeta.isReservedWord("from"));
    assertFalse(databaseMeta.isReservedWord("where"));
  }

  @Test
  void reservedWordsAreReadOnceAndSharedByClones() {
    for (int i = 0; i < 100; i++) {
      databaseMeta.isReservedWord("select");
      databaseMeta.quoteField("customer");
    }
    assertEquals(1, dialect.reservedWordCalls);

    MixedCaseDialect clone = (MixedCaseDialect) dialect.clone();
    assertTrue(clone.isReservedWord("ORDER"));
    assertEquals(1, clone.reservedWordCalls);

    dialect.clearReservedWordsCache();
    assertTrue(databaseMeta.isReservedWord("group"));
    assertEquals(2, dialect.reservedWordCalls);
  }

  @Test
  void foldingMatchesEqualsIgnoreCase() {
    String[] samples = {
      "select",
      "SELECT",
      "Straße",
      "STRASSE",
      "strasse",
      "STRAßE",
      "İndex",
      "index",
      "ındex",
      "INDEX",
      "ÿes",
      "ŸES",
      "µ",
      "Μ",
      "μ",
      "ǅ",
      "ǆ",
      "Ǆ",
      "\uD801\uDC00",
      "\uD801\uDC28",
      "K",
      "\u212A",
      "k",
      "",
      "a b",
      "ﬀ",
      "FF"
    };
    for (String reserved : samples) {
      ReservedWords words = ReservedWords.of(new String[] {reserved});
      for (String candidate : samples) {
        assertEquals(
            reserved.equalsIgnoreCase(candidate),
            words.contains(candidate),
            reserved + " vs " + candidate);
      }
    }
  }

  @Test
  void reservedWordsHandlesNullEntries() {
    ReservedWords words = ReservedWords.of(new String[] {null, "Select"});
    assertTrue(words.contains("SELECT"));
    assertFalse(words.contains(null));
    assertFalse(ReservedWords.of(null).contains("select"));
  }
}
