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

package org.apache.hop.core.database.dml;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.ConcreteBaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Golden SQL and bind layouts of the INSERT statements the DML builder writes, of one row and of
 * several rows; {@link DmlStatementBuilderTest} covers the upserts.
 */
class DmlStatementBuilderMultiRowTest {

  /** A dialect with a couple of reserved words, quoting with double quotes. */
  static class TestDialect extends ConcreteBaseDatabaseMeta {
    @Override
    public String[] getReservedWords() {
      return new String[] {"ORDER", "VALUE"};
    }
  }

  private static final DmlColumn ID = new DmlColumn("ID", "INTEGER");
  private static final DmlColumn NAME = new DmlColumn("NAME", "VARCHAR(50)");
  private static final DmlColumn AMOUNT = new DmlColumn("AMOUNT", "NUMERIC(10,2)");

  @BeforeAll
  static void setUpOnce() throws HopException {
    DatabasePluginType.getInstance().searchPlugins();
    HopClientEnvironment.init();
  }

  private DatabaseMeta databaseMeta;

  @BeforeEach
  void setUp() {
    databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new TestDialect());
  }

  private static String repeat(String part, int times, String separator) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < times; i++) {
      result.append(i == 0 ? "" : separator).append(part);
    }
    return result.toString();
  }

  private DmlStatement insert(DmlSyntax syntax, DmlSpec spec, int rows)
      throws HopDatabaseException {
    return DmlStatementBuilder.insert(databaseMeta, syntax, spec, rows);
  }

  private static DmlSpec insertSpec() throws HopDatabaseException {
    return DmlSpec.insert("TGT", List.of(ID, NAME, AMOUNT));
  }

  @Test
  void oneRowInsertNeedsNoSyntax() throws Exception {
    DmlStatement statement = insert(null, insertSpec(), 1);
    assertEquals("INSERT INTO TGT (ID, NAME, AMOUNT) VALUES (?, ?, ?)", statement.getSql());
    assertArrayEquals(new int[] {0, 1, 2}, statement.getLayout());
    assertEquals(1, statement.getRows());
    assertEquals(3, statement.getParameterCount());
  }

  @Test
  void reservedWordsAreQuoted() throws Exception {
    DmlSpec spec = DmlSpec.insert("TGT", List.of(DmlColumn.of("ORDER"), DmlColumn.of("VALUE"), ID));
    DmlSyntax syntax = DmlSyntax.builder().insertForm(DmlSyntax.InsertForm.VALUES_LIST).build();
    assertEquals(
        "INSERT INTO TGT (\"ORDER\", \"VALUE\", ID) VALUES (?, ?, ?), (?, ?, ?)",
        insert(syntax, spec, 2).getSql());
  }

  @Test
  void aValuesListOfSeveralRowsIsCastWhenAsked() throws Exception {
    DmlSyntax syntax =
        DmlSyntax.builder()
            .castParameters(true)
            .castValuesLists(true)
            .insertForm(DmlSyntax.InsertForm.VALUES_LIST)
            .build();
    String row = "(CAST(? AS INTEGER), CAST(? AS VARCHAR(50)), CAST(? AS NUMERIC(10,2)))";
    assertEquals(
        "INSERT INTO TGT (ID, NAME, AMOUNT) VALUES " + row + ", " + row,
        insert(syntax, insertSpec(), 2).getSql());
    // One row is assigned to the columns directly.
    assertEquals(
        "INSERT INTO TGT (ID, NAME, AMOUNT) VALUES (?, ?, ?)",
        insert(syntax, insertSpec(), 1).getSql());
    // Without castValuesLists, or without castParameters, nothing is cast.
    assertEquals(
        "INSERT INTO TGT (ID, NAME, AMOUNT) VALUES (?, ?, ?), (?, ?, ?)",
        insert(syntax.withCastValuesLists(false), insertSpec(), 2).getSql());
    assertEquals(
        "INSERT INTO TGT (ID, NAME, AMOUNT) VALUES (?, ?, ?), (?, ?, ?)",
        insert(syntax.withCastParameters(false), insertSpec(), 2).getSql());
  }

  @Test
  void aCastNeedsTheColumnType() throws Exception {
    DmlSyntax syntax =
        DmlSyntax.builder()
            .castParameters(true)
            .castValuesLists(true)
            .insertForm(DmlSyntax.InsertForm.VALUES_LIST)
            .build();
    DmlSpec untyped = DmlSpec.insert("TGT", List.of(DmlColumn.of("ID")));
    assertEquals("INSERT INTO TGT (ID) VALUES (?)", insert(syntax, untyped, 1).getSql());
    assertThrows(HopDatabaseException.class, () -> insert(syntax, untyped, 2));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 5})
  void insertValuesList(int rows) throws Exception {
    DmlSyntax syntax = DmlSyntax.builder().insertForm(DmlSyntax.InsertForm.VALUES_LIST).build();
    DmlStatement statement = insert(syntax, insertSpec(), rows);
    assertEquals(
        "INSERT INTO TGT (ID, NAME, AMOUNT) VALUES " + repeat("(?, ?, ?)", rows, ", "),
        statement.getSql());
    assertEquals(rows, statement.getRows());
    assertEquals(3, statement.getParametersPerRow());
    assertEquals(rows * 3, statement.getParameterCount());
    assertArrayEquals(new int[] {0, 1, 2}, statement.getLayout());
    assertEquals(1, statement.parameterIndex(0, 0));
    assertEquals(4, statement.parameterIndex(1, 0));
    assertEquals(rows * 3, statement.parameterIndex(rows - 1, 2));
  }

  @Test
  void severalRowsNeedADeclaredForm() throws Exception {
    DmlSyntax none = DmlSyntax.builder().build();
    for (DmlSyntax syntax : new DmlSyntax[] {null, none}) {
      assertThrows(HopDatabaseException.class, () -> insert(syntax, insertSpec(), 2));
    }
    assertThrows(HopDatabaseException.class, () -> insert(none, insertSpec(), 0));
    DmlSpec upsert = DmlSpec.upsert("TGT", List.of(ID), List.of(NAME), List.of(NAME), false);
    assertThrows(HopDatabaseException.class, () -> insert(DmlSyntax.STANDARD_MERGE, upsert, 1));
  }

  @Test
  void anInsertNeedsAColumn() {
    assertThrows(HopDatabaseException.class, () -> DmlSpec.insert("TGT", List.of()));
  }

  @Test
  void aPlanThatIsNotSupportedIsRefused() {
    DmlPlan plan = DmlPlan.unsupported(DmlOperation.INSERT, null, "no connection");
    HopDatabaseException e =
        assertThrows(
            HopDatabaseException.class,
            () -> DmlStatementBuilder.insert(databaseMeta, plan, insertSpec(), 2));
    assertEquals("no connection", e.getMessage().trim());
  }
}
