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
import static org.junit.jupiter.api.Assertions.assertTrue;

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

/** Golden SQL and bind layouts for every upsert form the DML builder writes. */
class DmlStatementBuilderTest {

  /** A dialect with a couple of reserved words, quoting with double quotes. */
  static class TestDialect extends ConcreteBaseDatabaseMeta {
    @Override
    public String[] getReservedWords() {
      return new String[] {"ORDER", "VALUE"};
    }
  }

  private static final DmlColumn ID = new DmlColumn("ID", "INTEGER");
  private static final DmlColumn REGION = new DmlColumn("REGION", "VARCHAR(10)");
  private static final DmlColumn NAME = new DmlColumn("NAME", "VARCHAR(50)");
  private static final DmlColumn AMOUNT = new DmlColumn("AMOUNT", "NUMERIC(10,2)");

  private static final DmlSyntax.Upsert MERGE = DmlSyntax.Upsert.of(UpsertForm.MERGE);

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

  private DmlStatement build(DmlSyntax syntax, DmlSyntax.Upsert upsert, DmlSpec spec)
      throws HopDatabaseException {
    return DmlStatementBuilder.upsert(databaseMeta, syntax, upsert, spec);
  }

  private static DmlSpec upsertSpec() throws HopDatabaseException {
    return DmlSpec.upsert("TGT", List.of(ID), List.of(NAME, AMOUNT), List.of(NAME, AMOUNT), false);
  }

  // ------------------------------------------------------------------------------ one-row MERGE

  private DmlStatement upsert(DmlSyntax syntax) throws HopDatabaseException {
    return build(syntax, syntax.upserts().get(0), upsertSpec());
  }

  private static DmlSyntax merge(DmlSyntax.RowSource rowSource, boolean cast, String terminator) {
    return DmlSyntax.builder()
        .rowSource(rowSource)
        .castParameters(cast)
        .upsert(MERGE.withTerminator(terminator))
        .build();
  }

  private static final String ON_UPDATE_INSERT =
      " ON (t.ID = s.ID)"
          + " WHEN MATCHED THEN UPDATE SET NAME = s.NAME, AMOUNT = s.AMOUNT"
          + " WHEN NOT MATCHED THEN INSERT (ID, NAME, AMOUNT) VALUES (s.ID, s.NAME, s.AMOUNT)";

  @Test
  void valuesSource() throws Exception {
    DmlStatement statement = upsert(DmlSyntax.STANDARD_MERGE);
    assertEquals(
        "MERGE INTO TGT t USING (VALUES (?, ?, ?)) AS s (ID, NAME, AMOUNT)" + ON_UPDATE_INSERT,
        statement.getSql());
    assertEquals(List.of(ID, NAME, AMOUNT), statement.getRowColumns());
    assertArrayEquals(new int[] {0, 1, 2}, statement.getLayout());
    assertEquals(3, statement.getParameterCount());
  }

  @Test
  void selectSource() throws Exception {
    assertEquals(
        "MERGE INTO TGT t USING (SELECT ? AS ID, ? AS NAME, ? AS AMOUNT) s" + ON_UPDATE_INSERT,
        upsert(merge(DmlSyntax.RowSource.SELECT, false, "")).getSql());
  }

  @Test
  void selectFromDualSource() throws Exception {
    assertEquals(
        "MERGE INTO TGT t USING (SELECT ? AS ID, ? AS NAME, ? AS AMOUNT FROM dual) s"
            + ON_UPDATE_INSERT,
        upsert(merge(DmlSyntax.RowSource.SELECT_FROM_DUAL, false, "")).getSql());
  }

  @Test
  void valuesWithCasts() throws Exception {
    assertEquals(
        "MERGE INTO TGT t USING (VALUES (CAST(? AS INTEGER), CAST(? AS VARCHAR(50)),"
            + " CAST(? AS NUMERIC(10,2)))) AS s (ID, NAME, AMOUNT)"
            + ON_UPDATE_INSERT,
        upsert(merge(DmlSyntax.RowSource.VALUES, true, "")).getSql());
  }

  @Test
  void selectWithCasts() throws Exception {
    assertEquals(
        "MERGE INTO TGT t USING (SELECT CAST(? AS INTEGER) AS ID, CAST(? AS VARCHAR(50)) AS NAME,"
            + " CAST(? AS NUMERIC(10,2)) AS AMOUNT FROM dual) s"
            + ON_UPDATE_INSERT,
        upsert(merge(DmlSyntax.RowSource.SELECT_FROM_DUAL, true, "")).getSql());
  }

  @Test
  void terminator() throws Exception {
    assertEquals(
        "MERGE INTO TGT t USING (VALUES (?, ?, ?)) AS s (ID, NAME, AMOUNT)"
            + ON_UPDATE_INSERT
            + ";",
        upsert(merge(DmlSyntax.RowSource.VALUES, false, ";")).getSql());
  }

  @Test
  void nullTerminatorIsNone() throws Exception {
    assertEquals(
        "MERGE INTO TGT t USING (VALUES (?, ?, ?)) AS s (ID, NAME, AMOUNT)" + ON_UPDATE_INSERT,
        upsert(merge(DmlSyntax.RowSource.VALUES, false, null)).getSql());
  }

  @Test
  void insertOnlyHasNoWhenMatched() throws Exception {
    DmlSpec spec = DmlSpec.upsert("TGT", List.of(ID), List.of(NAME, AMOUNT), List.of(), true);
    assertEquals(
        "MERGE INTO TGT t USING (VALUES (?, ?, ?)) AS s (ID, NAME, AMOUNT) ON (t.ID = s.ID)"
            + " WHEN NOT MATCHED THEN INSERT (ID, NAME, AMOUNT) VALUES (s.ID, s.NAME, s.AMOUNT)",
        build(DmlSyntax.STANDARD_MERGE, MERGE, spec).getSql());
  }

  @Test
  void updatingOnlyKeysIsInsertOnly() throws Exception {
    DmlSpec spec =
        DmlSpec.upsert(
            "TGT", List.of(ID), List.of(NAME), List.of(new DmlColumn("id", "INTEGER")), false);
    assertEquals(
        "MERGE INTO TGT t USING (VALUES (?, ?)) AS s (ID, NAME) ON (t.ID = s.ID)"
            + " WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (s.ID, s.NAME)",
        build(DmlSyntax.STANDARD_MERGE, MERGE, spec).getSql());
  }

  @Test
  void keysAreNeverInTheSetList() throws Exception {
    DmlSpec spec =
        DmlSpec.upsert("TGT", List.of(ID, REGION), List.of(NAME), List.of(ID, NAME, REGION), false);
    DmlStatement statement = build(DmlSyntax.STANDARD_MERGE, MERGE, spec);
    assertEquals(
        "MERGE INTO TGT t USING (VALUES (?, ?, ?)) AS s (ID, REGION, NAME)"
            + " ON (t.ID = s.ID AND t.REGION = s.REGION)"
            + " WHEN MATCHED THEN UPDATE SET NAME = s.NAME"
            + " WHEN NOT MATCHED THEN INSERT (ID, REGION, NAME) VALUES (s.ID, s.REGION, s.NAME)",
        statement.getSql());
    assertEquals(List.of(ID, REGION, NAME), statement.getRowColumns());
  }

  private static final String CHANGED_NAME =
      "(t.NAME <> s.NAME OR (t.NAME IS NULL AND s.NAME IS NOT NULL)"
          + " OR (t.NAME IS NOT NULL AND s.NAME IS NULL))";

  @Test
  void updateOnlyChangedComparesNullSafely() throws Exception {
    DmlSpec spec =
        DmlSpec.upsert("TGT", List.of(ID), List.of(NAME, AMOUNT), List.of(NAME, AMOUNT), true);
    assertEquals(
        "MERGE INTO TGT t USING (VALUES (?, ?, ?)) AS s (ID, NAME, AMOUNT) ON (t.ID = s.ID)"
            + " WHEN MATCHED AND ("
            + CHANGED_NAME
            + " OR (t.AMOUNT <> s.AMOUNT OR (t.AMOUNT IS NULL AND s.AMOUNT IS NOT NULL)"
            + " OR (t.AMOUNT IS NOT NULL AND s.AMOUNT IS NULL))"
            + ") THEN UPDATE SET NAME = s.NAME, AMOUNT = s.AMOUNT"
            + " WHEN NOT MATCHED THEN INSERT (ID, NAME, AMOUNT) VALUES (s.ID, s.NAME, s.AMOUNT)",
        build(DmlSyntax.STANDARD_MERGE, MERGE, spec).getSql());
  }

  @Test
  void updateOnlyChangedInUpdateWhere() throws Exception {
    DmlSyntax.Upsert oracleMerge =
        MERGE
            .withMinimumMajorVersion(10)
            .withChangedCondition(DmlSyntax.ChangedCondition.UPDATE_WHERE);
    DmlSyntax oracle =
        DmlSyntax.builder()
            .rowSource(DmlSyntax.RowSource.SELECT_FROM_DUAL)
            .upsert(oracleMerge)
            .build();
    DmlSpec changed = DmlSpec.upsert("TGT", List.of(ID), List.of(NAME), List.of(NAME), true);
    assertEquals(
        "MERGE INTO TGT t USING (SELECT ? AS ID, ? AS NAME FROM dual) s ON (t.ID = s.ID)"
            + " WHEN MATCHED THEN UPDATE SET NAME = s.NAME WHERE ("
            + CHANGED_NAME
            + ")"
            + " WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (s.ID, s.NAME)",
        build(oracle, oracleMerge, changed).getSql());
    // Without update-only-changed there is no condition at all.
    DmlSpec always = DmlSpec.upsert("TGT", List.of(ID), List.of(NAME), List.of(NAME), false);
    assertEquals(
        "MERGE INTO TGT t USING (SELECT ? AS ID, ? AS NAME FROM dual) s ON (t.ID = s.ID)"
            + " WHEN MATCHED THEN UPDATE SET NAME = s.NAME"
            + " WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (s.ID, s.NAME)",
        build(oracle, oracleMerge, always).getSql());
  }

  /**
   * A column the database can't compare with {@code <>} (a CLOB on Oracle, json on PostgreSQL)
   * would fail every row. Whether the row changed can't be told then: every matched row is updated,
   * in every form that compares.
   */
  @Test
  void anUncomparableColumnUpdatesEveryMatchedRow() throws Exception {
    DmlColumn notes = new DmlColumn("NOTES", "CLOB", false);
    DmlSpec spec =
        DmlSpec.upsert("TGT", List.of(ID), List.of(NAME, notes), List.of(NAME, notes), true);
    assertEquals(List.of(notes), spec.getUncomparableSetColumns());
    String always =
        " WHEN MATCHED THEN UPDATE SET NAME = s.NAME, NOTES = s.NOTES"
            + " WHEN NOT MATCHED THEN INSERT (ID, NAME, NOTES) VALUES (s.ID, s.NAME, s.NOTES)";
    assertEquals(
        "MERGE INTO TGT t USING (VALUES (?, ?, ?)) AS s (ID, NAME, NOTES) ON (t.ID = s.ID)"
            + always,
        build(DmlSyntax.STANDARD_MERGE, MERGE, spec).getSql());

    DmlSyntax.Upsert oracleMerge =
        MERGE.withChangedCondition(DmlSyntax.ChangedCondition.UPDATE_WHERE);
    DmlSyntax oracle =
        DmlSyntax.builder()
            .rowSource(DmlSyntax.RowSource.SELECT_FROM_DUAL)
            .upsert(oracleMerge)
            .build();
    assertEquals(
        "MERGE INTO TGT t USING (SELECT ? AS ID, ? AS NAME, ? AS NOTES FROM dual) s"
            + " ON (t.ID = s.ID)"
            + always,
        build(oracle, oracleMerge, spec).getSql());

    assertEquals(
        "INSERT INTO TGT (ID, NAME, NOTES) VALUES (?, ?, ?) ON CONFLICT (ID) DO UPDATE SET"
            + " NAME = EXCLUDED.NAME, NOTES = EXCLUDED.NOTES",
        build(DmlSyntax.builder().upsert(ON_CONFLICT).build(), ON_CONFLICT, spec).getSql());

    // An uncomparable column that is only inserted is never compared.
    DmlSpec insertedOnly =
        DmlSpec.upsert("TGT", List.of(ID), List.of(NAME, notes), List.of(NAME), true);
    assertTrue(insertedOnly.getUncomparableSetColumns().isEmpty());
    assertTrue(
        build(DmlSyntax.STANDARD_MERGE, MERGE, insertedOnly)
            .getSql()
            .contains("WHEN MATCHED AND ((t.NAME <> s.NAME"));
  }

  @Test
  void defaultChangedConditionIsWhenMatchedAnd() {
    assertEquals(DmlSyntax.ChangedCondition.WHEN_MATCHED_AND, MERGE.changedCondition());
    assertEquals(
        DmlSyntax.ChangedCondition.WHEN_MATCHED_AND,
        new DmlSyntax.Upsert(UpsertForm.MERGE, 0, 0, null, null, false).changedCondition());
  }

  @Test
  void columnsInSeveralListsAreBoundOnce() throws Exception {
    DmlColumn updatedAt = new DmlColumn("UPDATED_AT", "TIMESTAMP");
    DmlSpec spec =
        DmlSpec.upsert(
            "TGT",
            List.of(ID),
            // The key again, and NAME in another case: both already in the source row.
            List.of(new DmlColumn("id", "INTEGER"), NAME, new DmlColumn("name", "VARCHAR(50)")),
            // Only in the update: in the source row and SET, not in the INSERT.
            List.of(NAME, updatedAt),
            false);
    DmlStatement statement = build(DmlSyntax.STANDARD_MERGE, MERGE, spec);
    assertEquals(
        "MERGE INTO TGT t USING (VALUES (?, ?, ?)) AS s (ID, NAME, UPDATED_AT) ON (t.ID = s.ID)"
            + " WHEN MATCHED THEN UPDATE SET NAME = s.NAME, UPDATED_AT = s.UPDATED_AT"
            + " WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (s.ID, s.NAME)",
        statement.getSql());
    assertEquals(List.of(ID, NAME, updatedAt), statement.getRowColumns());
    assertEquals(List.of(ID, NAME, updatedAt), spec.getSourceColumns());
  }

  @Test
  void reservedWordsAndSpecialNamesAreQuoted() throws Exception {
    DmlColumn order = new DmlColumn("ORDER", "INTEGER");
    DmlColumn spaced = new DmlColumn("my col", "INTEGER");
    DmlColumn mixed = new DmlColumn("MixedCase", "INTEGER");
    DmlSpec spec =
        DmlSpec.upsert(
            "\"Target\"", List.of(order), List.of(spaced, mixed), List.of(spaced, mixed), false);
    assertEquals(
        "MERGE INTO \"Target\" t USING (VALUES (?, ?, ?)) AS s (\"ORDER\", \"my col\", MixedCase)"
            + " ON (t.\"ORDER\" = s.\"ORDER\")"
            + " WHEN MATCHED THEN UPDATE SET \"my col\" = s.\"my col\", MixedCase = s.MixedCase"
            + " WHEN NOT MATCHED THEN INSERT (\"ORDER\", \"my col\", MixedCase)"
            + " VALUES (s.\"ORDER\", s.\"my col\", s.MixedCase)",
        build(DmlSyntax.STANDARD_MERGE, MERGE, spec).getSql());
  }

  @Test
  void mixedCaseNamesAreQuotedWhenTheConnectionQuotesAllFields() throws Exception {
    databaseMeta.getIDatabase().setQuoteAllFields(true);
    DmlColumn mixed = new DmlColumn("MixedCase", "INTEGER");
    DmlSpec spec = DmlSpec.upsert("TGT", List.of(ID), List.of(mixed), List.of(mixed), false);
    assertEquals(
        "MERGE INTO TGT t USING (SELECT ? AS \"ID\", ? AS \"MixedCase\") s"
            + " ON (t.\"ID\" = s.\"ID\")"
            + " WHEN MATCHED THEN UPDATE SET \"MixedCase\" = s.\"MixedCase\""
            + " WHEN NOT MATCHED THEN INSERT (\"ID\", \"MixedCase\") VALUES (s.\"ID\","
            + " s.\"MixedCase\")",
        build(merge(DmlSyntax.RowSource.SELECT, false, ""), MERGE, spec).getSql());
  }

  @Test
  void castWithoutNativeTypeFails() {
    DmlSyntax casting = merge(DmlSyntax.RowSource.VALUES, true, "");
    for (String missing : new String[] {null, "", "  "}) {
      HopDatabaseException e =
          assertThrows(
              HopDatabaseException.class,
              () ->
                  build(
                      casting,
                      MERGE,
                      DmlSpec.upsert(
                          "TGT",
                          List.of(ID),
                          List.of(new DmlColumn("NAME", missing)),
                          List.of(),
                          false)));
      assertTrue(e.getMessage().contains("NAME"), e.getMessage());
    }
  }

  @Test
  void missingNativeTypeIsFineWithoutCasts() throws Exception {
    DmlSpec spec =
        DmlSpec.upsert(
            "TGT", List.of(DmlColumn.of("ID")), List.of(DmlColumn.of("NAME")), null, false);
    assertTrue(
        build(DmlSyntax.STANDARD_MERGE, MERGE, spec)
            .getSql()
            .startsWith("MERGE INTO TGT t USING (VALUES (?, ?))"));
  }

  @Test
  void noKeysFails() {
    assertThrows(
        HopDatabaseException.class,
        () -> DmlSpec.upsert("TGT", List.of(), List.of(NAME), List.of(NAME), false));
    assertThrows(
        HopDatabaseException.class,
        () -> DmlSpec.upsert("TGT", null, List.of(NAME), List.of(NAME), false));
  }

  @Test
  void anUnsupportedPlanIsRefusedWithItsReason() {
    HopDatabaseException e =
        assertThrows(
            HopDatabaseException.class,
            () ->
                DmlStatementBuilder.upsert(
                    databaseMeta, DmlPlan.unsupported(null, "no upsert here"), upsertSpec()));
    assertTrue(e.getMessage().contains("no upsert here"), e.getMessage());
  }

  @Test
  void aSupportedPlanBuildsItsForm() throws Exception {
    DmlPlan plan = DmlPlan.of(DmlSyntax.STANDARD_MERGE, MERGE);
    assertEquals(
        "MERGE INTO TGT t USING (VALUES (?, ?, ?)) AS s (ID, NAME, AMOUNT)" + ON_UPDATE_INSERT,
        DmlStatementBuilder.upsert(databaseMeta, plan, upsertSpec()).getSql());
  }

  @Test
  void anUpsertNeedsAForm() {
    assertThrows(
        HopDatabaseException.class, () -> build(DmlSyntax.STANDARD_MERGE, null, upsertSpec()));
  }

  // ------------------------------------------------------------------------------ ON CONFLICT

  private static final DmlSyntax.Upsert ON_CONFLICT = DmlSyntax.Upsert.of(UpsertForm.ON_CONFLICT);

  @Test
  void onConflictUpdates() throws Exception {
    DmlSyntax syntax = DmlSyntax.builder().upsert(ON_CONFLICT).build();
    DmlStatement statement = build(syntax, ON_CONFLICT, upsertSpec());
    assertEquals(
        "INSERT INTO TGT (ID, NAME, AMOUNT) VALUES "
            + "(?, ?, ?)"
            + " ON CONFLICT (ID) DO UPDATE SET NAME = EXCLUDED.NAME, AMOUNT = EXCLUDED.AMOUNT",
        statement.getSql());
    assertArrayEquals(new int[] {0, 1, 2}, statement.getLayout());
  }

  @Test
  void onConflictInsertOnlyDoesNothing() throws Exception {
    DmlSyntax syntax = DmlSyntax.builder().upsert(ON_CONFLICT).build();
    DmlSpec spec = DmlSpec.upsert("TGT", List.of(ID, REGION), List.of(NAME), List.of(), false);
    assertEquals(
        "INSERT INTO TGT (ID, REGION, NAME) VALUES (?, ?, ?)"
            + " ON CONFLICT (ID, REGION) DO NOTHING",
        build(syntax, ON_CONFLICT, spec).getSql());
  }

  @Test
  void onConflictUpdateOnlyChangedComparesWithTheExistingRow() throws Exception {
    DmlSyntax syntax = DmlSyntax.builder().upsert(ON_CONFLICT).build();
    DmlSpec spec = DmlSpec.upsert("TGT", List.of(ID), List.of(NAME), List.of(NAME), true);
    assertEquals(
        "INSERT INTO TGT AS t (ID, NAME) VALUES (?, ?) ON CONFLICT (ID) DO UPDATE SET NAME"
            + " = EXCLUDED.NAME WHERE ((t.NAME <> EXCLUDED.NAME OR (t.NAME IS NULL AND"
            + " EXCLUDED.NAME IS NOT NULL) OR (t.NAME IS NOT NULL AND EXCLUDED.NAME IS NULL)))",
        build(syntax, ON_CONFLICT, spec).getSql());
  }

  @Test
  void onConflictCantUpdateAColumnItDoesNotInsert() throws Exception {
    DmlSyntax syntax = DmlSyntax.builder().upsert(ON_CONFLICT).build();
    DmlColumn updatedAt = new DmlColumn("UPDATED_AT", "TIMESTAMP");
    DmlSpec spec =
        DmlSpec.upsert("TGT", List.of(ID), List.of(NAME), List.of(NAME, updatedAt), false);
    assertEquals(List.of(updatedAt), spec.getUpdateOnlyColumns());
    HopDatabaseException e =
        assertThrows(HopDatabaseException.class, () -> build(syntax, ON_CONFLICT, spec));
    assertTrue(e.getMessage().contains("UPDATED_AT"), e.getMessage());
  }

  // ------------------------------------------------------------------------------ ON DUPLICATE KEY

  private static final DmlSyntax.Upsert ON_DUPLICATE_KEY =
      DmlSyntax.Upsert.of(UpsertForm.ON_DUPLICATE_KEY);

  @Test
  void onDuplicateKeyUpdates() throws Exception {
    DmlSyntax syntax = DmlSyntax.builder().upsert(ON_DUPLICATE_KEY).build();
    assertEquals(
        "INSERT INTO TGT (ID, NAME, AMOUNT) VALUES "
            + "(?, ?, ?)"
            + " ON DUPLICATE KEY UPDATE NAME = VALUES(NAME), AMOUNT = VALUES(AMOUNT)",
        build(syntax, ON_DUPLICATE_KEY, upsertSpec()).getSql());
  }

  @Test
  void onDuplicateKeyInsertOnlyAssignsTheKeyToItself() throws Exception {
    DmlSyntax syntax = DmlSyntax.builder().upsert(ON_DUPLICATE_KEY).build();
    DmlSpec spec = DmlSpec.upsert("TGT", List.of(ID), List.of(NAME), List.of(), false);
    assertEquals(
        "INSERT INTO TGT (ID, NAME) VALUES (?, ?) ON DUPLICATE KEY UPDATE ID = ID",
        build(syntax, ON_DUPLICATE_KEY, spec).getSql());
  }

  @Test
  void onDuplicateKeyWithARowAliasNamesTheInsertedRow() throws Exception {
    DmlSyntax.Upsert alias = ON_DUPLICATE_KEY.withRowAlias(true);
    DmlSyntax syntax = DmlSyntax.builder().upsert(alias).build();
    assertEquals(
        "INSERT INTO TGT (ID, NAME, AMOUNT) VALUES "
            + "(?, ?, ?)"
            + " AS new ON DUPLICATE KEY UPDATE NAME = new.NAME, AMOUNT = new.AMOUNT",
        build(syntax, alias, upsertSpec()).getSql());
    DmlSpec insertOnly = DmlSpec.upsert("TGT", List.of(ID), List.of(NAME), List.of(), false);
    assertEquals(
        "INSERT INTO TGT (ID, NAME) VALUES (?, ?) AS new ON DUPLICATE KEY UPDATE ID = new.ID",
        build(syntax, alias, insertOnly).getSql());
  }

  // ------------------------------------------------------------------------------ syntax

  @Test
  void syntaxDefaults() {
    DmlSyntax syntax = DmlSyntax.builder().build();
    assertEquals(DmlSyntax.RowSource.VALUES, syntax.rowSource());
    assertTrue(syntax.upserts().isEmpty());
    assertEquals(false, syntax.castParameters());
    assertEquals(
        "3.24", MERGE.withMinimumMajorVersion(3).withMinimumMinorVersion(24).minimumVersion());
    assertEquals("15", MERGE.withMinimumMajorVersion(15).minimumVersion());
  }

  @Test
  void enumsHaveDescriptions() {
    for (UpsertForm form : UpsertForm.values()) {
      assertTrue(!form.getDescription().startsWith("!"), form.getDescription());
      assertEquals(form, UpsertForm.lookupCode(form.getCode().toLowerCase()));
    }
    assertEquals(null, UpsertForm.lookupCode(" "));
  }
}
