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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.ConcreteBaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.dml.FakeCatalog;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The selection rules of {@link DmlSupport}: upsert form, versions, unique keys. */
class DmlSupportTest {

  private static final DmlSyntax.Upsert MERGE = DmlSyntax.Upsert.of(UpsertForm.MERGE);
  private static final DmlSyntax.Upsert ON_CONFLICT = DmlSyntax.Upsert.of(UpsertForm.ON_CONFLICT);
  private static final DmlSyntax.Upsert ON_DUPLICATE_KEY =
      DmlSyntax.Upsert.of(UpsertForm.ON_DUPLICATE_KEY);

  @BeforeAll
  static void setUpOnce() throws HopException {
    DatabasePluginType.getInstance().searchPlugins();
    HopClientEnvironment.init();
  }

  private static DatabaseMeta connection(DmlSyntax syntax) {
    ConcreteBaseDatabaseMeta dialect =
        new ConcreteBaseDatabaseMeta() {
          @Override
          public DmlSyntax getDmlSyntax() {
            return syntax;
          }
        };
    dialect.setPluginName("TestDB");
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(dialect);
    return databaseMeta;
  }

  private static DmlSyntax merge(int minimumMajorVersion) {
    return DmlSyntax.builder().upsert(MERGE.withMinimumMajorVersion(minimumMajorVersion)).build();
  }

  /** Table T with a primary key and unique indexes, each a list of columns. */
  private static FakeMetaData server(int major, int minor, List<List<String>> uniqueKeys) {
    return new FakeMetaData(major, minor, uniqueKeys);
  }

  private static DmlSpec upsert(String... keys) throws Exception {
    List<DmlColumn> keyColumns = new ArrayList<>();
    for (String key : keys) {
      keyColumns.add(DmlColumn.of(key));
    }
    return DmlSpec.upsert(
        "T", keyColumns, List.of(DmlColumn.of("NAME")), List.of(DmlColumn.of("NAME")), false);
  }

  private static DmlPlan planOn(
      DmlSyntax syntax, DatabaseMetaData metaData, DmlSpec spec, UpsertForm form) throws Exception {
    return DmlSupport.plan(connection(syntax), metaData, spec, null, "T", form);
  }

  // ------------------------------------------------------------------------------ MERGE checks

  @Test
  void noConnection() {
    assertNotNull(DmlSupport.unsupportedReason(null, UpsertForm.MERGE));
    assertNotNull(DmlSupport.unsupportedReason(null, null));
  }

  @Test
  void unsupportedDialect() throws Exception {
    DatabaseMeta databaseMeta = connection(null);
    String reason = DmlSupport.unsupportedReason(databaseMeta, UpsertForm.MERGE);
    assertNotNull(reason);
    assertTrue(reason.contains("TestDB"), reason);
    assertTrue(reason.contains("MERGE"), reason);
    assertNotNull(DmlSupport.unsupportedReason(databaseMeta, null));
    // The server version can't make a dialect without MERGE support it.
    assertNotNull(
        plan(null, server(99, 0, List.of()), upsert("ID"), UpsertForm.MERGE)
            .getUnsupportedReason());
  }

  @Test
  void declaredDialectWithoutConnection() throws Exception {
    assertNull(DmlSupport.unsupportedReason(connection(merge(15)), UpsertForm.MERGE));
    DmlPlan plan = plan(merge(15), null, upsert("ID"), UpsertForm.MERGE);
    assertTrue(plan.isSupported());
    assertEquals(UpsertForm.MERGE, plan.getUpsert().form());
  }

  @Test
  void versionTooOld() throws Exception {
    String reason =
        plan(merge(15), server(14, 3, List.of()), upsert("ID"), UpsertForm.MERGE)
            .getUnsupportedReason();
    assertNotNull(reason);
    assertTrue(reason.contains("TestDB"), reason);
    assertTrue(reason.contains("14"), reason);
    assertTrue(reason.contains("15"), reason);
  }

  @Test
  void versionOk() throws Exception {
    assertTrue(
        plan(merge(15), server(15, 0, List.of()), upsert("ID"), UpsertForm.MERGE).isSupported());
    assertTrue(
        plan(merge(15), server(18, 0, List.of()), upsert("ID"), UpsertForm.MERGE).isSupported());
  }

  @Test
  void noMinimumVersionNeverAsksTheServer() throws Exception {
    FakeMetaData metaData =
        server(0, 0, List.of()).failVersion(new AssertionError("must not be asked"));
    assertTrue(
        plan(DmlSyntax.STANDARD_MERGE, metaData, upsert("ID"), UpsertForm.MERGE).isSupported());
  }

  @Test
  void driverThatCannotTellItsVersionIsGivenTheBenefitOfTheDoubt() throws Exception {
    FakeMetaData metaData =
        server(0, 0, List.of()).failVersion(new SQLException("not implemented"));
    assertTrue(plan(merge(10), metaData, upsert("ID"), UpsertForm.MERGE).isSupported());
  }

  @Test
  void minorVersionsCount() throws Exception {
    DmlSyntax sqlite =
        DmlSyntax.builder()
            .upsert(ON_CONFLICT.withMinimumMajorVersion(3).withMinimumMinorVersion(24))
            .build();
    List<List<String>> pk = List.of(List.of("ID"));
    String reason = plan(sqlite, server(3, 23, pk), upsert("ID"), null).getUnsupportedReason();
    assertNotNull(reason);
    assertTrue(reason.contains("3.23") && reason.contains("3.24"), reason);
    assertTrue(plan(sqlite, server(3, 24, pk), upsert("ID"), null).isSupported());
    assertTrue(plan(sqlite, server(4, 0, pk), upsert("ID"), null).isSupported());
  }

  // ------------------------------------------------------------------------------ unique keys

  @Test
  void onConflictNeedsAUniqueKeyOnExactlyTheKeyColumns() throws Exception {
    DmlSyntax syntax = DmlSyntax.builder().upsert(ON_CONFLICT).build();
    // The primary key, in another case and order.
    assertTrue(
        plan(syntax, server(1, 0, List.of(List.of("k2", "id"))), upsert("ID", "K2"), null)
            .isSupported());
    // A unique index next to another one is fine.
    assertTrue(
        plan(syntax, server(1, 0, List.of(List.of("CODE"), List.of("ID"))), upsert("ID"), null)
            .isSupported());
    // A key that covers more or fewer columns is not.
    for (List<String> unique : List.of(List.of("ID", "K2"), List.of("K2"))) {
      String reason =
          plan(syntax, server(1, 0, List.of(unique)), upsert("ID"), null).getUnsupportedReason();
      assertNotNull(reason, unique.toString());
      assertTrue(reason.contains("ID"), reason);
    }
    assertNotNull(plan(syntax, server(1, 0, List.of()), upsert("ID"), null).getUnsupportedReason());
  }

  @Test
  void onDuplicateKeyNeedsTheOnlyUniqueKey() throws Exception {
    DmlSyntax syntax = DmlSyntax.builder().upsert(ON_DUPLICATE_KEY).build();
    assertTrue(
        plan(syntax, server(8, 0, List.of(List.of("ID"))), upsert("ID"), null).isSupported());
    String reason =
        plan(syntax, server(8, 0, List.of(List.of("ID"), List.of("CODE"))), upsert("ID"), null)
            .getUnsupportedReason();
    assertNotNull(reason);
    assertTrue(reason.contains("ON_DUPLICATE_KEY"), reason);
  }

  @Test
  void theFirstQualifyingFormIsChosen() throws Exception {
    DmlSyntax syntax = DmlSyntax.builder().upsert(ON_CONFLICT).upsert(MERGE).build();
    assertEquals(
        UpsertForm.ON_CONFLICT,
        plan(syntax, server(1, 0, List.of(List.of("ID"))), upsert("ID"), null).getUpsert().form());
    // No unique key: the MERGE, which needs none.
    assertEquals(
        UpsertForm.MERGE,
        plan(syntax, server(1, 0, List.of()), upsert("ID"), null).getUpsert().form());
    // Without a connection nothing can be checked: the first form.
    assertEquals(UpsertForm.ON_CONFLICT, plan(syntax, null, upsert("ID"), null).getUpsert().form());
  }

  @Test
  void anOverrideIsHonouredOrRefused() throws Exception {
    DmlSyntax syntax = DmlSyntax.builder().upsert(ON_CONFLICT).upsert(MERGE).build();
    FakeMetaData metaData = server(1, 0, List.of(List.of("ID")));
    assertEquals(
        UpsertForm.MERGE,
        plan(syntax, metaData, upsert("ID"), UpsertForm.MERGE).getUpsert().form());
    String reason =
        plan(syntax, metaData, upsert("ID"), UpsertForm.ON_DUPLICATE_KEY).getUnsupportedReason();
    assertNotNull(reason);
    assertTrue(reason.contains("ON_DUPLICATE_KEY"), reason);
    // An override that doesn't qualify is not replaced by another form.
    assertNotNull(
        plan(syntax, server(1, 0, List.of()), upsert("ID"), UpsertForm.ON_CONFLICT)
            .getUnsupportedReason());
  }

  @Test
  void aFormDeclaredTwiceIsPickedByServerVersion() throws Exception {
    DmlSyntax.Upsert alias =
        ON_DUPLICATE_KEY.withRowAlias(true).withMinimumMajorVersion(8).withMinimumMinorVersion(0);
    DmlSyntax syntax = DmlSyntax.builder().upsert(alias).upsert(ON_DUPLICATE_KEY).build();
    assertEquals(2, syntax.upserts(UpsertForm.ON_DUPLICATE_KEY).size());
    for (UpsertForm form : new UpsertForm[] {null, UpsertForm.ON_DUPLICATE_KEY}) {
      assertTrue(
          plan(syntax, server(9, 4, List.of(List.of("ID"))), upsert("ID"), form)
              .getUpsert()
              .rowAlias());
      DmlPlan old = plan(syntax, server(5, 7, List.of(List.of("ID"))), upsert("ID"), form);
      assertTrue(old.isSupported(), old.getUnsupportedReason());
      assertFalse(old.getUpsert().rowAlias(), "an old server gets the other entry of the form");
    }
    assertNull(
        DmlSupport.unsupportedReason(
            connection(syntax), server(5, 7, List.of()).proxy(), UpsertForm.ON_DUPLICATE_KEY));
  }

  @Test
  void onConflictCantUpdateAColumnItDoesNotInsert() throws Exception {
    DmlSyntax syntax = DmlSyntax.builder().upsert(ON_CONFLICT).upsert(MERGE).build();
    DmlSpec spec =
        DmlSpec.upsert(
            "T",
            List.of(DmlColumn.of("ID")),
            List.of(DmlColumn.of("NAME")),
            List.of(DmlColumn.of("NAME"), DmlColumn.of("UPDATED_AT")),
            false);
    DmlPlan plan = plan(syntax, server(1, 0, List.of(List.of("ID"))), spec, null);
    assertEquals(UpsertForm.MERGE, plan.getUpsert().form());
    String reason =
        plan(syntax, server(1, 0, List.of(List.of("ID"))), spec, UpsertForm.ON_CONFLICT)
            .getUnsupportedReason();
    assertTrue(reason.contains("UPDATED_AT"), reason);
  }

  @Test
  void uniqueKeyLooksUpOtherCasesAndSurvivesADriverWithoutIndexes() throws Exception {
    FakeMetaData metaData = server(1, 0, List.of(List.of("ID"))).tableName("t").noIndexInfo();
    assertEquals(
        DmlSupport.UniqueKey.ONLY_THE_KEY,
        DmlSupport.uniqueKey(metaData.proxy(), null, "T", List.of("id")));
    assertEquals(
        DmlSupport.UniqueKey.NONE,
        DmlSupport.uniqueKey(metaData.proxy(), null, "T", List.of("NAME")));
  }

  // ------------------------------------------------------------------ which table is looked at

  private static FakeCatalog.Table customers(FakeCatalog catalog, String database) {
    return catalog
        .table(database, null, "customers")
        .column("id", "INT", Types.INTEGER, 10, 0)
        .column("code", "VARCHAR", Types.VARCHAR, 10, null)
        .column("name", "VARCHAR", Types.VARCHAR, 50, null);
  }

  /**
   * Connector/J ignores the schema and, without a catalog, lists the keys of every database on the
   * server. A primary key of {@code prod.customers} must not make ON DUPLICATE KEY the choice for
   * {@code staging.customers}, which has none: every row would be inserted again on every run.
   */
  @Test
  void aKeyInAnotherDatabaseDoesNotCount() throws Exception {
    FakeCatalog mysql = new FakeCatalog().schemaArgumentIgnored().currentCatalog("staging");
    customers(mysql, "staging");
    customers(mysql, "prod").primaryKey("id");

    for (String schema : new String[] {null, "", "staging"}) {
      assertEquals(
          DmlSupport.UniqueKey.NONE,
          DmlSupport.uniqueKey(mysql.metaData(), schema, "customers", List.of("id")),
          "schema " + schema);
    }
    assertEquals(
        DmlSupport.UniqueKey.ONLY_THE_KEY,
        DmlSupport.uniqueKey(mysql.metaData(), "prod", "customers", List.of("id")));

    DmlSyntax onDuplicateKey = DmlSyntax.builder().upsert(ON_DUPLICATE_KEY).build();
    DmlPlan plan =
        DmlSupport.plan(
            connection(onDuplicateKey), mysql.metaData(), upsert("id"), null, "customers", null);
    assertFalse(plan.isSupported(), "ON DUPLICATE KEY on a table without a unique key");
  }

  /**
   * The other way round: a different primary key, or a unique index, in another database must not
   * refuse this one. Every database names its primary key index PRIMARY.
   */
  @Test
  void aKeyInAnotherDatabaseDoesNotRefuseThisOne() throws Exception {
    FakeCatalog mysql = new FakeCatalog().schemaArgumentIgnored().currentCatalog("staging");
    customers(mysql, "staging").primaryKey("id");
    customers(mysql, "prod").primaryKey("code").uniqueIndex("ux_name", "name");
    assertEquals(
        DmlSupport.UniqueKey.ONLY_THE_KEY,
        DmlSupport.uniqueKey(mysql.metaData(), null, "customers", List.of("id")));
    assertEquals(
        DmlSupport.UniqueKey.THE_KEY_AND_OTHERS,
        DmlSupport.uniqueKey(mysql.metaData(), "prod", "customers", List.of("code")));
  }

  /** Without a schema, the table of the current schema; with one, that schema's. */
  @Test
  void withoutASchemaTheCurrentSchemaCounts() throws Exception {
    FakeCatalog postgres = new FakeCatalog().currentSchema("public");
    postgres
        .table("db", "archive", "t")
        .column("id", "int4", Types.INTEGER, 10, 0)
        .primaryKey("id");
    postgres.table("db", "public", "t").column("id", "int4", Types.INTEGER, 10, 0);
    assertEquals(
        DmlSupport.UniqueKey.NONE,
        DmlSupport.uniqueKey(postgres.metaData(), null, "t", List.of("id")));
    assertEquals(
        DmlSupport.UniqueKey.ONLY_THE_KEY,
        DmlSupport.uniqueKey(postgres.metaData(), "archive", "t", List.of("id")));
    // The only table of that name, in a schema further down the search path.
    FakeCatalog searchPath = new FakeCatalog().currentSchema("app");
    searchPath
        .table("db", "public", "t")
        .column("id", "int4", Types.INTEGER, 10, 0)
        .primaryKey("id");
    assertEquals(
        DmlSupport.UniqueKey.ONLY_THE_KEY,
        DmlSupport.uniqueKey(searchPath.metaData(), null, "t", List.of("id")));
  }

  /** Neither in the current schema nor in only one other: the database wouldn't use any. */
  @Test
  void anAmbiguousTableIsRefusedWithItsReason() throws Exception {
    FakeCatalog postgres = new FakeCatalog().currentSchema("app");
    postgres.table("db", "a", "t").column("id", "int4", Types.INTEGER, 10, 0).primaryKey("id");
    postgres.table("db", "b", "t").column("id", "int4", Types.INTEGER, 10, 0).primaryKey("id");
    SQLException e =
        assertThrows(
            SQLException.class,
            () -> DmlSupport.uniqueKey(postgres.metaData(), null, "t", List.of("id")));
    assertTrue(e.getMessage().contains("Fill in the schema"), e.getMessage());

    DmlSyntax onConflict = DmlSyntax.builder().upsert(ON_CONFLICT).build();
    String reason =
        DmlSupport.plan(connection(onConflict), postgres.metaData(), upsert("id"), null, "t", null)
            .getUnsupportedReason();
    assertNotNull(reason);
    assertTrue(reason.contains("a.t") && reason.contains("b.t"), reason);
  }

  /** ON CONFLICT can't use a partial unique index without its predicate. */
  @Test
  void aPartialUniqueIndexIsNoKey() throws Exception {
    FakeCatalog postgres = new FakeCatalog();
    postgres
        .table("db", "public", "t")
        .column("id", "int4", Types.INTEGER, 10, 0)
        .column("code", "varchar", Types.VARCHAR, 10, null)
        .primaryKey("id")
        .partialUniqueIndex("t_code_active", "active", "code");
    assertEquals(
        DmlSupport.UniqueKey.NONE,
        DmlSupport.uniqueKey(postgres.metaData(), null, "t", List.of("code")));
    // The primary key still counts, and the partial index is not another unique key.
    assertEquals(
        DmlSupport.UniqueKey.ONLY_THE_KEY,
        DmlSupport.uniqueKey(postgres.metaData(), null, "t", List.of("id")));
  }

  /** SQL Server matches names ignoring case and reports them as they were created. */
  @Test
  void aTableStoredInAnotherCaseIsFound() throws Exception {
    FakeCatalog sqlServer = new FakeCatalog().namesIgnoringCase().currentSchema("dbo");
    sqlServer
        .table("hop", "dbo", "OrderLine")
        .column("Id", "int", Types.INTEGER, 10, 0)
        .primaryKey("Id");
    assertEquals(
        DmlSupport.UniqueKey.ONLY_THE_KEY,
        DmlSupport.uniqueKey(sqlServer.metaData(), null, "orderline", List.of("ID")));
  }

  // ------------------------------------------------------------------------------ reasons

  /**
   * When no form qualifies, every reason is given, and one the table can fix comes before the
   * server version, which only Insert/Update gets around.
   */
  @Test
  void everyReasonIsGivenTheTableFirst() throws Exception {
    DmlSyntax postgres =
        DmlSyntax.builder()
            .upsert(MERGE.withMinimumMajorVersion(15))
            .upsert(ON_CONFLICT.withMinimumMajorVersion(9).withMinimumMinorVersion(5))
            .build();
    String reason =
        plan(postgres, server(14, 3, List.of()), upsert("ID"), null).getUnsupportedReason();
    assertNotNull(reason);
    int uniqueKey = reason.indexOf("ON_CONFLICT needs a primary key");
    int version = reason.indexOf("MERGE from version 15");
    assertTrue(uniqueKey >= 0 && version > uniqueKey, reason);
  }

  // ------------------------------------------------------------------------------ MariaDB

  /**
   * MariaDB answers a MySQL connection with versions (10, 11) that the row alias of MySQL 8.0.19+
   * would qualify for, but has no row alias: the {@code VALUES()} form is used.
   */
  @Test
  void mariaDbGetsNoRowAlias() throws Exception {
    DmlSyntax.Upsert alias =
        ON_DUPLICATE_KEY.withRowAlias(true).withMinimumMajorVersion(8).withMinimumMinorVersion(1);
    DmlSyntax mysql = DmlSyntax.builder().upsert(alias).upsert(ON_DUPLICATE_KEY).build();
    FakeCatalog mariaDb =
        new FakeCatalog().version(11, 4).product("MySQL", "11.4.2-MariaDB-ubu2404");
    mariaDb.table("hop", null, "T").column("ID", "INT", Types.INTEGER, 10, 0).primaryKey("ID");

    DmlPlan plan = planOn(mysql, mariaDb.metaData(), upsert("ID"), null);
    assertTrue(plan.isSupported(), plan.getUnsupportedReason());
    assertFalse(plan.getUpsert().rowAlias());
    assertNull(
        DmlSupport.unsupportedReason(
            connection(mysql), mariaDb.metaData(), UpsertForm.ON_DUPLICATE_KEY));

    // With only the row alias declared, MariaDB is refused with its reason.
    DmlSyntax aliasOnly = DmlSyntax.builder().upsert(alias).build();
    String reason =
        DmlSupport.unsupportedReason(
            connection(aliasOnly), mariaDb.metaData(), UpsertForm.ON_DUPLICATE_KEY);
    assertNotNull(reason);
    assertTrue(reason.contains("MariaDB"), reason);

    // MySQL itself keeps the row alias.
    FakeCatalog mysql84 = new FakeCatalog().version(8, 4).product("MySQL", "8.4.3");
    mysql84.table("hop", null, "T").column("ID", "INT", Types.INTEGER, 10, 0).primaryKey("ID");
    assertTrue(planOn(mysql, mysql84.metaData(), upsert("ID"), null).getUpsert().rowAlias());
  }

  /**
   * Table T, with a primary key (the first list) and unique indexes (the others), on a server of a
   * version. The columns are the key columns and NAME.
   */
  private static final class FakeMetaData {
    private final int major;
    private final int minor;
    private final List<List<String>> uniqueKeys;
    private Throwable versionFailure;
    private String tableName = "T";
    private boolean indexInfo = true;

    private FakeMetaData(int major, int minor, List<List<String>> uniqueKeys) {
      this.major = major;
      this.minor = minor;
      this.uniqueKeys = uniqueKeys;
    }

    FakeMetaData failVersion(Throwable failure) {
      this.versionFailure = failure;
      return this;
    }

    /** The name the table is stored under, as a database that stores lower case. */
    FakeMetaData tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    /** Only the first unique key is reported, as the primary key. */
    FakeMetaData noIndexInfo() {
      this.indexInfo = false;
      return this;
    }

    private DatabaseMetaData proxy() {
      FakeCatalog catalog = new FakeCatalog().version(major, minor);
      if (versionFailure != null) {
        catalog.failVersion(versionFailure);
      }
      if (!indexInfo) {
        catalog.noIndexInfo();
      }
      FakeCatalog.Table table = catalog.table(null, null, tableName);
      Set<String> columns = new LinkedHashSet<>();
      uniqueKeys.forEach(columns::addAll);
      columns.add("NAME");
      for (String column : columns) {
        table.column(column, "VARCHAR", Types.VARCHAR, 50, null);
      }
      for (int i = 0; i < uniqueKeys.size(); i++) {
        String[] key = uniqueKeys.get(i).toArray(new String[0]);
        if (i == 0) {
          table.primaryKey(key);
        } else {
          table.uniqueIndex("UX" + i, key);
        }
      }
      return catalog.metaData();
    }
  }

  private static DmlPlan plan(
      DmlSyntax syntax, FakeMetaData metaData, DmlSpec spec, UpsertForm form) throws Exception {
    return planOn(syntax, metaData == null ? null : metaData.proxy(), spec, form);
  }
}
