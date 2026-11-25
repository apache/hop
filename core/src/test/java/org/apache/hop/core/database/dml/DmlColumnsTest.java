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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.NoneDatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.junit.dml.ColumnMetaDataStub;
import org.apache.hop.junit.dml.ColumnMetaDataStub.Column;
import org.apache.hop.junit.dml.FakeCatalog;
import org.junit.jupiter.api.Test;

class DmlColumnsTest {

  @Test
  void characterTypesKeepTheirLength() {
    assertEquals("VARCHAR(50)", DmlColumns.nativeType("VARCHAR", Types.VARCHAR, 50, -1));
    assertEquals("bpchar(3)", DmlColumns.nativeType("bpchar", Types.CHAR, 3, 0));
    assertEquals(
        "CHARACTER VARYING(20)", DmlColumns.nativeType("CHARACTER VARYING", Types.VARCHAR, 20, 0));
    assertEquals("NVARCHAR(10)", DmlColumns.nativeType("NVARCHAR", Types.NVARCHAR, 10, 0));
  }

  @Test
  void unboundedCharacterTypesHaveNoLength() {
    assertEquals("text", DmlColumns.nativeType("text", Types.VARCHAR, Integer.MAX_VALUE, 0));
    assertEquals("varchar", DmlColumns.nativeType("varchar", Types.VARCHAR, Integer.MAX_VALUE, 0));
  }

  /**
   * A large object keeps its length, however large: DB2 reads {@code CAST(? AS CLOB)} as CLOB(1M)
   * and would refuse a 2 MB value that a CLOB(10M) column takes.
   */
  @Test
  void largeObjectsKeepTheirLength() {
    assertEquals("CLOB(10485760)", DmlColumns.nativeType("CLOB", Types.CLOB, 10485760, 0));
    assertEquals("BLOB(1073741824)", DmlColumns.nativeType("BLOB", Types.BLOB, 1073741824, 0));
    assertEquals(
        "CLOB(2147483647)", DmlColumns.nativeType("CLOB", Types.CLOB, Integer.MAX_VALUE, 0));
    assertEquals("NCLOB(1000)", DmlColumns.nativeType("NCLOB", Types.NCLOB, 1000, 0));
    // DB2's DBCLOB, whatever type number the driver gives it.
    assertEquals("DBCLOB(5242880)", DmlColumns.nativeType("DBCLOB", Types.OTHER, 5242880, 0));
    assertEquals("CLOB", DmlColumns.nativeType("CLOB", Types.CLOB, 0, 0));
  }

  @Test
  void exactNumbersKeepPrecisionAndScale() {
    assertEquals("NUMERIC(10,2)", DmlColumns.nativeType("NUMERIC", Types.NUMERIC, 10, 2));
    assertEquals("DECIMAL(5,0)", DmlColumns.nativeType("DECIMAL", Types.DECIMAL, 5, -1));
    // PostgreSQL's unconstrained numeric
    assertEquals("numeric", DmlColumns.nativeType("numeric", Types.NUMERIC, 131089, 0));
    assertEquals("NUMBER", DmlColumns.nativeType("NUMBER", Types.NUMERIC, 0, -127));
  }

  @Test
  void otherTypesHaveNoModifier() {
    assertEquals("INTEGER", DmlColumns.nativeType("INTEGER", Types.INTEGER, 10, 0));
    assertEquals("timestamp", DmlColumns.nativeType("timestamp", Types.TIMESTAMP, 29, 6));
    assertNull(DmlColumns.nativeType(null, Types.INTEGER, 0, 0));
  }

  @Test
  void sqlServerTypesAreWrittenTheWaySqlServerCastsThem() {
    // As mssql-jdbc 13 reports them for SQL Server 2022.
    assertEquals("int", DmlColumns.nativeType("int identity", Types.INTEGER, 10, 0, true));
    assertEquals("numeric(10,2)", DmlColumns.nativeType("numeric", Types.NUMERIC, 10, 2, true));
    assertEquals("money", DmlColumns.nativeType("money", Types.DECIMAL, 19, 4, true));
    assertEquals(
        "uniqueidentifier", DmlColumns.nativeType("uniqueidentifier", Types.CHAR, 36, -1, true));
    assertEquals(
        "varchar(max)",
        DmlColumns.nativeType("varchar", Types.VARCHAR, Integer.MAX_VALUE, -1, true));
    assertEquals(
        "nvarchar(max)",
        DmlColumns.nativeType("nvarchar", Types.NVARCHAR, Integer.MAX_VALUE, -1, true));
    assertEquals(
        "varbinary(max)",
        DmlColumns.nativeType("varbinary", Types.VARBINARY, Integer.MAX_VALUE, -1, true));
    assertEquals("nvarchar(20)", DmlColumns.nativeType("nvarchar", Types.NVARCHAR, 20, -1, true));
    assertEquals(
        "xml", DmlColumns.nativeType("xml", Types.LONGNVARCHAR, Integer.MAX_VALUE, -1, true));
    assertEquals("datetime2", DmlColumns.nativeType("datetime2", Types.TIMESTAMP, 27, 7, true));
    // Elsewhere an unbounded varchar stays without a length.
    assertEquals(
        "varchar", DmlColumns.nativeType("varchar", Types.VARCHAR, Integer.MAX_VALUE, -1, false));
  }

  @Test
  void serialPseudoTypesBecomeTheirIntegerType() {
    assertEquals("int4", DmlColumns.nativeType("serial", Types.INTEGER, 10, 0));
    assertEquals("int8", DmlColumns.nativeType("bigserial", Types.BIGINT, 19, 0));
  }

  @Test
  void derbyBitDataGetsItsLengthInside() {
    assertEquals(
        "VARCHAR(16) FOR BIT DATA",
        DmlColumns.nativeType("VARCHAR () FOR BIT DATA", Types.VARBINARY, 16, 0));
    assertEquals(
        "LONG VARCHAR FOR BIT DATA",
        DmlColumns.nativeType("LONG VARCHAR FOR BIT DATA", Types.LONGVARBINARY, 32700, 0));
  }

  /**
   * The dialect writes each column type as a cast target; without one the type keeps its length.
   */
  @Test
  void theDialectWritesTheCastTypes() throws Exception {
    DatabaseMetaData metaData =
        ColumnMetaDataStub.of(
            "Some database",
            "target",
            List.of(
                new Column("ID", "INTEGER", Types.INTEGER, 10, 0),
                new Column("NAME", "VARCHAR", Types.VARCHAR, 5, null)));
    IDatabase dialect =
        new NoneDatabaseMeta() {
          @Override
          public String getDmlCastType(
              String typeName, int dataType, int columnSize, int decimalDigits) {
            return typeName + "/" + dataType + "/" + columnSize + "/" + decimalDigits;
          }
        };
    assertEquals(
        List.of(new DmlColumn("ID", "INTEGER/4/10/0"), new DmlColumn("NAME", "VARCHAR/12/5/-1")),
        DmlColumns.fromTable(metaData, dialect, null, "target", List.of("ID", "NAME")));
    assertEquals(
        List.of(new DmlColumn("ID", "INTEGER"), new DmlColumn("NAME", "VARCHAR(5)")),
        DmlColumns.fromTable(metaData, null, null, "target", List.of("ID", "NAME")));
    assertEquals(
        List.of(new DmlColumn("NAME", "VARCHAR(5)")),
        DmlColumns.fromTable(metaData, new NoneDatabaseMeta(), null, "target", List.of("name")));
  }

  // ------------------------------------------------------------------ which table is read

  private static List<String> types(List<DmlColumn> columns) {
    return columns.stream().map(DmlColumn::nativeType).toList();
  }

  private static FakeCatalog.Table orders(FakeCatalog catalog, String schema, String createdType) {
    return catalog
        .table("db", schema, "orders")
        .column("id", "int4", Types.INTEGER, 10, 0)
        .column("created", createdType, Types.TIMESTAMP, 29, 6);
  }

  /**
   * Without a schema the casts follow the table the statement writes to: the one in the current
   * schema. PostgreSQL lists {@code archive.orders} before {@code public.orders}, and casting to
   * its {@code date} would drop the time of every row.
   */
  @Test
  void withoutASchemaTheCurrentSchemasTableIsRead() throws Exception {
    FakeCatalog postgres = new FakeCatalog().currentSchema("public");
    orders(postgres, "archive", "date");
    orders(postgres, "public", "timestamp");
    assertEquals(
        List.of("timestamp"),
        types(DmlColumns.fromTable(postgres.metaData(), null, null, "orders", List.of("created"))));
    assertEquals(
        List.of("date"),
        types(
            DmlColumns.fromTable(
                postgres.metaData(), null, "archive", "orders", List.of("created"))));
  }

  /** The only table of that name, in a schema further down the search path, is the one. */
  @Test
  void theOnlyTableOfThatNameIsRead() throws Exception {
    FakeCatalog postgres = new FakeCatalog().currentSchema("app");
    orders(postgres, "public", "timestamp");
    assertEquals(
        List.of("timestamp"),
        types(DmlColumns.fromTable(postgres.metaData(), null, "", "orders", List.of("created"))));
  }

  /** In several schemas and not in the current one: refused, not guessed. */
  @Test
  void anAmbiguousTableIsRefused() {
    FakeCatalog postgres = new FakeCatalog().currentSchema("app");
    orders(postgres, "archive", "date");
    orders(postgres, "public", "timestamp");
    HopDatabaseException e =
        assertThrows(
            HopDatabaseException.class,
            () -> DmlColumns.fromTable(postgres.metaData(), null, null, "orders", List.of("id")));
    assertTrue(e.getMessage().contains("archive.orders"), e.getMessage());
    assertTrue(e.getMessage().contains("Fill in the schema"), e.getMessage());
  }

  /** "_" in a schema pattern is a wildcard: only the schema asked for counts. */
  @Test
  void aSchemaIsMatchedExactlyNotAsAPattern() throws Exception {
    FakeCatalog postgres = new FakeCatalog();
    orders(postgres, "myXapp", "date");
    orders(postgres, "my_app", "timestamp");
    assertEquals(
        List.of("timestamp"),
        types(
            DmlColumns.fromTable(
                postgres.metaData(), null, "my_app", "orders", List.of("created"))));
  }

  /** Connector/J ignores the schema: the database is the catalog, current or named. */
  @Test
  void onMySqlTheDatabaseIsTheCatalog() throws Exception {
    FakeCatalog mysql = new FakeCatalog().schemaArgumentIgnored().currentCatalog("staging");
    mysql.table("prod", null, "orders").column("amount", "DECIMAL", Types.DECIMAL, 10, 0);
    mysql.table("staging", null, "orders").column("amount", "DECIMAL", Types.DECIMAL, 10, 2);
    assertEquals(
        List.of("DECIMAL(10,2)"),
        types(DmlColumns.fromTable(mysql.metaData(), null, null, "orders", List.of("amount"))));
    assertEquals(
        List.of("DECIMAL(10,0)"),
        types(DmlColumns.fromTable(mysql.metaData(), null, "prod", "orders", List.of("amount"))));
  }

  /**
   * A case-insensitive SQL Server returns {@code OrderLine} for {@code orderline}, as the table was
   * created: it is the table, as SQL Server itself would take it.
   */
  @Test
  void aTableStoredInAnotherCaseIsFound() throws Exception {
    FakeCatalog sqlServer = new FakeCatalog().namesIgnoringCase().currentSchema("dbo");
    sqlServer
        .table("hop", "dbo", "OrderLine")
        .column("Id", "int", Types.INTEGER, 10, 0)
        .column("Label", "nvarchar", Types.NVARCHAR, 20, null);
    assertEquals(
        List.of(new DmlColumn("Id", "int"), new DmlColumn("Label", "nvarchar(20)")),
        DmlColumns.fromTable(
            sqlServer.metaData(), null, null, "orderline", List.of("ID", "label")));
  }

  /** Whether "update only changed rows" can compare a column, by default and per dialect. */
  @Test
  void comparableTypes() throws Exception {
    for (int type : new int[] {Types.CLOB, Types.NCLOB, Types.BLOB, Types.SQLXML, Types.STRUCT}) {
      assertFalse(DmlColumns.comparable("X", type), "type " + type);
    }
    for (int type : new int[] {Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP, Types.ARRAY}) {
      assertTrue(DmlColumns.comparable("X", type), "type " + type);
    }
    assertTrue(DmlColumns.isOneOf("SYS.XMLTYPE", Set.of("xmltype")));
    assertTrue(DmlColumns.isOneOf(" Text ", Set.of("text")));
    assertFalse(DmlColumns.isOneOf(null, Set.of("text")));

    FakeCatalog catalog = new FakeCatalog();
    catalog
        .table(null, "s", "t")
        .column("id", "INTEGER", Types.INTEGER, 10, 0)
        .column("doc", "CLOB", Types.CLOB, 1000, null)
        .column("payload", "json", Types.OTHER, 0, null);
    List<DmlColumn> columns =
        DmlColumns.fromTable(catalog.metaData(), null, null, "t", List.of("id", "doc", "payload"));
    assertEquals(List.of(true, false, true), columns.stream().map(DmlColumn::comparable).toList());
    IDatabase noJson =
        new NoneDatabaseMeta() {
          @Override
          public boolean isDmlComparable(String typeName, int dataType) {
            return !"json".equals(typeName) && super.isDmlComparable(typeName, dataType);
          }
        };
    columns =
        DmlColumns.fromTable(
            catalog.metaData(), noJson, null, "t", List.of("id", "doc", "payload"));
    assertEquals(List.of(true, false, false), columns.stream().map(DmlColumn::comparable).toList());
  }
}
