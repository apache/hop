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

package org.apache.hop.databases.mssql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlColumns;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlStatementBuilder;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.junit.dml.ColumnMetaDataStub;
import org.apache.hop.junit.dml.ColumnMetaDataStub.Column;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The casts the MsSqlServer dialect writes into its MERGE, read from the column types a driver
 * reports.
 */
class MsSqlServerDmlCastSqlTest {

  private static final MsSqlServerDatabaseMeta DIALECT = new MsSqlServerDatabaseMeta();

  private static final DatabaseMetaData METADATA =
      ColumnMetaDataStub.of(
          "Microsoft SQL Server",
          "target",
          List.of(
              new Column("id", "int identity", Types.INTEGER, 10, 0),
              new Column("name", "varchar", Types.VARCHAR, 5, null),
              new Column("uname", "nvarchar", Types.NVARCHAR, 5, null),
              new Column("code", "char", Types.CHAR, 3, null),
              new Column("ucode", "nchar", Types.NCHAR, 3, null),
              new Column("bytes", "varbinary", Types.VARBINARY, 4, null),
              new Column("fixed", "binary", Types.BINARY, 4, null),
              new Column("amount", "decimal", Types.DECIMAL, 10, 2),
              new Column("label", "sysname", Types.NVARCHAR, 128, null),
              new Column("text", "varchar", Types.VARCHAR, Integer.MAX_VALUE, null),
              new Column("ts", "datetime2", Types.TIMESTAMP, 27, 7),
              new Column("price", "money", Types.DECIMAL, 19, 4)));

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
  }

  private static DatabaseMeta databaseMeta() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(DIALECT);
    return databaseMeta;
  }

  private static List<DmlColumn> columns(String... names) throws Exception {
    return DmlColumns.fromTable(METADATA, DIALECT, null, "target", List.of(names));
  }

  /** The cast types of a statement's parameters, in order. */
  private static List<String> casts(String sql) {
    List<String> casts = new ArrayList<>();
    Matcher matcher =
        Pattern.compile("CAST\\(\\? AS ([^()]+(?:\\([^()]*\\))?[^()]*)\\)").matcher(sql);
    while (matcher.find()) {
      casts.add(matcher.group(1));
    }
    return casts;
  }

  private static String sql(DmlSpec spec, DmlSyntax.Upsert upsert) throws Exception {
    return DmlStatementBuilder.upsert(databaseMeta(), DIALECT.getDmlSyntax(), upsert, spec)
        .getSql();
  }

  /**
   * The casts change no value: character and binary types are cast to their {@code (max)} type,
   * since SQL Server would cut a longer value cast to {@code varchar(5)}, and a decimal to the
   * widest scale for its integer digits, since a cast to {@code decimal(10,2)} would round a key.
   */
  @Test
  void castTypesAreWideEnough() throws Exception {
    List<String> types =
        List.of(
            "int",
            "varchar(max)",
            "nvarchar(max)",
            "varchar(max)",
            "nvarchar(max)",
            "varbinary(max)",
            "varbinary(max)",
            "decimal(38,30)",
            "nvarchar(max)",
            "varchar(max)",
            "datetime2",
            "money");
    List<DmlColumn> all =
        columns(
            "id", "name", "uname", "code", "ucode", "bytes", "fixed", "amount", "label", "text",
            "ts", "price");
    DmlSyntax.Upsert merge = DIALECT.getDmlSyntax().upsert(UpsertForm.MERGE);
    assertEquals(
        types,
        casts(
            sql(
                DmlSpec.upsert(
                    "target", all.subList(0, 1), all.subList(1, 12), all.subList(1, 12), false),
                merge)));
  }

  /** SQL Server has no {@code <>} on text, ntext, image, xml and the spatial types. */
  @Test
  void typesWithoutComparison() {
    for (String type : new String[] {"text", "ntext", "image", "xml", "geometry", "geography"}) {
      assertFalse(DIALECT.isDmlComparable(type, Types.LONGVARCHAR), type);
    }
    for (String type : new String[] {"varchar", "nvarchar", "varbinary", "int", "datetime2"}) {
      assertTrue(DIALECT.isDmlComparable(type, Types.VARCHAR), type);
    }
    // A dialect that extends it, as the native one does, inherits it.
    assertFalse(new MsSqlServerDatabaseMeta() {}.isDmlComparable("xml", Types.LONGNVARCHAR));
  }

  /**
   * The VALUES list of an INSERT of several rows is cast with the MERGE source's wide types, on
   * every row, except that character types become {@code nvarchar(max)}: a cast to {@code varchar}
   * would convert the value to the database's default code page rather than the column's. One row
   * is not cast.
   */
  @Test
  void anInsertOfSeveralRowsIsCastWideEnough() throws Exception {
    List<DmlColumn> all =
        columns(
            "id", "name", "uname", "code", "ucode", "bytes", "fixed", "amount", "label", "text",
            "ts", "price");
    List<String> types =
        List.of(
            "int",
            "nvarchar(max)",
            "nvarchar(max)",
            "nvarchar(max)",
            "nvarchar(max)",
            "varbinary(max)",
            "varbinary(max)",
            "decimal(38,30)",
            "nvarchar(max)",
            "nvarchar(max)",
            "datetime2",
            "money");
    List<String> twice = new ArrayList<>(types);
    twice.addAll(types);
    assertEquals(twice, casts(insertSql(DmlSpec.insert("target", all), 2)));
    assertEquals(List.of(), casts(insertSql(DmlSpec.insert("target", all), 1)));
  }

  @Test
  void characterTypesOfAValuesListAreUnicode() {
    for (String type : new String[] {"varchar(max)", "VARCHAR(MAX)", "char(10)", "text"}) {
      assertEquals("nvarchar(max)", DIALECT.getDmlValuesListCastType(type), type);
    }
    for (String type :
        new String[] {"nvarchar(max)", "ntext", "varbinary(max)", "decimal(38,30)", "int"}) {
      assertEquals(type, DIALECT.getDmlValuesListCastType(type), type);
    }
    // The MERGE source keeps varchar(max): an nvarchar key would cost the index seek.
    assertEquals("varchar(max)", DIALECT.getDmlCastType("varchar", Types.VARCHAR, 5, 0));
  }

  private static String insertSql(DmlSpec spec, int rows) throws Exception {
    return DmlStatementBuilder.insert(databaseMeta(), DIALECT.getDmlSyntax(), spec, rows).getSql();
  }
}
