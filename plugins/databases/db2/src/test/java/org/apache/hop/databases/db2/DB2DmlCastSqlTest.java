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

package org.apache.hop.databases.db2;

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
 * The casts the DB2 dialect writes into its statements of several rows and its MERGE, read from the
 * column types a driver reports.
 */
class DB2DmlCastSqlTest {

  private static final DB2DatabaseMeta DIALECT = new DB2DatabaseMeta();

  private static final DatabaseMetaData METADATA =
      ColumnMetaDataStub.of(
          "DB2/LINUXX8664",
          "target",
          List.of(
              new Column("ID", "INTEGER", Types.INTEGER, 10, 0),
              new Column("NAME", "VARCHAR", Types.VARCHAR, 5, null),
              new Column("CODE", "CHAR", Types.CHAR, 3, null),
              new Column("AMOUNT", "DECIMAL", Types.DECIMAL, 10, 2),
              new Column("BYTES", "VARBINARY", Types.VARBINARY, 4, null),
              new Column("TS", "TIMESTAMP", Types.TIMESTAMP, 26, 6)));

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
   * DB2 keeps the lengths in its casts: it types the parameter marker of {@code CAST(? AS
   * VARCHAR(5))} as VARCHAR(5) and refuses a longer value (SQLCODE -302), as the plain statement
   * does; DB2MergeIT checks it on a server. A cast without a length would mean CHAR(1) or
   * DECIMAL(5,0).
   */
  @Test
  void castTypesKeepTheLength() throws Exception {
    List<DmlColumn> all = columns("ID", "NAME", "CODE", "AMOUNT", "BYTES", "TS");
    DmlSyntax.Upsert merge = DIALECT.getDmlSyntax().upsert(UpsertForm.MERGE);
    assertEquals(
        List.of("INTEGER", "VARCHAR(5)", "CHAR(3)", "DECIMAL(10,2)", "VARBINARY(4)", "TIMESTAMP"),
        casts(
            sql(
                DmlSpec.upsert(
                    "target", all.subList(0, 1), all.subList(1, 6), all.subList(1, 6), false),
                merge)));
  }

  /**
   * A large object keeps its length: DB2 reads {@code CAST(? AS CLOB)} as CLOB(1M) and would refuse
   * a 2 MB value that a CLOB(10M) column takes.
   */
  @Test
  void largeObjectsKeepTheirLength() {
    assertEquals("CLOB(10485760)", DIALECT.getDmlCastType("CLOB", Types.CLOB, 10485760, 0));
    assertEquals("BLOB(1048576000)", DIALECT.getDmlCastType("BLOB", Types.BLOB, 1048576000, 0));
    assertEquals("DBCLOB(5242880)", DIALECT.getDmlCastType("DBCLOB", Types.CLOB, 5242880, 0));
  }

  /** DB2 has no {@code <>} on large objects, XML and the long string types. */
  @Test
  void typesWithoutComparison() {
    for (String type : new String[] {"CLOB", "BLOB", "DBCLOB", "XML", "LONG VARCHAR"}) {
      int dataType = type.endsWith("LOB") ? Types.CLOB : Types.OTHER;
      assertFalse(DIALECT.isDmlComparable(type, dataType), type);
    }
    assertTrue(DIALECT.isDmlComparable("VARCHAR", Types.VARCHAR));
    assertTrue(DIALECT.isDmlComparable("DECIMAL", Types.DECIMAL));
  }
}
