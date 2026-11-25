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

package org.apache.hop.databases.postgresql;

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
 * The casts the PostgreSql dialect writes into its MERGE, read from the column types a driver
 * reports.
 */
class PostgreSqlDmlCastSqlTest {

  private static final PostgreSqlDatabaseMeta DIALECT = new PostgreSqlDatabaseMeta();

  private static final DatabaseMetaData METADATA =
      ColumnMetaDataStub.of(
          "PostgreSQL",
          "target",
          List.of(
              new Column("id", "int4", Types.INTEGER, 10, 0),
              new Column("name", "varchar", Types.VARCHAR, 5, null),
              new Column("code", "bpchar", Types.CHAR, 3, null),
              new Column("amount", "numeric", Types.NUMERIC, 10, 2),
              new Column("flags", "bit", Types.BIT, 3, null),
              new Column("ts", "timestamp", Types.TIMESTAMP, 29, 6),
              new Column("doc", "jsonb", Types.OTHER, Integer.MAX_VALUE, null)));

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
   * The casts name the type without its length or precision: PostgreSQL would cut a longer string
   * cast to {@code varchar(5)} and round a key cast to {@code numeric(10,2)}; {@code bit(3)} is
   * cast to {@code varbit}, as {@code bit} means {@code bit(1)}.
   */
  @Test
  void castTypesHaveNoLength() throws Exception {
    List<String> types =
        List.of("int4", "varchar", "bpchar", "numeric", "varbit", "timestamp", "jsonb");
    List<DmlColumn> all = columns("id", "name", "code", "amount", "flags", "ts", "doc");

    DmlSyntax.Upsert merge = DIALECT.getDmlSyntax().upsert(UpsertForm.MERGE);
    String upsert =
        sql(
            DmlSpec.upsert(
                "target", all.subList(0, 1), all.subList(1, 7), all.subList(1, 7), false),
            merge);
    assertEquals(types, casts(upsert));
  }

  /** The VALUES list of ON CONFLICT, which the target types, is not cast. */
  @Test
  void onConflictIsNotCast() throws Exception {
    List<DmlColumn> all = columns("id", "name", "code", "amount");
    DmlSyntax.Upsert onConflict = DIALECT.getDmlSyntax().upsert(UpsertForm.ON_CONFLICT);
    assertEquals(
        List.of(),
        casts(
            sql(
                DmlSpec.upsert(
                    "target", all.subList(0, 1), all.subList(1, 4), all.subList(1, 4), false),
                onConflict)));
  }

  /**
   * PostgreSQL has no {@code <>} on json and xml, the geometric types have none or compare areas,
   * and citext ignores case: none of them can tell whether a row changed.
   */
  @Test
  void typesWithoutComparison() {
    for (String type :
        new String[] {"json", "_json", "point", "box", "polygon", "circle", "citext", "_citext"}) {
      assertFalse(DIALECT.isDmlComparable(type, Types.OTHER), type);
    }
    assertFalse(DIALECT.isDmlComparable("xml", Types.SQLXML));
    for (String type : new String[] {"jsonb", "text", "varchar", "int4", "_int4", "uuid"}) {
      assertTrue(DIALECT.isDmlComparable(type, Types.OTHER), type);
    }
  }
}
