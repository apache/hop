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

package org.apache.hop.calcite;

import java.util.Locale;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.calcite.sql.dialect.ClickHouseSqlDialect;
import org.apache.calcite.sql.dialect.Db2SqlDialect;
import org.apache.calcite.sql.dialect.DerbySqlDialect;
import org.apache.calcite.sql.dialect.ExasolSqlDialect;
import org.apache.calcite.sql.dialect.FirebirdSqlDialect;
import org.apache.calcite.sql.dialect.H2SqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.apache.calcite.sql.dialect.InformixSqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.dialect.RedshiftSqlDialect;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;
import org.apache.calcite.sql.dialect.SqliteSqlDialect;
import org.apache.calcite.sql.dialect.SybaseSqlDialect;
import org.apache.calcite.sql.dialect.TeradataSqlDialect;
import org.apache.calcite.sql.dialect.VerticaSqlDialect;

/**
 * Maps a Hop relational database plugin id to a Calcite {@link CalciteSqlStyle} so identifiers are
 * quoted the way that dialect expects.
 */
public final class CalciteSqlDialects {

  /** ANSI SQL quotes identifiers with double quotes. Calcite {@link Lex#JAVA} uses backticks. */
  public static final CalciteSqlStyle ANSI =
      new CalciteSqlStyle(AnsiSqlDialect.DEFAULT, Lex.MYSQL_ANSI);

  private CalciteSqlDialects() {}

  /**
   * @param databasePluginId Hop {@code DatabaseMeta} plugin id, or {@code null} for ANSI
   * @return matching style, never {@code null}
   */
  public static CalciteSqlStyle of(String databasePluginId) {
    if (databasePluginId == null || databasePluginId.isBlank()) {
      return ANSI;
    }
    return switch (databasePluginId.toUpperCase(Locale.ROOT)) {
      case "MYSQL", "MARIADB", "INFOBRIGHT", "INFINIDB", "SINGLESTORE", "DORIS" ->
          new CalciteSqlStyle(MysqlSqlDialect.DEFAULT, Lex.MYSQL);
      case "ORACLE", "ORACLERDB" -> new CalciteSqlStyle(OracleSqlDialect.DEFAULT, Lex.ORACLE);
      case "MSSQL", "MSSQLNATIVE" -> new CalciteSqlStyle(MssqlSqlDialect.DEFAULT, Lex.SQL_SERVER);
      case "POSTGRESQL", "GREENPLUM", "COCKROACHDB" ->
          new CalciteSqlStyle(PostgresqlSqlDialect.DEFAULT, Lex.MYSQL_ANSI);
      case "REDSHIFT" -> new CalciteSqlStyle(RedshiftSqlDialect.DEFAULT, Lex.MYSQL_ANSI);
      case "GOOGLEBIGQUERY" -> new CalciteSqlStyle(BigQuerySqlDialect.DEFAULT, Lex.BIG_QUERY);
      case "SNOWFLAKE" -> new CalciteSqlStyle(SnowflakeSqlDialect.DEFAULT, Lex.JAVA);
      case "HIVE", "CLOUDERA-IMPALA" -> new CalciteSqlStyle(HiveSqlDialect.DEFAULT, Lex.JAVA);
      case "H2" -> new CalciteSqlStyle(H2SqlDialect.DEFAULT, Lex.JAVA);
      case "DERBY" -> new CalciteSqlStyle(DerbySqlDialect.DEFAULT, Lex.JAVA);
      case "DB2" -> new CalciteSqlStyle(Db2SqlDialect.DEFAULT, Lex.JAVA);
      case "SYBASE", "SYBASEIQ" -> new CalciteSqlStyle(SybaseSqlDialect.DEFAULT, Lex.JAVA);
      case "VERTICA", "VERTICA5" -> new CalciteSqlStyle(VerticaSqlDialect.DEFAULT, Lex.JAVA);
      case "EXASOL4" -> new CalciteSqlStyle(ExasolSqlDialect.DEFAULT, Lex.JAVA);
      case "CLICKHOUSE" -> new CalciteSqlStyle(ClickHouseSqlDialect.DEFAULT, Lex.MYSQL);
      case "FIREBIRD", "INTERBASE" -> new CalciteSqlStyle(FirebirdSqlDialect.DEFAULT, Lex.JAVA);
      case "INFORMIX" -> new CalciteSqlStyle(InformixSqlDialect.DEFAULT, Lex.JAVA);
      case "HYPERSONIC" -> new CalciteSqlStyle(HsqldbSqlDialect.DEFAULT, Lex.JAVA);
      case "SQLITE" -> new CalciteSqlStyle(SqliteSqlDialect.DEFAULT, Lex.JAVA);
      case "TERADATA" -> new CalciteSqlStyle(TeradataSqlDialect.DEFAULT, Lex.JAVA);
      default -> ANSI;
    };
  }
}
