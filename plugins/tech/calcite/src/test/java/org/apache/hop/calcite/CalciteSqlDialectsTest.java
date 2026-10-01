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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.junit.jupiter.api.Test;

class CalciteSqlDialectsTest {

  @Test
  void nullAndUnknownFallBackToAnsi() {
    assertSame(CalciteSqlDialects.ANSI, CalciteSqlDialects.of(null));
    assertSame(CalciteSqlDialects.ANSI, CalciteSqlDialects.of(""));
    assertSame(CalciteSqlDialects.ANSI, CalciteSqlDialects.of("NOT-A-DATABASE"));
  }

  @Test
  void mapsCommonHopPluginIds() {
    assertEquals(Lex.MYSQL, CalciteSqlDialects.of("MYSQL").lex());
    assertEquals(MysqlSqlDialect.DEFAULT, CalciteSqlDialects.of("mariadb").dialect());
    assertEquals(Lex.ORACLE, CalciteSqlDialects.of("ORACLE").lex());
    assertEquals(OracleSqlDialect.DEFAULT, CalciteSqlDialects.of("ORACLERDB").dialect());
    assertEquals(Lex.SQL_SERVER, CalciteSqlDialects.of("MSSQLNATIVE").lex());
    assertEquals(PostgresqlSqlDialect.DEFAULT, CalciteSqlDialects.of("POSTGRESQL").dialect());
    assertEquals(Lex.MYSQL_ANSI, CalciteSqlDialects.of("POSTGRESQL").lex());
    assertEquals(Lex.MYSQL_ANSI, CalciteSqlDialects.of("REDSHIFT").lex());
    assertEquals(Lex.MYSQL_ANSI, CalciteSqlDialects.ANSI.lex());
    assertEquals(Lex.BIG_QUERY, CalciteSqlDialects.of("GOOGLEBIGQUERY").lex());
    assertEquals(Lex.JAVA, CalciteSqlDialects.of("H2").lex());
  }
}
