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

package org.apache.hop.databases.mssqlnative;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Types;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.types.ColumnContext;
import org.apache.hop.core.database.types.DatabaseTypeMapper;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.mssql.MsSqlServerDatabaseMeta;
import org.apache.hop.junit.database.TypeRuleFixture;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class MsSqlServerNativeDatabaseMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void nvarcharRoundTripUsesTheInheritedWriteRules() throws Exception {
    HopClientEnvironment.init();
    MsSqlServerNativeDatabaseMeta nativeMeta = new MsSqlServerNativeDatabaseMeta();
    IValueMeta valueMeta =
        DatabaseTypeMapper.getValueMeta(
            new Variables(),
            TypeRuleFixture.meta(nativeMeta),
            TypeRuleFixture.column(Types.NVARCHAR, "nvarchar", 20, 20),
            false,
            false);
    assertEquals(
        "NVARCHAR(20)",
        nativeMeta.getColumnDefinition(
            valueMeta, null, null, false, false, false, ColumnContext.Purpose.CREATE));
  }

  @Test
  void testMsSqlOverrides() {
    MsSqlServerNativeDatabaseMeta localNativeMeta = new MsSqlServerNativeDatabaseMeta();
    localNativeMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);

    assertEquals("com.microsoft.sqlserver.jdbc.SQLServerDriver", localNativeMeta.getDriverClass());
    assertEquals(
        "jdbc:sqlserver://FOO:1234;databaseName=WIBBLE",
        localNativeMeta.getURL("FOO", "1234", "WIBBLE"));

    localNativeMeta.setUsingIntegratedSecurity(false);
    assertEquals(
        "jdbc:sqlserver://FOO:1234;databaseName=WIBBLE",
        localNativeMeta.getURL("FOO", "1234", "WIBBLE"));

    localNativeMeta.setUsingIntegratedSecurity(true);
    assertEquals(
        "jdbc:sqlserver://FOO:1234;databaseName=WIBBLE;integratedSecurity=true",
        localNativeMeta.getURL("FOO", "1234", "WIBBLE"));

    localNativeMeta.setInstanceName("TEST");
    assertEquals(
        "jdbc:sqlserver://FOO:1234;databaseName=WIBBLE;integratedSecurity=true",
        localNativeMeta.getURL("FOO", "1234", "WIBBLE"));

    localNativeMeta.setPort("");
    assertEquals(
        "jdbc:sqlserver://FOO\\TEST;databaseName=WIBBLE;integratedSecurity=true",
        localNativeMeta.getURL("FOO", "", "WIBBLE"));
  }

  @Test
  void setSqlServerInstanceTest() {
    DatabaseMeta dbmeta = new DatabaseMeta();
    IDatabase mssqlServerDatabaseMeta = new MsSqlServerDatabaseMeta();
    mssqlServerDatabaseMeta.setPluginId("MSSQL");
    IDatabase mssqlServerNativeDatabaseMeta = new MsSqlServerNativeDatabaseMeta();
    mssqlServerNativeDatabaseMeta.setPluginId("MSSQLNATIVE");
    dbmeta.setIDatabase(mssqlServerDatabaseMeta);
    dbmeta.setSqlServerInstance("");
    assertEquals(null, dbmeta.getSqlServerInstance());
    dbmeta.setSqlServerInstance("instance1");
    assertEquals("instance1", dbmeta.getSqlServerInstance());
    dbmeta.setIDatabase(mssqlServerNativeDatabaseMeta);
    dbmeta.setSqlServerInstance("");
    assertEquals(null, dbmeta.getSqlServerInstance());
    dbmeta.setSqlServerInstance("instance1");
    assertEquals("instance1", dbmeta.getSqlServerInstance());
  }
}
