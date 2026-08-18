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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.Types;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Postgres takes JSON as a typed object rather than a string. This used to be an
 * isPostgresVariant() check inside ValueMetaJson.
 */
class PostgreSqlValueBindingTest {

  private DatabaseMeta databaseMeta;
  private PreparedStatement preparedStatement;
  private Database database;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.getIDatabase()).thenReturn(new PostgreSqlDatabaseMeta());
    preparedStatement = mock(PreparedStatement.class);
    ILoggingObject log = mock(ILoggingObject.class);
    when(log.getLogLevel()).thenReturn(LogLevel.NOTHING);
    database = new Database(log, new Variables(), databaseMeta);
  }

  @Test
  void jsonIsSentAsATypedObject() throws Exception {
    IValueMeta valueMeta = new ValueMetaJson("payload");
    database.setValue(preparedStatement, valueMeta, "{\"a\":1}", 1);

    verify(preparedStatement).setObject(eq(1), any(), eq(Types.OTHER));
  }

  @Test
  void aNullIsSentAsANullOfTheSameType() throws Exception {
    IValueMeta valueMeta = new ValueMetaJson("payload");
    database.setValue(preparedStatement, valueMeta, null, 1);

    verify(preparedStatement).setNull(1, Types.OTHER);
  }
}
