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

package org.apache.hop.databases.sqlite;

import static org.apache.hop.junit.database.TypeRuleFixture.column;
import static org.apache.hop.junit.database.TypeRuleFixture.meta;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Types;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.types.DatabaseTypeMapper;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The binary handling that used to be an isSqliteVariant() branch in core. */
class SqliteTypeRulesTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void binaryColumnsAreReadAsStringsBecauseTypingIsDynamic() throws Exception {
    for (int sqlType : new int[] {Types.BINARY, Types.BLOB, Types.VARBINARY, Types.LONGVARBINARY}) {
      IValueMeta valueMeta =
          DatabaseTypeMapper.getValueMeta(
              new Variables(),
              meta(new SqliteDatabaseMeta()),
              column(sqlType, "BLOB", 0, 0),
              false,
              false);
      assertTrue(valueMeta.isString(), "SQL type " + sqlType + " should be read as a string");
    }
  }
}
