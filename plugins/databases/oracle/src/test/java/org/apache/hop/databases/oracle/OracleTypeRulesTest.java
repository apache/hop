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

package org.apache.hop.databases.oracle;

import static org.apache.hop.junit.database.TypeRuleFixture.column;
import static org.apache.hop.junit.database.TypeRuleFixture.meta;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Types;
import java.util.Properties;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.types.DatabaseTypeMapper;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The RAW handling that used to be an isOracleVariant() branch in core. */
class OracleTypeRulesTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  private IValueMeta map(boolean twicePrecision, int sqlType, int precision, int displaySize)
      throws Exception {
    return DatabaseTypeMapper.getValueMeta(
        new Variables(),
        meta(new OracleDatabaseMeta(), new Properties(), twicePrecision),
        column(sqlType, "RAW", precision, displaySize),
        false,
        false);
  }

  @Test
  void rawAndLongRawAreReadAsStrings() throws Exception {
    for (int sqlType : new int[] {Types.VARBINARY, Types.LONGVARBINARY}) {
      IValueMeta valueMeta = map(false, sqlType, 8, 20);
      assertTrue(valueMeta.isString());
      assertEquals(20, valueMeta.getLength());
    }
  }

  @Test
  void plainBinaryAndBlobStayBinary() throws Exception {
    assertTrue(map(false, Types.BLOB, 8, 20).isBinary());
    assertTrue(map(false, Types.BINARY, 8, 20).isBinary());
  }

  @Test
  void theCharForBitDataShapeStillOutranksTheDialectRule() throws Exception {
    IValueMeta valueMeta = map(true, Types.VARBINARY, 8, 16);

    assertTrue(valueMeta.isBinary());
    assertEquals(8, valueMeta.getLength());
  }
}
