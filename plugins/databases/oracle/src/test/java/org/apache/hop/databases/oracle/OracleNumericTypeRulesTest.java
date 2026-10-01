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

import static org.apache.hop.junit.database.TypeRuleFixture.meta;
import static org.apache.hop.junit.database.TypeRuleFixture.numericColumn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Types;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.types.DatabaseTypeMapper;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The numeric rules that used to be isOracleVariant() branches in core. */
class OracleNumericTypeRulesTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  private IValueMeta map(boolean strict, int precision, int scale) throws Exception {
    OracleDatabaseMeta dialect = new OracleDatabaseMeta();
    dialect.setStrictBigNumberInterpretation(strict);
    return DatabaseTypeMapper.getValueMeta(
        new Variables(),
        meta(dialect),
        numericColumn(Types.NUMERIC, "NUMBER", precision, scale),
        false,
        false);
  }

  @Test
  void aThirtyEightDigitNumberIsAnIntegerByDefault() throws Exception {
    IValueMeta valueMeta = map(false, 38, 0);

    assertTrue(valueMeta.isInteger());
    assertEquals(38, valueMeta.getLength());
  }

  @Test
  void theStrictReadingMakesItABigNumber() throws Exception {
    // The option is Oracle's own now, read off the connection rather than off the interface every
    // dialect implements.
    assertTrue(map(true, 38, 0).isBigNumber());
  }

  @Test
  void anUndefinedSizeIsABigNumber() throws Exception {
    IValueMeta valueMeta = map(false, 0, 0);

    assertTrue(valueMeta.isBigNumber());
    assertEquals(-1, valueMeta.getLength());
  }
}
