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

/** The numeric rules that used to be isPostgresVariant() branches in core. */
class PostgreSqlTypeRulesTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  private IValueMeta map(int sqlType, String typeName, int precision, int scale) throws Exception {
    return DatabaseTypeMapper.getValueMeta(
        new Variables(),
        meta(new PostgreSqlDatabaseMeta()),
        numericColumn(sqlType, typeName, precision, scale),
        false,
        false);
  }

  @Test
  void aDoublePrecisionColumnReportsNoUsableSize() throws Exception {
    // The driver reports 17 significant digits and 17 decimals for float8: the widest a double
    // can hold, not a declared size.
    IValueMeta valueMeta = map(Types.DOUBLE, "float8", 17, 17);

    assertTrue(valueMeta.isNumber());
    assertEquals(-1, valueMeta.getLength());
    assertEquals(-1, valueMeta.getPrecision());
  }

  @Test
  void anUndefinedNumericMeansArbitraryPrecision() throws Exception {
    IValueMeta valueMeta = map(Types.NUMERIC, "numeric", 0, 0);

    assertTrue(valueMeta.isBigNumber());
    assertEquals(-1, valueMeta.getLength());
  }

  @Test
  void asizedNumericIsUnaffected() throws Exception {
    IValueMeta valueMeta = map(Types.NUMERIC, "numeric", 10, 2);

    assertTrue(valueMeta.isNumber());
    // Hop length is the total number of significant digits, the same as the database precision.
    assertEquals(10, valueMeta.getLength());
    assertEquals(2, valueMeta.getPrecision());
  }

  /**
   * An address is Types.OTHER, which the standard mapping takes as a string. The dialect has to
   * name it, or the value type Hop has for addresses never sees the column.
   */
  @Test
  void anAddressColumnIsReadAsAnInternetAddress() throws Exception {
    IValueMeta valueMeta = map(Types.OTHER, "inet", 0, 0);

    assertEquals(IValueMeta.TYPE_INET, valueMeta.getType());
    assertEquals(-1, valueMeta.getLength());
    assertEquals(-1, valueMeta.getPrecision());
  }
}
