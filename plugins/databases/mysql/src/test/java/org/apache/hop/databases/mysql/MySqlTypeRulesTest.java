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

package org.apache.hop.databases.mysql;

import static org.apache.hop.junit.database.TypeRuleFixture.column;
import static org.apache.hop.junit.database.TypeRuleFixture.meta;
import static org.apache.hop.junit.database.TypeRuleFixture.numericColumn;
import static org.apache.hop.junit.database.TypeRuleFixture.properties;
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

/** The rules that used to be isMySqlVariant() branches in core. */
class MySqlTypeRulesTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  private IValueMeta map(
      Properties connectionProperties,
      boolean twicePrecision,
      int sqlType,
      String typeName,
      int precision,
      int displaySize)
      throws Exception {
    return DatabaseTypeMapper.getValueMeta(
        new Variables(),
        meta(new MySqlDatabaseMeta(), connectionProperties, twicePrecision),
        column(sqlType, typeName, precision, displaySize),
        false,
        false);
  }

  private IValueMeta mapNumeric(int sqlType, String typeName, int precision, int scale)
      throws Exception {
    return DatabaseTypeMapper.getValueMeta(
        new Variables(),
        meta(new MySqlDatabaseMeta()),
        numericColumn(sqlType, typeName, precision, scale),
        false,
        false);
  }

  @Test
  void aDoubleReportingMoreDecimalsThanDigitsHasNoUsableSize() throws Exception {
    // MySQL reports (12,31) for a plain double: nobody declared that.
    IValueMeta valueMeta = mapNumeric(Types.DOUBLE, "DOUBLE", 12, 31);

    assertTrue(valueMeta.isNumber());
    assertEquals(-1, valueMeta.getLength());
    assertEquals(-1, valueMeta.getPrecision());
  }

  @Test
  void aDoubleWithMoreDecimalsThanIntegerDigitsHasNoUsableSizeEither() throws Exception {
    // (10,6) leaves four digits before the decimal and asks for six after it. The rule compares
    // the scale against those four, not against the ten - so it still claims the column.
    IValueMeta valueMeta = mapNumeric(Types.DOUBLE, "DOUBLE", 10, 6);

    assertTrue(valueMeta.isNumber());
    assertEquals(-1, valueMeta.getLength());
    assertEquals(-1, valueMeta.getPrecision());
  }

  @Test
  void aDoubleWithRoomForItsDecimalsKeepsItsSize() throws Exception {
    IValueMeta valueMeta = mapNumeric(Types.DOUBLE, "DOUBLE", 10, 2);

    assertTrue(valueMeta.isNumber());
    assertEquals(10, valueMeta.getLength());
    assertEquals(2, valueMeta.getPrecision());
  }

  @Test
  void yearIsAnIntegerWhenTheDriverSaysItIsNotADate() throws Exception {
    IValueMeta valueMeta =
        map(properties("yearIsDateType", "false"), false, Types.DATE, "YEAR", 4, 4);

    assertTrue(valueMeta.isInteger());
    assertEquals(4, valueMeta.getLength());
    assertEquals(0, valueMeta.getPrecision());
  }

  @Test
  void yearStaysADateByDefault() throws Exception {
    assertTrue(map(new Properties(), false, Types.DATE, "YEAR", 4, 4).isDate());
  }

  @Test
  void onlyColumnsActuallyNamedYearAreAffected() throws Exception {
    assertTrue(
        map(properties("yearIsDateType", "false"), false, Types.DATE, "DATE", 0, 0).isDate());
  }

  @Test
  void variableLengthBinaryKeepsNoLengthSoStringFunctionsStillWork() throws Exception {
    IValueMeta valueMeta = map(new Properties(), false, Types.VARBINARY, "VARBINARY", 16, 16);

    assertTrue(valueMeta.isBinary());
    assertEquals(-1, valueMeta.getLength());
  }

  @Test
  void theCharForBitDataShapeStillOutranksTheDialectRule() throws Exception {
    IValueMeta valueMeta = map(new Properties(), true, Types.VARBINARY, "VARBINARY", 8, 16);

    assertTrue(valueMeta.isBinary());
    assertEquals(8, valueMeta.getLength());
  }
}
