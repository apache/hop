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

package org.apache.hop.databases.mssql;

import static org.apache.hop.junit.database.TypeRuleFixture.meta;
import static org.apache.hop.junit.database.TypeRuleFixture.numericColumn;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Types;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.types.DatabaseTypeMapper;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Reading a numeric column and writing it back has to give the same declaration. A pipeline that
 * reads a table and re-creates it elsewhere does exactly that, and getFieldDefinition writes a
 * scaled column as DECIMAL(length, precision) - so the length read off a column has to be its total
 * number of digits, not the digits before the decimal.
 */
class MsSqlServerNumericRoundTripTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  private static String roundTrip(int precision, int scale) throws Exception {
    MsSqlServerDatabaseMeta databaseMeta = new MsSqlServerDatabaseMeta();
    IValueMeta valueMeta =
        DatabaseTypeMapper.getValueMeta(
            new Variables(),
            meta(databaseMeta),
            numericColumn(Types.DECIMAL, "decimal", precision, scale),
            false,
            false);
    return databaseMeta.getFieldDefinition(valueMeta, null, null, false, false, false);
  }

  @Test
  void aScaledDecimalKeepsItsDeclaration() throws Exception {
    assertEquals("DECIMAL(5,2)", roundTrip(5, 2));
    assertEquals("DECIMAL(10,6)", roundTrip(10, 6));
    assertEquals("DECIMAL(24,15)", roundTrip(24, 15));
  }

  @Test
  void anUnscaledDecimalKeepsItsDeclaration() throws Exception {
    assertEquals("DECIMAL(38,0)", roundTrip(38, 0));
  }

  @Test
  void aNarrowUnscaledDecimalStillBecomesAnIntegerColumn() throws Exception {
    // Unchanged by the length meaning: an unscaled decimal that fits a Long is written as one.
    assertEquals("INT", roundTrip(3, 0));
    assertEquals("BIGINT", roundTrip(10, 0));
  }
}
