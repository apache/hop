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
package org.apache.hop.core.database.types;

import java.sql.Types;
import java.util.List;
import org.apache.hop.core.row.IValueMeta;

/**
 * Reusable column rules, named after the behaviour rather than after a vendor.
 *
 * <p>These are shapes several dialects share, not one dialect's private business: MySQL, Hive and
 * SingleStore all read variable length binary the same way, because they all speak to a MySQL
 * driver. A dialect opts into the ones it wants from its own {@code getTypeRules()}; core never
 * decides which dialect gets what, which is the whole difference from the {@code isXVariant()}
 * switch these replace.
 *
 * <p>Keeping them here rather than in one dialect's plugin also means a dialect can share them
 * without taking a dependency on another database plugin, and that {@link LegacyVariantBridge} can
 * honour the deprecated flags without needing any particular plugin to be installed.
 */
public final class ColumnTypeRules {

  private ColumnTypeRules() {
    // Utility class.
  }

  /**
   * A YEAR column is reported as a date, but is really a four digit integer when the driver has
   * been told not to treat it as a date.
   */
  public static final IDatabaseTypeRule YEAR_AS_INTEGER =
      DatabaseTypes.rules()
          .read(Types.DATE, Types.TIME)
          .nativeName("YEAR")
          .where(
              (variables, databaseMeta, column) -> {
                String property =
                    databaseMeta.getConnectionProperties(variables).getProperty("yearIsDateType");
                return property != null && "false".equalsIgnoreCase(property);
              })
          .as(IValueMeta.TYPE_INTEGER, 4, 0)
          .build()
          .get(0);

  /**
   * Variable length binary carries no length, deliberately, so that string functions such as CONCAT
   * still work on the result.
   */
  public static final IDatabaseTypeRule UNSIZED_VARIABLE_BINARY =
      DatabaseTypes.rules()
          .read(Types.VARBINARY, Types.LONGVARBINARY)
          .where(ColumnTypeRules::notCharForBitData)
          .as(IValueMeta.TYPE_BINARY, -1, -1)
          .build()
          .get(0);

  /**
   * An approximate column reporting at least as many decimals as digits, which cannot be a real
   * declaration. MySQL reports (12,31) for a plain double.
   */
  public static final IDatabaseTypeRule OVERSCALED_APPROXIMATE_AS_UNSIZED_NUMBER =
      DatabaseTypes.rules()
          .read(Types.DOUBLE, Types.FLOAT, Types.REAL)
          .where(
              (variables, databaseMeta, column) ->
                  StandardJdbcTypeMapper.numericScale(column)
                      >= StandardJdbcTypeMapper.numericLength(column))
          .as(IValueMeta.TYPE_NUMBER, -1, -1)
          .build()
          .get(0);

  /** What a dialect speaking to a MySQL driver reads differently. */
  public static final List<IDatabaseTypeRule> MYSQL_COMPATIBLE =
      List.of(YEAR_AS_INTEGER, UNSIZED_VARIABLE_BINARY, OVERSCALED_APPROXIMATE_AS_UNSIZED_NUMBER);

  /**
   * The "CHAR(X) FOR BIT DATA" shape outranks every dialect rule for binary columns, and rules run
   * before the standard mapping, so the binary rules above have to defer to it explicitly.
   */
  private static boolean notCharForBitData(
      org.apache.hop.core.variables.IVariables variables,
      org.apache.hop.core.database.DatabaseMeta databaseMeta,
      DatabaseColumn column) {
    return !StandardJdbcTypeMapper.displaySizeIsTwiceThePrecision(databaseMeta, column);
  }
}
