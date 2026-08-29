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
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;

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

  /**
   * An integer with no declared length must still be written as a column that holds one. A Hop
   * Integer is a 64 bit Long whatever its length says, so a length of zero or -1 means "the full
   * width", not "no width".
   *
   * <p>Without this the value falls into whichever branch of a dialect's size ladder happens to
   * catch a length of zero. Ten dialects answer with a floating point column, which cannot hold a
   * large Long exactly (issue #4174); others answer with a one byte column, which cannot hold most
   * of them at all. Neither is a decision anyone made.
   *
   * <p>The width used is {@value #LONG_DIGITS} digits, the most a Long is guaranteed to hold, and
   * the same boundary {@link StandardJdbcTypeMapper} uses when it reads such a column back. The
   * dialect is then asked how it spells an integer that wide, so no type name is invented here and
   * every database keeps its own vocabulary.
   *
   * <p>Key columns are left to the dialect: a technical or primary key already has its own spelling
   * wherever one exists, and it is not a plain integer column.
   */
  public static final IDatabaseTypeRule UNSIZED_INTEGER_AS_LONG = new UnsizedIntegerRule();

  /** The most significant digits a Java Long is guaranteed to hold. */
  private static final int LONG_DIGITS = 18;

  /**
   * Asks the dialect for its own spelling of a Long wide integer rather than naming a type, which
   * is what lets one rule serve databases that spell it BIGINT, INT64, NUMERIC(18) or DECIMAL(18,
   * 0).
   */
  private static final class UnsizedIntegerRule implements IDatabaseTypeRule {

    @Override
    public String getColumnType(
        IVariables variables, IDatabase database, IValueMeta valueMeta, ColumnContext context) {

      if (valueMeta.getType() != IValueMeta.TYPE_INTEGER || valueMeta.getLength() > 0) {
        return null;
      }
      if (context.isKey(valueMeta.getName())) {
        return null;
      }

      // A copy: a dialect's getFieldDefinition is free to modify the value it is handed, and
      // several do.
      IValueMeta asLong = valueMeta.clone();
      asLong.setLength(LONG_DIGITS);
      asLong.setPrecision(0);

      return database.getFieldDefinition(
          asLong,
          context.getTechnicalKeyField(),
          context.getPrimaryKeyField(),
          context.isUseAutoIncrement(),
          false,
          false);
    }
  }

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
