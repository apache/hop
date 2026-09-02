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

import java.sql.SQLException;
import java.sql.Types;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;

/**
 * The baseline mapping from a JDBC column to Hop value metadata.
 *
 * <p>Hop used to carry three copies of these rules — one in {@code Database}, one in {@code
 * ValueMetaBase.getValueFromSqlType} and one in {@code ValueMetaBase.getMetadataPreview} — which
 * had drifted apart in four places. This class is the single surviving copy, and it keeps the
 * semantics of the {@code Database} version because that is the path production used and the only
 * one of the three whose numeric handling round-trips (see JdbcTypeMappingCharacterizationTest).
 *
 * <p>The dialect-specific branches in here are the ones the type-mapping rework will move out to
 * the dialects themselves. Until then, adding a new one means adding a vendor name to core, which
 * is exactly the problem; prefer {@code IDatabase.customizeValueFromSqlType} in the meantime.
 */
public final class StandardJdbcTypeMapper {

  private static final String CONST_FALSE = "false";

  private StandardJdbcTypeMapper() {
    // Utility class.
  }

  /**
   * Maps a column onto Hop value metadata.
   *
   * @return the value metadata, or null when the SQL type is not one of the standard mappings, so
   *     that specialized value meta plugins (UUID, INET, JSON, ...) get their turn.
   */
  public static IValueMeta getValueMeta(
      IVariables variables,
      DatabaseMeta databaseMeta,
      DatabaseColumn column,
      boolean ignoreLength,
      boolean lazyConversion)
      throws HopDatabaseException {

    int sqlType = column.getSqlType();
    int valtype = IValueMeta.TYPE_NONE;
    int length = -1;
    int precision = -1;
    boolean isClob = false;

    switch (sqlType) {
      case Types.CHAR,
          Types.VARCHAR,
          Types.NVARCHAR,
          Types.LONGVARCHAR,
          Types.NCHAR,
          Types.LONGNVARCHAR:
        valtype = IValueMeta.TYPE_STRING;
        if (!ignoreLength) {
          length = column.getDisplaySize();
        }
        break;

      case Types.CLOB, Types.NCLOB:
        valtype = IValueMeta.TYPE_STRING;
        length = DatabaseMeta.CLOB_LENGTH;
        isClob = true;
        break;

      case Types.BIGINT:
        // An unsigned BIGINT overflows a Java Long, so it has to widen to BigNumber.
        if (column.isSigned()) {
          valtype = IValueMeta.TYPE_INTEGER;
          precision = 0; // Max 9.223.372.036.854.775.807
          length = 15;
        } else {
          valtype = IValueMeta.TYPE_BIGNUMBER;
          precision = 0; // Max 18.446.744.073.709.551.615
          length = 16;
        }
        break;

      case Types.INTEGER:
        valtype = IValueMeta.TYPE_INTEGER;
        precision = 0; // Max 2.147.483.647
        length = 9;
        break;

      case Types.SMALLINT:
        valtype = IValueMeta.TYPE_INTEGER;
        precision = 0; // Max 32.767
        length = 4;
        break;

      case Types.TINYINT:
        valtype = IValueMeta.TYPE_INTEGER;
        precision = 0; // Max 127
        length = 2;
        break;

      case Types.DECIMAL, Types.DOUBLE, Types.FLOAT, Types.REAL, Types.NUMERIC:
        Numeric numeric = mapDecimal(databaseMeta, column, sqlType);
        valtype = numeric.valtype();
        length = numeric.length();
        precision = numeric.precision();
        break;

      case Types.TIMESTAMP:
        if (databaseMeta.supportsTimestampDataType()) {
          valtype = IValueMeta.TYPE_TIMESTAMP;
          length = column.getScale();
        } else {
          valtype = IValueMeta.TYPE_DATE;
        }
        break;

      case Types.DATE, Types.TIME:
        // MySQL's YEAR and Teradata's date marker used to be decided here. They are dialect rules
        // now; see VariantTypeRules.
        valtype = IValueMeta.TYPE_DATE;
        break;

      case Types.BOOLEAN, Types.BIT:
        valtype = IValueMeta.TYPE_BOOLEAN;
        break;

      case Types.BINARY, Types.BLOB, Types.VARBINARY, Types.LONGVARBINARY:
        // Oracle RAW, MySQL variable length binary and SQLite's dynamic typing used to be decided
        // here. They are dialect rules now; see VariantTypeRules.
        valtype = IValueMeta.TYPE_BINARY;
        if (displaySizeIsTwiceThePrecision(databaseMeta, column)) {
          // The length of a "CHAR(X) FOR BIT DATA" column.
          length = column.getPrecision();
        } else {
          length = -1;
        }
        precision = -1;
        break;

      default:
        return null;
    }

    return build(
        databaseMeta, column, valtype, length, precision, isClob, ignoreLength, lazyConversion);
  }

  /**
   * The mapping used when no standard rule and no value meta plugin claims the column: treat it as
   * a string. Callers that must always produce a value (rather than deferring) use this.
   */
  public static IValueMeta getFallbackValueMeta(
      DatabaseMeta databaseMeta,
      DatabaseColumn column,
      boolean ignoreLength,
      boolean lazyConversion)
      throws HopDatabaseException {
    return build(
        databaseMeta,
        column,
        IValueMeta.TYPE_STRING,
        -1,
        column.getScale(),
        false,
        ignoreLength,
        lazyConversion);
  }

  /** The Hop type, length and precision decided for one numeric column. */
  private record Numeric(int valtype, int length, int precision) {}

  /** DECIMAL / NUMERIC / DOUBLE / FLOAT / REAL, which carry all size-dependent decisions. */
  /**
   * DECIMAL / NUMERIC / DOUBLE / FLOAT / REAL.
   *
   * <p>The dialect-specific adjustments that used to sit in here — Postgres double precision and
   * undefined numerics, MySQL's overstated scale, Oracle's 38 digit numbers and undefined sizes —
   * are dialect rules now; see ColumnTypeRules. Each of them fully determined the outcome, so they
   * express cleanly as rules even though they read here like adjustments part way through.
   */
  private static Numeric mapDecimal(DatabaseMeta databaseMeta, DatabaseColumn column, int sqlType) {
    int valtype = IValueMeta.TYPE_NUMBER;
    int length = numericLength(column);
    int precision = numericScale(column);

    if (isApproximateNumeric(sqlType)) {
      if (length > 15 || precision > 15) {
        valtype = IValueMeta.TYPE_BIGNUMBER;
      }
    } else {
      if (precision == 0) {
        if (length <= 18 && length > 0) {
          // A Long holds up to 18 significant digits.
          valtype = IValueMeta.TYPE_INTEGER;
        } else if (length > 18) {
          valtype = IValueMeta.TYPE_BIGNUMBER;
        }
      } else if (length > 15 || precision > 15) {
        valtype = IValueMeta.TYPE_BIGNUMBER;
      }
    }

    return new Numeric(valtype, length, precision);
  }

  /** The tail every branch shares: build the value meta, attach original metadata, customize. */
  private static IValueMeta build(
      DatabaseMeta databaseMeta,
      DatabaseColumn column,
      int valtype,
      int length,
      int precision,
      boolean isClob,
      boolean ignoreLength,
      boolean lazyConversion)
      throws HopDatabaseException {
    try {
      IValueMeta valueMeta = ValueMetaFactory.createValueMeta(column.getName(), valtype);
      valueMeta.setLength(length);
      valueMeta.setPrecision(precision);
      valueMeta.setLargeTextField(isClob);

      setOriginalColumnMetadata(valueMeta, column, ignoreLength);

      if (lazyConversion && valtype == IValueMeta.TYPE_STRING) {
        valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
        IValueMeta storageMetaData =
            ValueMetaFactory.cloneValueMeta(valueMeta, IValueMeta.TYPE_STRING);
        storageMetaData.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
        valueMeta.setStorageMetadata(storageMetaData);
      }

      // Only the result set metadata path can offer this hook, which is what it took before.
      if (column.getResultSetMetaData() != null) {
        IValueMeta customized =
            databaseMeta
                .getIDatabase()
                .customizeValueFromSqlType(
                    valueMeta, column.getResultSetMetaData(), column.getColumnIndex());
        if (customized != null) {
          return customized;
        }
      }
      return valueMeta;
    } catch (SQLException | RuntimeException | org.apache.hop.core.exception.HopPluginException e) {
      throw new HopDatabaseException(
          "Error determining value metadata from SQL resultset metadata", e);
    }
  }

  /** True for the approximate numeric types, whose reported scale is not meaningful. */
  public static boolean isApproximateNumeric(int sqlType) {
    return sqlType == Types.DOUBLE || sqlType == Types.FLOAT || sqlType == Types.REAL;
  }

  /**
   * The Hop length of a numeric column: the total number of significant digits, the same thing the
   * database reports as its precision.
   *
   * <p>Every getFieldDefinition writes a scaled column as DECIMAL(length, precision), and the
   * length a user types into a transform dialog means the same total. Taking the scale off here
   * made the value read from a column incompatible with both. A value at or beyond 126 means the
   * database did not really say.
   */
  public static int numericLength(DatabaseColumn column) {
    int length = column.getPrecision();
    return length >= 126 ? -1 : length;
  }

  /**
   * The digits before the decimal: the reported precision with the scale taken off. Only a rule
   * asking whether a declaration is possible at all needs this - the Hop length of the column is
   * numericLength. The 126 marker is applied after the subtraction, as it always was.
   */
  public static int integerDigits(DatabaseColumn column) {
    int length = column.getPrecision();
    int scale = column.getScale();
    if (length > 0 && length > scale) {
      length -= scale;
    }
    return length >= 126 ? -1 : length;
  }

  /** The Hop precision of a numeric column: the digits after the decimal. */
  public static int numericScale(DatabaseColumn column) {
    int scale = column.getScale();
    if (scale >= 126) {
      return -1;
    }
    // A scale of zero is meaningless on an approximate type.
    return isApproximateNumeric(column.getSqlType()) && scale == 0 ? -1 : scale;
  }

  /**
   * True when the column has the "CHAR(X) FOR BIT DATA" shape: some dialects report a binary
   * column's display size as twice its precision.
   *
   * <p>Public because this check outranks every dialect rule for binary columns, and rules are
   * consulted before the standard mapping, so a dialect rule has to defer to it explicitly.
   */
  public static boolean displaySizeIsTwiceThePrecision(
      DatabaseMeta databaseMeta, DatabaseColumn column) {
    return databaseMeta.isDisplaySizeTwiceThePrecision()
        && (2 * column.getPrecision()) == column.getDisplaySize();
  }

  /** Records what the database actually reported, for transforms that need the raw truth. */
  public static void setOriginalColumnMetadata(
      IValueMeta valueMeta, DatabaseColumn column, boolean ignoreLength) {
    valueMeta.setComments(column.getComment());
    valueMeta.setOriginalColumnType(column.getSqlType());
    valueMeta.setOriginalColumnTypeName(column.getNativeTypeName());
    valueMeta.setOriginalPrecision(ignoreLength ? -1 : column.getPrecision());
    valueMeta.setOriginalScale(column.getScale());
    valueMeta.setOriginalSigned(column.isSigned());
  }
}
