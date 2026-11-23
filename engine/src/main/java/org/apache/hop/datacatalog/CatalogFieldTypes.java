/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.datacatalog;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.datacatalog.ISchemaCapability.CatalogField;

/**
 * Turns the fields of a dataset in a data catalog into Hop field definitions, for a transform
 * dialog to start from.
 *
 * <p>The native type comes first: {@code VARCHAR(255)} is a String of length 255, {@code
 * DECIMAL(10,2)} a BigNumber with length 10 and precision 2, and the names the Hop lineage sinks
 * write ({@code string}, {@code int}, {@code double}, {@code decimal}, ...) map back to the Hop
 * type they came from. A native type it doesn't know falls back to the catalog's own type ({@code
 * STRING}, {@code NUMBER}, ...), and anything else becomes a String. A catalog has no formats, so
 * no field gets one: a date then uses Hop's default format, which is what Hop itself writes to a
 * file when no format is set.
 */
public final class CatalogFieldTypes {

  /** Longest integer that still fits a Hop Integer. */
  private static final int MAX_INTEGER_DIGITS = 18;

  /** A type name with an optional length and scale: {@code DECIMAL(10, 2)}, {@code varchar(20)}. */
  private static final Pattern NATIVE_TYPE =
      Pattern.compile("^\\s*([a-z][a-z0-9_ ]*)(?:\\(\\s*(\\d+)\\s*(?:,\\s*(-?\\d+)\\s*)?\\))?");

  /** Modifiers that don't change the type: {@code INT UNSIGNED}, {@code INT(11) ZEROFILL}. */
  private static final Pattern MODIFIERS = Pattern.compile("\\b(?:unsigned|signed|zerofill)\\b");

  /** Integer types whose unsigned values can be larger than a Hop Integer holds. */
  private static final Set<String> SIXTY_FOUR_BIT_INTEGERS =
      Set.of("bigint", "int8", "bigserial", "int64");

  private CatalogFieldTypes() {}

  /** The Hop fields of a catalog schema, in its order. */
  public static IRowMeta toRowMeta(List<CatalogField> fields) {
    IRowMeta rowMeta = new RowMeta();
    for (CatalogField field : fields) {
      rowMeta.addValueMeta(toValueMeta(field));
    }
    return rowMeta;
  }

  /** The Hop field for a catalog field. */
  public static IValueMeta toValueMeta(CatalogField field) {
    IValueMeta valueMeta = fromNativeType(field.name(), field.nativeType());
    if (valueMeta == null) {
      valueMeta = fromCatalogType(field.name(), field.type());
    }
    // A number comes with a default mask of its own; a catalog says nothing about formats
    valueMeta.setConversionMask(null);
    return valueMeta;
  }

  private static IValueMeta fromNativeType(String name, String nativeType) {
    if (StringUtils.isBlank(nativeType)) {
      return null;
    }
    String lowerCase = nativeType.toLowerCase(Locale.ROOT);
    // MySQL writes the modifier after the length: bigint(20) unsigned
    boolean unsigned = lowerCase.matches(".*\\bunsigned\\b.*");
    Matcher matcher =
        NATIVE_TYPE.matcher(MODIFIERS.matcher(lowerCase).replaceAll(" ").replaceAll("\\s+", " "));
    if (!matcher.lookingAt()) {
      return null;
    }
    String typeName = matcher.group(1).trim();
    int length = number(matcher.group(2));
    int scale = number(matcher.group(3));
    if (unsigned && SIXTY_FOUR_BIT_INTEGERS.contains(typeName)) {
      // Up to 18446744073709551615 doesn't fit a Hop Integer; Hop reads it from a database the same
      IValueMeta bigNumber = new ValueMetaBigNumber(name);
      bigNumber.setPrecision(0);
      return bigNumber;
    }
    IValueMeta valueMeta = fromTypeName(name, typeName, length, scale);
    if (valueMeta == null && typeName.contains(" ")) {
      // A dialect's spelling such as timestamp with local time zone: the first word is the type
      valueMeta = fromTypeName(name, typeName.substring(0, typeName.indexOf(' ')), length, scale);
    }
    return valueMeta;
  }

  private static IValueMeta fromTypeName(String name, String typeName, int length, int scale) {
    return switch (typeName) {
      case "varchar",
              "varchar2",
              "nvarchar",
              "nvarchar2",
              "char",
              "nchar",
              "character",
              "character varying",
              "bpchar",
              "text",
              "string",
              "clob",
              "nclob",
              "uuid",
              "long varchar",
              "long nvarchar" ->
          withLength(new ValueMetaString(name), length, -1);
      case "int",
              "integer",
              "bigint",
              "smallint",
              "tinyint",
              "mediumint",
              "long",
              "int2",
              "int4",
              "int8",
              "int64",
              "serial",
              "bigserial" ->
          new ValueMetaInteger(name);
      case "decimal", "numeric", "number", "bignumber" ->
          length > 0 && length <= MAX_INTEGER_DIGITS && scale <= 0
              ? withLength(new ValueMetaInteger(name), length, 0)
              : withLength(new ValueMetaBigNumber(name), length, scale);
      case "double",
              "double precision",
              "float",
              "float4",
              "float8",
              "float64",
              "real",
              "binary_double" ->
          new ValueMetaNumber(name);
      case "boolean", "bool" -> new ValueMetaBoolean(name);
        // bit(n) with n > 1 is a string of bits such as 10110000, not a flag
      case "bit" ->
          length > 1
              ? withLength(new ValueMetaString(name), length, -1)
              : new ValueMetaBoolean(name);
      case "bit varying", "varbit" -> withLength(new ValueMetaString(name), length, -1);
      case "date" -> new ValueMetaDate(name);
      case "datetime", "datetime2", "smalldatetime" -> new ValueMetaDate(name);
      case "time" -> new ValueMetaDate(name);
      case "timestamp",
              "timestamptz",
              "timestamp with time zone",
              "timestamp without time zone",
              "timestamp with local time zone",
              "timestamp_ntz",
              "timestamp_ltz",
              "timestamp_tz",
              "datetimeoffset" ->
          new ValueMetaTimestamp(name);
      case "binary",
              "varbinary",
              "blob",
              "longblob",
              "bytea",
              "bytes",
              "raw",
              "long raw",
              "long varbinary" ->
          new ValueMetaBinary(name);
      default -> null;
    };
  }

  /** DataHub's generic field types, the names of SchemaFieldDataType. */
  private static IValueMeta fromCatalogType(String name, String type) {
    String typeName = StringUtils.defaultString(type).trim().toUpperCase(Locale.ROOT);
    return switch (typeName) {
      case "NUMBER" -> new ValueMetaNumber(name);
      case "BOOLEAN" -> new ValueMetaBoolean(name);
      case "DATE" -> new ValueMetaDate(name);
        // DataHub's TimeType holds times and datetimes; a native time or datetime is a Date too
      case "TIME" -> new ValueMetaDate(name);
      case "BYTES", "FIXED" -> new ValueMetaBinary(name);
      default -> new ValueMetaString(name);
    };
  }

  /**
   * A length or scale, or -1 when there is none or it doesn't fit an int, as a MySQL LONGTEXT shown
   * as {@code varchar(4294967295)}.
   */
  private static int number(String digits) {
    if (digits == null) {
      return -1;
    }
    try {
      return Integer.parseInt(digits);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private static IValueMeta withLength(IValueMeta valueMeta, int length, int precision) {
    if (length > 0) {
      valueMeta.setLength(length);
    }
    if (precision >= 0 && length > 0) {
      valueMeta.setPrecision(precision);
    }
    return valueMeta;
  }
}
