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
package org.apache.hop.core.database.validation;

import java.sql.Types;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hop.core.database.types.DatabaseColumn;
import org.apache.hop.core.row.IValueMeta;

/**
 * What a target table column will reject, independent of any one row. Built once at transform init
 * from JDBC metadata plus dialect enrichers.
 */
@Getter
@Setter
@NoArgsConstructor
public class ColumnValueConstraints {

  private String columnName;
  private String nativeTypeName;
  private int sqlType;
  private int hopType;

  /** Used to convert the stream value toward the column type. May be null in unit tests. */
  private IValueMeta targetValueMeta;

  private boolean nullable = true;

  /** True when the column has a declared default (including nextval() for serials). */
  private boolean hasDefault;

  /** Maximum string length, or -1 when unlimited. */
  private int stringMaxLength = -1;

  private StringLengthUnit lengthUnit = StringLengthUnit.CHARACTERS;

  /** Reject U+0000 in character data (PostgreSQL text/varchar). */
  private boolean rejectNulChar;

  /** Database encoding name, e.g. UTF8 or UTF-8. */
  private String characterSet;

  /** NUMERIC/DECIMAL precision, or -1 when not applicable / unlimited. */
  private int numericPrecision = -1;

  /** NUMERIC/DECIMAL scale, or -1 when not applicable. */
  private int numericScale = -1;

  private Long integerMin;
  private Long integerMax;

  private boolean uuid;
  private boolean json;

  /**
   * @return true when an insert would fail if this column is not mapped and not supplied.
   */
  public boolean isRequiredWithoutDefault() {
    return !nullable && !hasDefault;
  }

  /** Generic JDBC-based constraints used by {@code IDatabase} and dialect enrichers. */
  public static void enrichFromJdbc(ColumnValueConstraints spec, DatabaseColumn column) {
    if (spec == null || column == null) {
      return;
    }
    int sqlType = column.getSqlType();
    int length = column.getDisplaySize() > 0 ? column.getDisplaySize() : column.getPrecision();
    switch (sqlType) {
      case Types.CHAR,
          Types.VARCHAR,
          Types.NCHAR,
          Types.NVARCHAR,
          Types.LONGVARCHAR,
          Types.LONGNVARCHAR:
        if (ColumnValueValidator.hasLimitedLength(length)) {
          spec.setStringMaxLength(length);
          spec.setLengthUnit(StringLengthUnit.CHARACTERS);
        }
        break;
      case Types.DECIMAL, Types.NUMERIC:
        if (column.getPrecision() > 0) {
          spec.setNumericPrecision(column.getPrecision());
          spec.setNumericScale(Math.max(column.getScale(), 0));
        }
        break;
      case Types.TINYINT:
        spec.setIntegerMin(-128L);
        spec.setIntegerMax(127L);
        break;
      case Types.SMALLINT:
        spec.setIntegerMin(-32768L);
        spec.setIntegerMax(32767L);
        break;
      case Types.INTEGER:
        spec.setIntegerMin((long) Integer.MIN_VALUE);
        spec.setIntegerMax((long) Integer.MAX_VALUE);
        break;
      case Types.BIGINT:
        spec.setIntegerMin(Long.MIN_VALUE);
        spec.setIntegerMax(Long.MAX_VALUE);
        break;
      default:
        break;
    }
  }

  @Override
  public String toString() {
    return columnName
        + " native="
        + nativeTypeName
        + " nullable="
        + nullable
        + " length="
        + stringMaxLength
        + " numeric="
        + numericPrecision
        + ","
        + numericScale
        + " int=["
        + integerMin
        + ","
        + integerMax
        + "]"
        + (uuid ? " uuid" : "")
        + (json ? " json" : "")
        + (rejectNulChar ? " rejectNul" : "");
  }
}
