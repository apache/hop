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

package org.apache.hop.databases.sqlite;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.types.IValueBinding;
import org.apache.hop.core.row.IValueMeta;

/**
 * Reading a column declared NUMERIC, DECIMAL or NUMBER on SQLite, exactly.
 *
 * <p>Such a column has numeric affinity: SQLite stores each value as an integer when it is a whole
 * number, as a real otherwise, and keeps text it cannot convert as text (<a
 * href="https://www.sqlite.org/datatype3.html">datatype3</a>). The JDBC driver types the column
 * from the value on the current row once a query runs, so the same column is INTEGER on one row and
 * NUMERIC on the next, while the prepared statement said NUMERIC for all of them. Hop describes a
 * query both ways and needs the two to agree (issue #3633).
 *
 * <p>So the column is a BigNumber whatever the row holds, read through {@link ResultSet#getObject}:
 * an integer stays exact to the last digit, where a double loses anything past 2^53, and a real
 * keeps the digits it prints with. Text that is not a number cannot be a BigNumber; rather than
 * turn it into zero, the way getDouble does, the read fails and says which value it was.
 */
final class SqliteNumericValues {

  /** Declared type names, with or without a size, read as exact numbers. */
  static final String DECLARED_TYPE = "\\s*(NUMERIC|DECIMAL|NUMBER)\\s*(\\(.*\\))?\\s*";

  /** Reads exactly; writing a BigNumber is the default handling. */
  static final IValueBinding BINDING =
      new IValueBinding() {
        @Override
        public Object read(IDatabase database, IValueMeta valueMeta, ResultSet resultSet, int index)
            throws SQLException {
          return toBigDecimal(resultSet.getObject(index), valueMeta.getName());
        }

        @Override
        public void write(
            IDatabase database,
            IValueMeta valueMeta,
            PreparedStatement preparedStatement,
            int index,
            Object value) {
          throw new UnsupportedOperationException("This binding only reads values");
        }
      };

  private SqliteNumericValues() {
    // Utility class.
  }

  static BigDecimal toBigDecimal(Object value, String columnName) throws SQLException {
    if (value == null) {
      return null;
    }
    if (value instanceof BigDecimal bigDecimal) {
      return bigDecimal;
    }
    if (value instanceof Long || value instanceof Integer || value instanceof Short) {
      return BigDecimal.valueOf(((Number) value).longValue());
    }
    try {
      if (value instanceof Double || value instanceof Float) {
        // valueOf, not new BigDecimal(double): 2.5 stays 2.5 and 0.1 stays 0.1.
        return withoutNegativeScale(BigDecimal.valueOf(((Number) value).doubleValue()));
      }
      if (value instanceof String text) {
        return withoutNegativeScale(new BigDecimal(text.trim()));
      }
    } catch (NumberFormatException e) {
      // Not a number: reported below.
    }
    throw new SQLException(
        "Column '"
            + columnName
            + "' is declared as a number but holds "
            + describe(value)
            + ". Read it as text with CAST("
            + columnName
            + " AS TEXT) to keep such values.");
  }

  /**
   * 55487400.0 comes out of valueOf as 5.54874E+7, a negative scale that toString() and every
   * driver inlining the value then print in scientific notation. Rescaling to zero is exact. The
   * same as ValueMetaBase.convertDoubleToBigNumber.
   */
  private static BigDecimal withoutNegativeScale(BigDecimal number) {
    return number.scale() < 0 ? number.setScale(0) : number;
  }

  private static String describe(Object value) {
    if (value instanceof byte[] bytes) {
      return "a blob of " + bytes.length + " bytes";
    }
    return "the value '" + value + "'";
  }
}
