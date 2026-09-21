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

package org.apache.hop.databases.oracle;

import java.io.StringReader;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.types.IValueBinding;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;

/**
 * How Oracle writes strings: the national-character columns (NVARCHAR2/NCHAR/NCLOB) that the driver
 * will not accept through {@code setString} without converting them to the database character set,
 * and the LOB columns that need a stream bind so a batch mixing short and long values does not
 * raise ORA-01461.
 *
 * <p>Which column a value is bound to is not something the value's own metadata knows -- a string
 * is a string whether it is going into a VARCHAR2 or an NVARCHAR2 -- but the statement does. The
 * Oracle driver parses the SQL behind {@link PreparedStatement#getParameterMetaData()} and
 * describes the target columns, so the column type of every bind parameter is asked from the
 * statement itself, once, and kept for as long as the statement lives. A driver that cannot say (an
 * old one, or a statement it cannot parse) leaves the value written the way Hop always wrote it,
 * with {@code setString}.
 *
 * <p>Write only. {@link #read} throws, so reading a string off an Oracle result set stays exactly
 * what it was before this binding existed -- see {@link
 * org.apache.hop.core.database.BaseDatabaseMeta#getValueFromResultSet}. Nothing was wrong with
 * reads.
 */
final class OraclePreparedStatementBinding implements IValueBinding {

  static final OraclePreparedStatementBinding INSTANCE = new OraclePreparedStatementBinding();

  /** What the statement could not tell us: bound the way Hop always bound a string. */
  private static final int UNKNOWN_COLUMN_TYPE = Types.OTHER;

  /**
   * The column types of every statement this binding has written to, by statement identity. The
   * driver caches parameter metadata per SQL text as well, but asking it costs a lock and a lookup
   * per value, and this is the innermost loop of every Oracle insert. Entries go when the statement
   * is collected.
   */
  private static final Map<PreparedStatement, int[]> COLUMN_TYPES =
      Collections.synchronizedMap(new WeakHashMap<>());

  private OraclePreparedStatementBinding() {}

  /**
   * Declining to read costs an exception per value, and unlike the bindings that decline a JSON or
   * a date column this one is asked about every string Oracle reads -- the innermost loop there is.
   * So it is thrown without a stack trace nobody looks at, and without allocating.
   */
  private static final class ReadNotSupported extends UnsupportedOperationException {
    private ReadNotSupported() {
      super("This binding only writes values");
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }

  private static final ReadNotSupported READ_NOT_SUPPORTED = new ReadNotSupported();

  @Override
  public Object read(IDatabase database, IValueMeta valueMeta, ResultSet resultSet, int index) {
    // Declared for writing only. The caller falls back to the value type's own reading.
    throw READ_NOT_SUPPORTED;
  }

  @Override
  public void write(
      IDatabase database,
      IValueMeta valueMeta,
      PreparedStatement preparedStatement,
      int index,
      Object value)
      throws SQLException, HopValueException {
    if (valueMeta.isNull(value)) {
      // What ValueMetaBase does for a null string, kept identical.
      preparedStatement.setNull(index, Types.VARCHAR);
      return;
    }
    String string = valueMeta.getString(value);
    switch (columnType(preparedStatement, index)) {
      case Types.NCLOB ->
          preparedStatement.setNCharacterStream(
              index, new StringReader(string), (long) string.length());
      case Types.CLOB ->
          preparedStatement.setCharacterStream(
              index, new StringReader(string), (long) string.length());
      case Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR ->
          preparedStatement.setNString(index, string);
      default -> preparedStatement.setString(index, string);
    }
  }

  /** The JDBC type of the column behind one bind parameter, or {@link #UNKNOWN_COLUMN_TYPE}. */
  static int columnType(PreparedStatement preparedStatement, int index) {
    int[] types = COLUMN_TYPES.get(preparedStatement);
    if (types == null) {
      types = describeColumns(preparedStatement);
      COLUMN_TYPES.put(preparedStatement, types);
    }
    return index >= 1 && index <= types.length ? types[index - 1] : UNKNOWN_COLUMN_TYPE;
  }

  /**
   * Asks the statement for the column type of each of its parameters. Whatever the driver cannot
   * answer -- a driver too old to describe binds, a statement its parser does not understand, a
   * parameter it has no type for -- is recorded as unknown, so the question is asked once per
   * statement whatever the outcome.
   */
  private static int[] describeColumns(PreparedStatement preparedStatement) {
    try {
      ParameterMetaData metaData = preparedStatement.getParameterMetaData();
      int[] types = new int[metaData.getParameterCount()];
      for (int i = 0; i < types.length; i++) {
        types[i] = columnType(metaData, i + 1);
      }
      return types;
    } catch (SQLException | RuntimeException e) {
      return new int[0];
    }
  }

  private static int columnType(ParameterMetaData metaData, int parameter) {
    try {
      int type = metaData.getParameterType(parameter);
      // The driver reports the national types by their own JDBC codes, but a type name is the
      // safer of the two answers when both are there.
      String typeName = metaData.getParameterTypeName(parameter);
      if (typeName != null) {
        switch (typeName.toUpperCase(Locale.ROOT)) {
          case "NCLOB" -> type = Types.NCLOB;
          case "CLOB" -> type = Types.CLOB;
          case "NCHAR" -> type = Types.NCHAR;
          case "NVARCHAR2" -> type = Types.NVARCHAR;
          default -> {
            // Trust the code.
          }
        }
      }
      return type;
    } catch (SQLException | RuntimeException e) {
      return UNKNOWN_COLUMN_TYPE;
    }
  }
}
