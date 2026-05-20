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
import java.lang.reflect.Method;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.types.IValueBinding;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;

/**
 * How Oracle writes strings: the ORA-01461 batch workaround, and the national-character columns
 * (NVARCHAR2/NCHAR/NCLOB) that the driver will not accept through {@code setString}.
 *
 * <p>Write only. {@link #read} throws, so reading a string off an Oracle result set stays exactly
 * what it was before this binding existed -- see {@link
 * org.apache.hop.core.database.BaseDatabaseMeta#getValueFromResultSet}. Nothing was wrong with
 * reads.
 *
 * <p>Which column a value is bound to is not something the value's own metadata knows, so {@link
 * #enrichInsertRowMeta} reads it off the target table while the INSERT is being built. Without that
 * step every string here looks like a VARCHAR2.
 */
final class OraclePreparedStatementBinding implements IValueBinding {

  static final OraclePreparedStatementBinding INSTANCE = new OraclePreparedStatementBinding();

  private static final short ORACLE_JDBC_FORM_CHAR = 1;
  private static final short ORACLE_JDBC_FORM_NCHAR = 2;

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
    setPreparedStatementStringValue(
        preparedStatement, index, valueMeta.getString(value), valueMeta, database);
  }

  static void enrichInsertRowMeta(
      Database database, String schemaName, String tableName, IRowMeta insertRowMeta)
      throws HopDatabaseException {
    if (insertRowMeta == null || insertRowMeta.isEmpty()) {
      return;
    }

    IRowMeta tableFields = database.getTableFieldsMeta(schemaName, tableName);
    if (tableFields != null) {
      for (int i = 0; i < insertRowMeta.size(); i++) {
        IValueMeta insertMeta = insertRowMeta.getValueMeta(i);
        int idx = tableFields.indexOfValue(insertMeta.getName());
        if (idx < 0) {
          continue;
        }
        IValueMeta tableMeta = tableFields.getValueMeta(idx);
        insertMeta.setOriginalColumnType(tableMeta.getOriginalColumnType());
        insertMeta.setOriginalColumnTypeName(tableMeta.getOriginalColumnTypeName());
        if (insertMeta.isString() && tableMeta.getLength() > 0) {
          insertMeta.setLength(tableMeta.getLength());
        }
      }
    }

    try {
      enrichInsertRowMetaFromJdbcColumns(database, schemaName, tableName, insertRowMeta);
    } catch (SQLException e) {
      throw new HopDatabaseException(
          "Unable to read JDBC column metadata for Oracle insert binding hints", e);
    }
  }

  /** The column metadata of one table, as the JDBC driver reports it. */
  private record JdbcColumns(
      Map<String, Integer> sqlTypes, Map<String, String> typeNames, Map<String, Integer> sizes) {
    boolean isEmpty() {
      return sqlTypes.isEmpty();
    }
  }

  /**
   * Reads the columns of one table.
   *
   * <p>Asking without a schema matches the table name in every schema the user can see, and the
   * columns come back with nothing but their name to tell them apart, so a table of the same name
   * in another schema would silently supply the types. The schema the insert itself resolves
   * against is asked first for that reason. It is only when that finds nothing -- a synonym, or a
   * grant from somewhere else -- that the wider question is asked, because a wrong answer is still
   * better than treating a national column as a VARCHAR2, which is the bug this all exists to fix.
   */
  private static JdbcColumns readJdbcColumns(Database database, String schemaName, String tableName)
      throws SQLException {
    DatabaseMetaData dbmd = database.getConnection().getMetaData();
    String tablePattern = tableName.trim().toUpperCase(Locale.ROOT);

    String schemaPattern =
        Utils.isEmpty(schemaName)
            ? currentSchema(database)
            : schemaName.trim().toUpperCase(Locale.ROOT);

    JdbcColumns columns = readJdbcColumns(dbmd, schemaPattern, tablePattern);
    if (columns.isEmpty() && schemaPattern != null) {
      columns = readJdbcColumns(dbmd, null, tablePattern);
    }
    return columns;
  }

  private static JdbcColumns readJdbcColumns(
      DatabaseMetaData dbmd, String schemaPattern, String tablePattern) throws SQLException {
    Map<String, Integer> sqlTypes = new HashMap<>();
    Map<String, String> typeNames = new HashMap<>();
    Map<String, Integer> columnSizes = new HashMap<>();
    try (ResultSet rs = dbmd.getColumns(null, schemaPattern, tablePattern, null)) {
      while (rs.next()) {
        String col = rs.getString("COLUMN_NAME");
        if (col == null) {
          continue;
        }
        String key = col.toUpperCase(Locale.ROOT);
        sqlTypes.put(key, rs.getInt("DATA_TYPE"));
        typeNames.put(key, rs.getString("TYPE_NAME"));
        columnSizes.put(key, rs.getInt("COLUMN_SIZE"));
      }
    }
    return new JdbcColumns(sqlTypes, typeNames, columnSizes);
  }

  /** The schema an unqualified table name resolves to, or null when the driver will not say. */
  private static String currentSchema(Database database) {
    try {
      String schema = database.getConnection().getSchema();
      return Utils.isEmpty(schema) ? null : schema.trim().toUpperCase(Locale.ROOT);
    } catch (SQLException | AbstractMethodError e) {
      // getSchema arrived in JDBC 4.1 and an older driver may not carry it. Without it the wider
      // question below is the only one that can be asked.
      return null;
    }
  }

  private static void enrichInsertRowMetaFromJdbcColumns(
      Database database, String schemaName, String tableName, IRowMeta insertRowMeta)
      throws SQLException {
    JdbcColumns columns = readJdbcColumns(database, schemaName, tableName);
    Map<String, Integer> sqlTypes = columns.sqlTypes();
    Map<String, String> typeNames = columns.typeNames();
    Map<String, Integer> columnSizes = columns.sizes();

    for (int i = 0; i < insertRowMeta.size(); i++) {
      IValueMeta insertMeta = insertRowMeta.getValueMeta(i);
      if (!insertMeta.isString()) {
        continue;
      }
      String key = insertMeta.getName().toUpperCase(Locale.ROOT);
      Integer dt = sqlTypes.get(key);
      if (dt == null) {
        continue;
      }
      insertMeta.setOriginalColumnType(dt);
      String tn = typeNames.get(key);
      if (tn != null) {
        insertMeta.setOriginalColumnTypeName(tn);
      }
      Integer colSize = columnSizes.get(key);
      if (colSize != null && colSize > 0) {
        insertMeta.setLength(colSize);
      }
    }
  }

  static void setPreparedStatementStringValue(
      PreparedStatement preparedStatement,
      int index,
      String string,
      IValueMeta valueMeta,
      IDatabase database)
      throws SQLException {
    if (valueMeta.getLength() == DatabaseMeta.CLOB_LENGTH) {
      valueMeta.setLength(database.getMaxTextFieldLength());
    }

    if (isOracleClobColumn(valueMeta)) {
      if (isOracleNationalCharacterColumn(valueMeta)) {
        preparedStatement.setNCharacterStream(
            index, new StringReader(string), (long) string.length());
      } else {
        preparedStatement.setCharacterStream(
            index, new StringReader(string), (long) string.length());
      }
      return;
    }

    /*
     * Oracle batch mode needs setFormOfUse(FORM_NCHAR|FORM_CHAR) before setNString/setString for
     * VARCHAR2/NVARCHAR2. Stream binds still trigger ORA-01461 when batch rows mix short and long
     * values.
     */
    applyOracleJdbcFormOfUse(preparedStatement, index, isOracleNationalCharacterColumn(valueMeta));
    if (isOracleNationalCharacterColumn(valueMeta)) {
      preparedStatement.setNString(index, string);
      return;
    }

    preparedStatement.setString(index, string);
  }

  /**
   * Whether the column being written is a LOB, which is decided by the column type and not by the
   * length of the value.
   *
   * <p>A string read out of a CLOB arrives carrying {@link DatabaseMeta#CLOB_LENGTH} whatever it is
   * being written to, so taking that as the answer would stream into a VARCHAR2 whenever the target
   * column could not be read -- and a stream is what raises ORA-01461 on a batch that mixes long
   * and short values, which is one of the things this class exists to avoid. When nothing is known
   * about the column there is no reason to believe it is a LOB, and writing it the way Hop wrote it
   * before any of this existed is the safer answer.
   */
  private static boolean isOracleClobColumn(IValueMeta valueMeta) {
    int columnType = valueMeta.getOriginalColumnType();
    return columnType == Types.CLOB || columnType == Types.NCLOB;
  }

  private static boolean isOracleNationalCharacterColumn(IValueMeta valueMeta) {
    String typeName = valueMeta.getOriginalColumnTypeName();
    if (typeName != null) {
      String upper = typeName.toUpperCase(Locale.ROOT);
      if (upper.contains("NVARCHAR")
          || "NCHAR".equals(upper)
          || upper.startsWith("NCHAR(")
          || "NCLOB".equals(upper)
          || upper.startsWith("NCLOB(")) {
        return true;
      }
    }
    int columnType = valueMeta.getOriginalColumnType();
    return columnType == Types.NCHAR
        || columnType == Types.NVARCHAR
        || columnType == Types.LONGNVARCHAR
        || columnType == Types.NCLOB;
  }

  /**
   * setFormOfUse is reached reflectively because the driver is not on the compile classpath, and it
   * is reached for every string this dialect writes. Looking the method up per value would make the
   * innermost loop of an Oracle insert do a class lookup and allocate a Method each time, so it is
   * resolved once. Holder-class initialisation is what makes that thread safe.
   *
   * <p>Resolved against this plugin's own classloader, which is the one the driver is loaded in
   * (the dialect declares classLoaderGroup "oracle-db"); the isInstance check below is still what
   * decides whether the statement in hand is really the driver's, so a stale or unrelated class can
   * only cause the call to be skipped, never misapplied.
   */
  private static final class SetFormOfUse {
    private static final Class<?> ORACLE_PS = resolveClass();
    private static final Method METHOD = resolveMethod();

    private static Class<?> resolveClass() {
      try {
        return Class.forName("oracle.jdbc.OraclePreparedStatement");
      } catch (ClassNotFoundException | LinkageError e) {
        return null;
      }
    }

    private static Method resolveMethod() {
      if (ORACLE_PS == null) {
        return null;
      }
      try {
        return ORACLE_PS.getMethod("setFormOfUse", int.class, short.class);
      } catch (NoSuchMethodException | LinkageError e) {
        // An older JDBC build without it.
        return null;
      }
    }

    private SetFormOfUse() {}
  }

  private static void applyOracleJdbcFormOfUse(
      PreparedStatement preparedStatement, int index, boolean national) throws SQLException {
    Class<?> oraclePsClass = SetFormOfUse.ORACLE_PS;
    Method setFormOfUse = SetFormOfUse.METHOD;
    if (oraclePsClass == null || setFormOfUse == null) {
      return;
    }
    Object oraclePs = preparedStatement;
    if (!oraclePsClass.isInstance(preparedStatement)) {
      if (preparedStatement.isWrapperFor(oraclePsClass)) {
        oraclePs = preparedStatement.unwrap(oraclePsClass);
      } else {
        return;
      }
    }
    short form = national ? ORACLE_JDBC_FORM_NCHAR : ORACLE_JDBC_FORM_CHAR;
    try {
      setFormOfUse.invoke(oraclePs, index, form);
    } catch (ReflectiveOperationException | ClassCastException ignored) {
      // Not the Oracle driver after all, or it refused the call.
    }
  }
}
