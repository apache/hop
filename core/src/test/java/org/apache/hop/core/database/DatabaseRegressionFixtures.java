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
package org.apache.hop.core.database;

import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.database.types.DatabaseTypes;
import org.apache.hop.core.database.types.IDatabaseTypeRule;
import org.apache.hop.core.database.types.IValueBinding;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;

/**
 * Dialects, value bindings and JDBC fakes shared by the regression tests that pin how {@link
 * Database} writes values to prepared statements, reads them from result sets and derives lookup
 * layouts.
 *
 * <p>Every binding leaves a distinct mark (a prefix and a JDBC setter nobody else uses), so a test
 * can tell from the statement or the value which way a value went.
 */
final class DatabaseRegressionFixtures {

  private DatabaseRegressionFixtures() {
    // Fixtures only.
  }

  /** What {@link #STRING_BINDING} puts in front of every value it writes or reads. */
  static final String BOUND = "bound:";

  /** What {@link #INTEGER_BINDING} puts in front of every value it writes or reads. */
  static final String INT_BOUND = "int:";

  /**
   * Writes a string as setObject(OTHER), reads it with getObject. Both marked with {@link #BOUND}.
   */
  static final IValueBinding STRING_BINDING =
      new IValueBinding() {
        @Override
        public Object read(IDatabase database, IValueMeta valueMeta, ResultSet resultSet, int index)
            throws SQLException {
          Object value = resultSet.getObject(index);
          return value == null ? null : BOUND + value;
        }

        @Override
        public void write(
            IDatabase database,
            IValueMeta valueMeta,
            PreparedStatement preparedStatement,
            int index,
            Object value)
            throws SQLException {
          preparedStatement.setObject(index, value == null ? null : BOUND + value, Types.OTHER);
        }
      };

  /**
   * Writes an integer as setObject(BIGINT), reads it with getObject. Marked with {@link
   * #INT_BOUND}.
   */
  static final IValueBinding INTEGER_BINDING =
      new IValueBinding() {
        @Override
        public Object read(IDatabase database, IValueMeta valueMeta, ResultSet resultSet, int index)
            throws SQLException {
          Object value = resultSet.getObject(index);
          return value == null ? null : INT_BOUND + value;
        }

        @Override
        public void write(
            IDatabase database,
            IValueMeta valueMeta,
            PreparedStatement preparedStatement,
            int index,
            Object value)
            throws SQLException {
          preparedStatement.setObject(
              index, value == null ? null : INT_BOUND + value, Types.BIGINT);
        }
      };

  /** Declines writing, the way the SQLite numeric binding does. */
  static final IValueBinding READ_ONLY_BINDING =
      new IValueBinding() {
        @Override
        public Object read(IDatabase database, IValueMeta valueMeta, ResultSet resultSet, int index)
            throws SQLException {
          Object value = resultSet.getObject(index);
          return value == null ? null : "readonly:" + value;
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

  /** Declines reading, the way the Postgres JSON and Oracle string bindings do. */
  static final IValueBinding WRITE_ONLY_BINDING =
      new IValueBinding() {
        @Override
        public Object read(
            IDatabase database, IValueMeta valueMeta, ResultSet resultSet, int index) {
          throw new UnsupportedOperationException("This binding only writes values");
        }

        @Override
        public void write(
            IDatabase database,
            IValueMeta valueMeta,
            PreparedStatement preparedStatement,
            int index,
            Object value)
            throws SQLException {
          preparedStatement.setObject(index, "writeonly:" + value, Types.OTHER);
        }
      };

  static final String READ_REFUSED = "binding refused to read";
  static final String WRITE_REFUSED = "binding refused to write";

  /** A driver error in both directions. */
  static final IValueBinding FAILING_BINDING =
      new IValueBinding() {
        @Override
        public Object read(IDatabase database, IValueMeta valueMeta, ResultSet resultSet, int index)
            throws SQLException {
          throw new SQLException(READ_REFUSED);
        }

        @Override
        public void write(
            IDatabase database,
            IValueMeta valueMeta,
            PreparedStatement preparedStatement,
            int index,
            Object value)
            throws SQLException {
          throw new SQLException(WRITE_REFUSED);
        }
      };

  /**
   * Handles values but declines nulls in both directions by throwing UnsupportedOperationException,
   * leaving those to the value type. The {@link IValueBinding} contract does not say a decline is
   * permanent, so this is a legal binding even though no shipped binding declines per value.
   */
  static final IValueBinding DECLINES_NULLS_BINDING =
      new IValueBinding() {
        @Override
        public Object read(IDatabase database, IValueMeta valueMeta, ResultSet resultSet, int index)
            throws SQLException {
          Object value = resultSet.getObject(index);
          if (value == null) {
            throw new UnsupportedOperationException("Nulls are the value type's business");
          }
          return BOUND + value;
        }

        @Override
        public void write(
            IDatabase database,
            IValueMeta valueMeta,
            PreparedStatement preparedStatement,
            int index,
            Object value)
            throws SQLException {
          if (value == null) {
            throw new UnsupportedOperationException("Nulls are the value type's business");
          }
          preparedStatement.setObject(index, BOUND + value, Types.OTHER);
        }
      };

  @DatabaseMetaPlugin(type = "REGRESSION_PLAIN", typeDescription = "No bindings at all")
  static class PlainDialect extends NoneDatabaseMeta {}

  @DatabaseMetaPlugin(type = "REGRESSION_STRING_BINDING", typeDescription = "Binds strings")
  static class StringBindingDialect extends NoneDatabaseMeta {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules().bind(IValueMeta.TYPE_STRING, STRING_BINDING).build();
    }
  }

  @DatabaseMetaPlugin(type = "REGRESSION_INTEGER_BINDING", typeDescription = "Binds integers")
  static class IntegerBindingDialect extends NoneDatabaseMeta {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules().bind(IValueMeta.TYPE_INTEGER, INTEGER_BINDING).build();
    }
  }

  @DatabaseMetaPlugin(type = "REGRESSION_READ_ONLY", typeDescription = "Read only string binding")
  static class ReadOnlyBindingDialect extends NoneDatabaseMeta {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules().bind(IValueMeta.TYPE_STRING, READ_ONLY_BINDING).build();
    }
  }

  @DatabaseMetaPlugin(type = "REGRESSION_WRITE_ONLY", typeDescription = "Write only string binding")
  static class WriteOnlyBindingDialect extends NoneDatabaseMeta {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules().bind(IValueMeta.TYPE_STRING, WRITE_ONLY_BINDING).build();
    }
  }

  @DatabaseMetaPlugin(type = "REGRESSION_FAILING", typeDescription = "Failing string binding")
  static class FailingBindingDialect extends NoneDatabaseMeta {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules().bind(IValueMeta.TYPE_STRING, FAILING_BINDING).build();
    }
  }

  @DatabaseMetaPlugin(type = "REGRESSION_DECLINES_NULLS", typeDescription = "Declines nulls")
  static class DecliningBindingDialect extends NoneDatabaseMeta {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules().bind(IValueMeta.TYPE_STRING, DECLINES_NULLS_BINDING).build();
    }
  }

  /** Binds strings longer than 100 only, like the MySQL and CrateDB vector length condition. */
  @DatabaseMetaPlugin(type = "REGRESSION_LONG_STRING", typeDescription = "Binds long strings")
  static class LongStringBindingDialect extends NoneDatabaseMeta {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules()
          .bind(
              IValueMeta.TYPE_STRING,
              (database, valueMeta) -> valueMeta.getLength() > 100,
              STRING_BINDING)
          .build();
    }
  }

  /**
   * Binds strings read from an NVARCHAR column only, like the DuckDB time binding that looks at the
   * original column type. {@link ValueMetaBase#equals} ignores the original column type.
   */
  @DatabaseMetaPlugin(type = "REGRESSION_NATIONAL", typeDescription = "Binds national strings")
  static class NationalStringBindingDialect extends NoneDatabaseMeta {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules()
          .bind(
              IValueMeta.TYPE_STRING,
              (database, valueMeta) -> valueMeta.getOriginalColumnType() == Types.NVARCHAR,
              STRING_BINDING)
          .build();
    }
  }

  /**
   * Binds strings while a setting of the dialect instance says so, like the Netezza date binding
   * that asks the dialect whether it converts timestamps to dates.
   */
  @DatabaseMetaPlugin(type = "REGRESSION_SWITCHABLE", typeDescription = "Switchable binding")
  static class SwitchableBindingDialect extends NoneDatabaseMeta {
    boolean bindStrings;

    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules()
          .bind(
              IValueMeta.TYPE_STRING,
              (database, valueMeta) -> ((SwitchableBindingDialect) database).bindStrings,
              STRING_BINDING)
          .build();
    }
  }

  /**
   * Reads every value itself, wrapping what the default reading gives. It also declares a string
   * binding, so a reader that skipped this override would still produce bound values, without the
   * dialect's mark.
   */
  @DatabaseMetaPlugin(type = "REGRESSION_OVERRIDING_READ", typeDescription = "Overrides reading")
  static class OverridingReadDialect extends NoneDatabaseMeta {
    final List<String> asked = new ArrayList<>();

    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules().bind(IValueMeta.TYPE_STRING, STRING_BINDING).build();
    }

    @Override
    public Object getValueFromResultSet(ResultSet rs, IValueMeta val, int i)
        throws HopDatabaseException {
      asked.add(val.getName() + "@" + i);
      return "dialect:" + super.getValueFromResultSet(rs, val, i);
    }
  }

  /** Inherits the override rather than declaring it. */
  @DatabaseMetaPlugin(type = "REGRESSION_INHERITED_READ", typeDescription = "Inherits override")
  static class InheritedOverrideDialect extends OverridingReadDialect {}

  /**
   * A value type whose type can be changed after it is created. {@link ValueMetaBase} keeps its
   * type in a protected field, so a subclass can do this; nothing in Hop itself does.
   */
  static class RetypeableValueMeta extends ValueMetaBase {
    RetypeableValueMeta(String name, int type) {
      super(name, type);
    }

    void retype(int newType) {
      this.type = newType;
    }
  }

  /** A connection-less database on a real {@link DatabaseMeta} using the given dialect. */
  static Database database(IDatabase dialect) {
    return database(databaseMeta(dialect));
  }

  static DatabaseMeta databaseMeta(IDatabase dialect) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(dialect);
    return databaseMeta;
  }

  static Database database(DatabaseMeta databaseMeta) {
    ILoggingObject log = mock(ILoggingObject.class);
    when(log.getLogLevel()).thenReturn(LogLevel.NOTHING);
    return new Database(log, new Variables(), databaseMeta);
  }

  /** A row layout of the given Hop types, fields named after their position. */
  static IRowMeta layout(String prefix, int... types) {
    IRowMeta rowMeta = new RowMeta();
    for (int i = 0; i < types.length; i++) {
      rowMeta.addValueMeta(valueMeta(prefix + i, types[i]));
    }
    return rowMeta;
  }

  static IValueMeta valueMeta(String name, int type) {
    return switch (type) {
      case IValueMeta.TYPE_STRING -> new ValueMetaString(name);
      case IValueMeta.TYPE_INTEGER -> new ValueMetaInteger(name);
      case IValueMeta.TYPE_NUMBER -> new ValueMetaNumber(name);
      case IValueMeta.TYPE_BIGNUMBER -> new ValueMetaBigNumber(name);
      case IValueMeta.TYPE_BOOLEAN -> new ValueMetaBoolean(name);
      default -> throw new IllegalArgumentException("No test value type " + type);
    };
  }

  /** A value of the type for column {@code column}, different for every {@code round}. */
  static Object sampleValue(int type, int column, int round) {
    return switch (type) {
      case IValueMeta.TYPE_STRING -> "s" + column + "r" + round;
      case IValueMeta.TYPE_INTEGER -> (long) (column * 1000 + round);
      case IValueMeta.TYPE_NUMBER -> column + round / 8.0;
      case IValueMeta.TYPE_BIGNUMBER -> BigDecimal.valueOf(column * 1000L + round, 2);
      case IValueMeta.TYPE_BOOLEAN -> (column + round) % 2 == 0;
      default -> throw new IllegalArgumentException("No test value type " + type);
    };
  }

  static Object[] sampleRow(IRowMeta layout, int round) {
    Object[] row = new Object[layout.size()];
    for (int i = 0; i < layout.size(); i++) {
      row[i] = sampleValue(layout.getValueMeta(i).getType(), i, round);
    }
    return row;
  }

  /** A column of a fake result set. */
  record Column(String name, int sqlType, int precision, int scale) {
    static Column of(String name, int sqlType) {
      return new Column(name, sqlType, 0, 0);
    }
  }

  static ResultSetMetaData metaData(Column... columns) throws SQLException {
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(metaData.getColumnCount()).thenReturn(columns.length);
    for (int i = 0; i < columns.length; i++) {
      Column column = columns[i];
      int index = i + 1;
      when(metaData.getColumnName(index)).thenReturn(column.name());
      when(metaData.getColumnLabel(index)).thenReturn(column.name());
      when(metaData.getColumnType(index)).thenReturn(column.sqlType());
      when(metaData.getPrecision(index)).thenReturn(column.precision());
      when(metaData.getScale(index)).thenReturn(column.scale());
      when(metaData.isSigned(index)).thenReturn(true);
    }
    return metaData;
  }

  /**
   * A forward-only result set over the given rows. Every getter taking a column index answers the
   * value stored for that column of the current row, whatever getter it is, and {@code wasNull()}
   * reports whether the last value read was null. A primitive getter on a null answers the JDBC
   * default. Values are handed back as stored, so a test stores them as the getter returns them (a
   * {@link Timestamp} for a date, a {@link Long} for an integer).
   */
  static ResultSet resultSet(ResultSetMetaData metaData, Object[]... rows) {
    int[] cursor = {-1};
    boolean[] lastWasNull = {false};
    return mock(
        ResultSet.class,
        invocation -> {
          String method = invocation.getMethod().getName();
          Object[] arguments = invocation.getArguments();
          switch (method) {
            case "next":
              cursor[0]++;
              return cursor[0] < rows.length;
            case "getMetaData":
              return metaData;
            case "wasNull":
              return lastWasNull[0];
            default:
              break;
          }
          if (method.startsWith("get")
              && arguments.length >= 1
              && arguments[0] instanceof Integer) {
            int column = (Integer) arguments[0];
            Object value = rows[cursor[0]][column - 1];
            lastWasNull[0] = value == null;
            if (value == null) {
              return RETURNS_DEFAULTS.answer(invocation);
            }
            // Like a driver: a character column can be fetched as bytes and the other way round.
            Class<?> returnType = invocation.getMethod().getReturnType();
            if (returnType == byte[].class && value instanceof String string) {
              return string.getBytes(StandardCharsets.UTF_8);
            }
            if (returnType == String.class && value instanceof byte[] bytes) {
              return new String(bytes, StandardCharsets.UTF_8);
            }
            return value;
          }
          return RETURNS_DEFAULTS.answer(invocation);
        });
  }

  /** A prepared statement that answers a fresh result set over the same rows on every execution. */
  static PreparedStatement statement(ResultSetMetaData metaData, Object[]... rows)
      throws SQLException {
    PreparedStatement statement = mock(PreparedStatement.class);
    when(statement.executeQuery()).thenAnswer(invocation -> resultSet(metaData, rows));
    return statement;
  }

  static Timestamp timestamp(long millis) {
    return new Timestamp(millis);
  }
}
