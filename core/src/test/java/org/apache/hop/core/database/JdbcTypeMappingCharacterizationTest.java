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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.types.DatabaseColumn;
import org.apache.hop.core.database.types.DatabaseTypeMapper;
import org.apache.hop.core.database.types.StandardJdbcTypeMapper;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Characterizes the three JDBC-to-Hop type mappers that Hop currently carries, side by side, so the
 * places where they disagree are enumerable instead of anecdotal.
 *
 * <ul>
 *   <li><b>DB</b> — {@code Database.getDataTypeFromKnownSqlType}, reached through the public {@code
 *       getQueryFieldsFromPreparedStatement}. This is the path production actually uses.
 *   <li><b>VMB</b> — {@code ValueMetaBase.getValueFromSqlType}, reached only when {@code
 *       HOP_DB_DDL_COMPATIBLE=true}.
 *   <li><b>PRV</b> — {@code ValueMetaBase.getMetadataPreview}, which has no production caller left.
 * </ul>
 *
 * <p>Dialects are represented as mocked capability profiles. Only the ones core can answer are
 * here: rules that belong to a single dialect now live in that dialect's plugin, and are checked
 * there against the real dialect. MySQL remains because Generic, Hive and SingleStore claim to be
 * MySQL-like without extending the MySQL dialect, so its rules are shared and resolved in core.
 *
 * <p>This started with 117 diverging cases across four root causes. All three now delegate to
 * {@code StandardJdbcTypeMapper}, so the only {@code DIVERGE} lines left are the ones no shared
 * implementation can remove: {@code DatabaseMetaData.getColumns()} does not report signedness, so
 * the PRV path cannot tell an unsigned BIGINT from a signed one. Regenerate with {@code
 * -Dhop.golden.update=true}.
 */
class JdbcTypeMappingCharacterizationTest {

  private static final String UPDATE_PROPERTY = "hop.golden.update";
  private static final String GOLDEN =
      "/org/apache/hop/core/database/JdbcTypeMappingCharacterizationTest.txt";

  private IVariables variables;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void mappersAgreeWhereTheGoldenSaysTheyDo() throws Exception {
    variables = new Variables();
    String actual = render();

    if (Boolean.getBoolean(UPDATE_PROPERTY)) {
      Path target = Paths.get("src", "test", "resources").resolve(GOLDEN.substring(1));
      Files.createDirectories(target.getParent());
      Files.writeString(target, actual, StandardCharsets.UTF_8);
      System.out.println("Wrote golden file: " + target.toAbsolutePath());
      return;
    }

    String expected;
    try (InputStream in = getClass().getResourceAsStream(GOLDEN)) {
      expected = in == null ? null : new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    if (expected == null) {
      fail(
          "No golden at "
              + GOLDEN
              + ". Generate with -D"
              + UPDATE_PROPERTY
              + "=true.\n\n"
              + actual);
    }
    assertEquals(
        expected.trim(),
        actual.trim(),
        "JDBC type mapping changed. Regenerate with -D"
            + UPDATE_PROPERTY
            + "=true and justify"
            + " every differing line; each one is a behaviour change for some database.");
  }

  private String render() throws Exception {
    StringBuilder out = new StringBuilder();
    out.append("# Side-by-side characterization of Hop's three JDBC -> Hop type mappers.\n");
    out.append("# DB  = Database.getDataTypeFromKnownSqlType   (the live production path)\n");
    out.append(
        "# VMB = ValueMetaBase.getValueFromSqlType      (only under HOP_DB_DDL_COMPATIBLE)\n");
    out.append("# PRV = ValueMetaBase.getMetadataPreview       (no production caller)\n");
    out.append(
        "# NEW = StandardJdbcTypeMapper                 (the replacement, not yet wired in)\n");
    out.append(
        "# NEW!=DB would mean the replacement has drifted from the live path. There are none.\n");
    out.append(
        "# All three now delegate to StandardJdbcTypeMapper, so DIVERGE lines are no longer\n");
    out.append(
        "# implementation differences. What remains is inherent: DatabaseMetaData.getColumns()\n");
    out.append(
        "# reports no signedness, so the PRV path cannot see an explicitly unsigned column.\n");

    int diverging = 0;
    for (Profile profile : profiles()) {
      out.append("\n[").append(profile.name).append("]\n");
      for (Column column : columns()) {
        String db = describe(() -> viaDatabase(profile, column));
        String vmb = describe(() -> viaValueMetaBase(profile, column));
        String prv = describe(() -> viaMetadataPreview(profile, column));
        String neu = describe(() -> viaNewMapper(profile, column));
        boolean diverge = !(db.equals(vmb) && vmb.equals(prv));
        if (diverge) {
          diverging++;
        }
        out.append(
            String.format(
                "%-14s p=%4d s=%4d %-7s | DB=%-18s VMB=%-18s PRV=%-18s NEW=%-18s %s%s%n",
                column.typeName,
                column.precision,
                column.scale,
                column.signed ? "signed" : "unsigned",
                db,
                vmb,
                prv,
                neu,
                diverge ? "DIVERGE" : "",
                neu.equals(db) ? "" : " NEW!=DB"));
      }
    }
    out.append("\n# diverging cases: ").append(diverging).append('\n');
    return out.toString();
  }

  // ---------------------------------------------------------------- the three mappers

  private IValueMeta viaDatabase(Profile profile, Column column) throws Exception {
    DatabaseMeta meta = profile.databaseMeta();
    Database database = new Database(logging(), variables, meta);

    ResultSetMetaData rm = column.asResultSetMetaData();
    PreparedStatement ps = mock(PreparedStatement.class);
    when(ps.getMetaData()).thenReturn(rm);
    Connection connection = mock(Connection.class);
    when(connection.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(ps);
    database.setConnection(connection);

    IRowMeta rowMeta = database.getQueryFieldsFromPreparedStatement("select 1");
    return rowMeta == null || rowMeta.isEmpty() ? null : rowMeta.getValueMeta(0);
  }

  private IValueMeta viaValueMetaBase(Profile profile, Column column) throws Exception {
    return new ValueMetaString()
        .getValueFromSqlType(
            variables,
            profile.databaseMeta(),
            "COL",
            column.asResultSetMetaData(),
            1,
            false,
            false);
  }

  /** The replacement mapper, kept alongside the three until callers are migrated. */
  private IValueMeta viaNewMapper(Profile profile, Column column) throws Exception {
    DatabaseMeta meta = profile.databaseMeta();
    DatabaseColumn dc = DatabaseColumn.of(column.asResultSetMetaData(), 1, "COL");
    // Through the full resolution path: contributed rules, the dialect's own, the legacy variant
    // bridge, then the standard mapping.
    IValueMeta v = DatabaseTypeMapper.getValueMeta(variables, meta, dc, false, false);
    // Database falls back to the value meta plugin chain, which ends in a string, when the
    // standard rules decline the column; mirror that so the two columns compare like for like.
    return v == null ? StandardJdbcTypeMapper.getFallbackValueMeta(meta, dc, false, false) : v;
  }

  private IValueMeta viaMetadataPreview(Profile profile, Column column) throws Exception {
    return new ValueMetaString()
        .getMetadataPreview(variables, profile.databaseMeta(), column.asGetColumnsRow());
  }

  /** Renders a value meta as a stable "Type(length,precision)" string, or the failure it threw. */
  private String describe(ThrowingSupplier supplier) {
    try {
      IValueMeta v = supplier.get();
      if (v == null) {
        return "<null>";
      }
      return v.getTypeDesc() + "(" + v.getLength() + "," + v.getPrecision() + ")";
    } catch (Exception e) {
      Throwable root = e;
      while (root.getCause() != null) {
        root = root.getCause();
      }
      return "!" + root.getClass().getSimpleName();
    }
  }

  private interface ThrowingSupplier {
    IValueMeta get() throws Exception;
  }

  private ILoggingObject logging() {
    ILoggingObject log = mock(ILoggingObject.class);
    when(log.getLogLevel()).thenReturn(LogLevel.NOTHING);
    return log;
  }

  // ---------------------------------------------------------------- the matrix

  /** One synthetic column, expressible through either JDBC metadata API. */
  private static final class Column {
    private final String typeName;
    private final int sqlType;
    private final int precision;
    private final int scale;
    private final boolean signed;

    private Column(String typeName, int sqlType, int precision, int scale, boolean signed) {
      this.typeName = typeName;
      this.sqlType = sqlType;
      this.precision = precision;
      this.scale = scale;
      this.signed = signed;
    }

    private ResultSetMetaData asResultSetMetaData() throws Exception {
      ResultSetMetaData rm = mock(ResultSetMetaData.class);
      when(rm.getColumnCount()).thenReturn(1);
      when(rm.getColumnName(1)).thenReturn("COL");
      when(rm.getColumnLabel(1)).thenReturn("COL");
      when(rm.getColumnType(1)).thenReturn(sqlType);
      when(rm.getColumnTypeName(1)).thenReturn(typeName);
      when(rm.getPrecision(1)).thenReturn(precision);
      when(rm.getScale(1)).thenReturn(scale);
      when(rm.getColumnDisplaySize(1)).thenReturn(precision);
      when(rm.isSigned(1)).thenReturn(signed);
      return rm;
    }

    private ResultSet asGetColumnsRow() throws Exception {
      ResultSet rs = mock(ResultSet.class);
      when(rs.getString("COLUMN_NAME")).thenReturn("COL");
      when(rs.getInt("DATA_TYPE")).thenReturn(sqlType);
      when(rs.getObject("DECIMAL_DIGITS")).thenReturn(scale);
      when(rs.getInt("DECIMAL_DIGITS")).thenReturn(scale);
      when(rs.getInt("COLUMN_SIZE")).thenReturn(precision);
      when(rs.getString("TYPE_NAME")).thenReturn(typeName);
      when(rs.getString("REMARKS")).thenReturn("COL");
      return rs;
    }
  }

  private List<Column> columns() {
    int[][] sizes = {{0, 0}, {38, 0}, {10, 2}, {20, 10}, {16, 16}, {126, 0}};
    Object[][] sqlTypes = {
      {"CHAR", Types.CHAR},
      {"VARCHAR", Types.VARCHAR},
      {"LONGVARCHAR", Types.LONGVARCHAR},
      {"CLOB", Types.CLOB},
      {"BIGINT", Types.BIGINT},
      {"INTEGER", Types.INTEGER},
      {"SMALLINT", Types.SMALLINT},
      {"TINYINT", Types.TINYINT},
      {"DECIMAL", Types.DECIMAL},
      {"NUMERIC", Types.NUMERIC},
      {"DOUBLE", Types.DOUBLE},
      {"FLOAT", Types.FLOAT},
      {"REAL", Types.REAL},
      {"TIMESTAMP", Types.TIMESTAMP},
      {"DATE", Types.DATE},
      {"TIME", Types.TIME},
      {"BOOLEAN", Types.BOOLEAN},
      {"BIT", Types.BIT},
      {"BINARY", Types.BINARY},
      {"VARBINARY", Types.VARBINARY},
      {"LONGVARBINARY", Types.LONGVARBINARY},
      {"BLOB", Types.BLOB},
      {"OTHER", Types.OTHER},
      {"STRUCT", Types.STRUCT},
    };

    List<Column> list = new ArrayList<>();
    for (Object[] t : sqlTypes) {
      for (int[] size : sizes) {
        list.add(new Column((String) t[0], (Integer) t[1], size[0], size[1], true));
      }
    }
    // Unsigned BIGINT is handled differently by two of the three mappers.
    list.add(new Column("BIGINT", Types.BIGINT, 20, 0, false));
    // MySQL's YEAR special case keys off the column type name.
    list.add(new Column("YEAR", Types.DATE, 4, 0, true));
    return list;
  }

  /** A dialect reduced to the capability flags these mappers actually consult. */
  private static final class Profile {
    private final String name;
    private boolean postgres;
    private boolean mysql;
    private boolean oracle;
    private boolean strictBigNumber;
    private boolean sqlite;
    private boolean teradata;
    private boolean netezza;
    private boolean displaySizeTwiceThePrecision;
    private boolean supportsTimestamp = true;

    private Profile(String name) {
      this.name = name;
    }

    private DatabaseMeta databaseMeta() {
      IDatabase iDatabase = mock(IDatabase.class);
      when(iDatabase.isPostgresVariant()).thenReturn(postgres);
      when(iDatabase.isMySqlVariant()).thenReturn(mysql);
      when(iDatabase.isOracleVariant()).thenReturn(oracle);
      when(iDatabase.isStrictBigNumberInterpretation()).thenReturn(strictBigNumber);
      when(iDatabase.isSqliteVariant()).thenReturn(sqlite);
      when(iDatabase.isTeradataVariant()).thenReturn(teradata);
      when(iDatabase.isNetezzaVariant()).thenReturn(netezza);

      DatabaseMeta meta = mock(DatabaseMeta.class);
      when(meta.getIDatabase()).thenReturn(iDatabase);
      when(meta.getName()).thenReturn(name);
      when(meta.isMySqlVariant()).thenReturn(mysql);
      when(meta.supportsTimestampDataType()).thenReturn(supportsTimestamp);
      when(meta.isDisplaySizeTwiceThePrecision()).thenReturn(displaySizeTwiceThePrecision);
      when(meta.getConnectionProperties(org.mockito.ArgumentMatchers.any()))
          .thenReturn(new Properties());
      when(meta.stripCR(anyString())).thenReturn("select 1");
      return meta;
    }
  }

  private List<Profile> profiles() {
    List<Profile> list = new ArrayList<>();
    list.add(new Profile("PLAIN"));

    Profile mysql = new Profile("MYSQL");
    mysql.mysql = true;
    list.add(mysql);

    Profile netezza = new Profile("NETEZZA");
    netezza.netezza = true;
    list.add(netezza);

    Profile twice = new Profile("DISPLAY_SIZE_TWICE_PRECISION");
    twice.displaySizeTwiceThePrecision = true;
    list.add(twice);

    Profile noTimestamp = new Profile("NO_TIMESTAMP_SUPPORT");
    noTimestamp.supportsTimestamp = false;
    list.add(noTimestamp);

    return list;
  }
}
