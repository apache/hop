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

package org.apache.hop.databases.mysql;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DriverDownload;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.batch.BatchFailureContract;
import org.apache.hop.core.database.dml.DmlOperation;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.core.database.types.ColumnContext;
import org.apache.hop.core.database.types.ColumnTypeRules;
import org.apache.hop.core.database.types.DatabaseTypes;
import org.apache.hop.core.database.types.IDatabaseTypeRule;
import org.apache.hop.core.database.types.IValueBinding;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/** Contains MySQL specific information through static final members */
@DatabaseMetaPlugin(
    type = "MYSQL",
    typeDescription = "MySQL",
    image = "mysql.svg",
    documentationUrl = "/database/databases/mysql.html",
    classLoaderGroup = "mysql-db")
@GuiPlugin(id = "GUI-MySQLDatabaseMeta")
public class MySqlDatabaseMeta extends BaseDatabaseMeta implements IDatabase {

  private static final String PROTOBUF_JAVA_VERSION = loadProtobufJavaVersion();

  /**
   * Everything except the vector rules, so that the dialects built on this one can take these
   * without the vector type. Doris has no vector type at all, and MariaDB's is its own; neither
   * inherits MySQL's by accident.
   */
  protected static final List<IDatabaseTypeRule> BASE_TYPE_RULES =
      DatabaseTypes.rules()
          // MySQL has a JSON type. It has no UUID and no address type, and both of those fall
          // back to text on their own.
          .write(IValueMeta.TYPE_JSON)
          .as("JSON")
          .include(ColumnTypeRules.MYSQL_COMPATIBLE)
          .build();

  /**
   * MySQL will not accept a vector as text. An INSERT of '[1,2,3]' into a VECTOR column fails with
   * "Value of type 'string, size: 7' cannot be converted to 'vector' type", so a VECTOR column Hop
   * generated would be a column Hop could not then write to. What the server does accept is the
   * storage format itself, one little endian float32 per dimension, which is what the binding
   * writes.
   *
   * <p>Both rules turn on the same condition, a known dimension, and that is what keeps them
   * consistent: MySQL has no spelling for a vector of unknown length, so a vector without a
   * dimension goes to a text column, and must still be written as text. The binding and the column
   * therefore always agree about which of the two it is.
   */
  private static final List<IDatabaseTypeRule> VECTOR_RULES =
      DatabaseTypes.rules()
          .write(IValueMeta.TYPE_VECTOR)
          .where(v -> v.getLength() > 0)
          .as(v -> "VECTOR(" + v.getLength() + ")")
          .bind(
              IValueMeta.TYPE_VECTOR,
              (database, valueMeta) -> valueMeta.getLength() > 0,
              new MySqlVectorBinding())
          .build();

  private static final List<IDatabaseTypeRule> TYPE_RULES =
      DatabaseTypes.rules().include(BASE_TYPE_RULES).include(VECTOR_RULES).build();

  @Override
  public List<IDatabaseTypeRule> getTypeRules() {
    return TYPE_RULES;
  }

  /** MySQL 9.0 is the first release with a VECTOR type. */
  private static final int FIRST_VERSION_WITH_VECTOR = 9;

  @Override
  public boolean isColumnTypeAvailable(String columnType) {
    if ("VECTOR".equals(columnType)) {
      return serverIsAtLeast(FIRST_VERSION_WITH_VECTOR);
    }
    return true;
  }

  /** Writes a vector in MySQL's own storage format: little endian float32, one per dimension. */
  private static final class MySqlVectorBinding implements IValueBinding {
    /** The width of a float32, in bytes. */
    private static final int FLOAT_BYTES = 4;

    @Override
    public Object read(IDatabase database, IValueMeta valueMeta, ResultSet resultSet, int index) {
      throw new UnsupportedOperationException("This binding only writes values");
    }

    @Override
    public void write(
        IDatabase database,
        IValueMeta valueMeta,
        PreparedStatement preparedStatement,
        int index,
        Object value)
        throws SQLException, HopValueException {
      // Through the value type rather than by casting: the row may still hold the vector as text
      // or as lazily converted bytes, and only the value type knows how to read its own storage.
      float[] vector = (float[]) valueMeta.convertData(valueMeta, value);
      if (vector == null) {
        preparedStatement.setNull(index, Types.VARBINARY);
        return;
      }
      ByteBuffer buffer =
          ByteBuffer.allocate(vector.length * FLOAT_BYTES).order(ByteOrder.LITTLE_ENDIAN);
      for (float element : vector) {
        buffer.putFloat(element);
      }
      preparedStatement.setBytes(index, buffer.array());
    }
  }

  private static final Class<?> PKG = MySqlDatabaseMeta.class;
  public static final String CONST_MYSQL_8 = "Mysql 8+";

  @GuiWidgetElement(
      id = "resultStreaming",
      order = "10",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n:org.apache.hop.ui.core.database:DatabaseDialog.label.MySQLStreamResults")
  private boolean resultStreaming;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = "mySqlDriverClass",
      order = "20",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      comboValuesMethod = "getDriverClassNames",
      label = "i18n::MySQLDatabaseMeta.label.MySQLDriverType")
  @HopMetadataProperty(key = "driverClassName")
  private String driverClassName = CONST_MYSQL_8;

  /**
   * Gets resultStreaming
   *
   * @return value of resultStreaming
   */
  public boolean isResultStreaming() {
    String streaming = getAttributeProperty(ATTRIBUTE_USE_RESULT_STREAMING);
    return "Y".equalsIgnoreCase(streaming);
  }

  /**
   * @param resultStreaming The resultStreaming to set
   */
  public void setResultStreaming(boolean resultStreaming) {
    getAttributes().put(ATTRIBUTE_USE_RESULT_STREAMING, resultStreaming ? "Y" : "N");
  }

  private static final int VARCHAR_LIMIT = 65_535;

  private static final Set<String> SHORT_MESSAGE_EXCEPTIONS =
      Sets.newHashSet("com.mysql.jdbc.PacketTooBigException", "com.mysql.jdbc.MysqlDataTruncation");

  @Override
  public int[] getAccessTypeList() {
    return new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE};
  }

  @Override
  public int getDefaultDatabasePort() {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
      return 3306;
    }
    return -1;
  }

  @Override
  public String getLimitClause(int nrRows) {
    return " LIMIT " + nrRows;
  }

  /**
   * Returns the minimal SQL to launch in order to determine the layout of the resultset for a given
   * database table
   *
   * @param tableName The name of the table to determine the layout for
   * @return The SQL to launch.
   */
  @Override
  public String getSqlQueryFields(String tableName) {
    return "SELECT * FROM " + tableName + " LIMIT 0";
  }

  @Override
  public String getSqlObjectDdl(String schemaName, String objectName) {
    return showCreateTableSql(schemaName, objectName);
  }

  @Override
  public String getSqlTableExists(String tableName) {
    return getSqlQueryFields(tableName);
  }

  @Override
  public String getSqlColumnExists(String columnname, String tableName) {
    return getSqlQueryColumnFields(columnname, tableName);
  }

  public String getSqlQueryColumnFields(String columnname, String tableName) {
    return "SELECT " + columnname + " FROM " + tableName + " LIMIT 0";
  }

  /**
   * @see IDatabase#getNotFoundTK(boolean)
   */
  @Override
  public int getNotFoundTK(boolean useAutoinc) {
    if (isSupportsAutoInc() && useAutoinc) {
      return 1;
    }
    return super.getNotFoundTK(useAutoinc);
  }

  @Override
  public String getDriverClass() {

    return switch (driverClassName) {
      case "Mysql" -> "org.gjt.mm.mysql.Driver";
      case CONST_MYSQL_8 -> "com.mysql.cj.jdbc.Driver";
      default -> "com.mysql.cj.jdbc.Driver";
    };
  }

  @Override
  public DriverDownload getDriverDownload() {
    // Only offer MySQL Connector/J for databases that actually use it (Doris, Infobright, ...).
    String driverClass = getDriverClass();
    if (driverClass == null || !driverClass.contains("mysql")) {
      return null;
    }
    return DriverDownload.builder()
        .mavenCoordinate("com.mysql:mysql-connector-j")
        .defaultVersion("9.7.0")
        .licenseCategory("X")
        .licenseName("GPLv2 with Universal FOSS Exception")
        .licenseUrl("https://github.com/mysql/mysql-connector-j/blob/release/9.x/LICENSE")
        .vendor("Oracle / MySQL")
        .vendorUrl("https://dev.mysql.com/downloads/connector/j/")
        // Connector/J needs protobuf-java for the X DevAPI. Download it at the version the rest of
        // Hop uses, so it is the same jar another plugin may ship in lib/core.
        .managedVersions(
            PROTOBUF_JAVA_VERSION == null
                ? List.of()
                : List.of("com.google.protobuf:protobuf-java:" + PROTOBUF_JAVA_VERSION))
        .build();
  }

  /**
   * The protobuf-java version Hop is built with, filled in by the build in
   * driver-download.properties. Null when the resource is missing or was not filtered, and the
   * driver download then uses the version Connector/J declares.
   */
  private static String loadProtobufJavaVersion() {
    try (InputStream in =
        MySqlDatabaseMeta.class.getResourceAsStream("driver-download.properties")) {
      if (in == null) {
        return null;
      }
      Properties properties = new Properties();
      properties.load(in);
      String version = properties.getProperty("protobuf-java.version");
      return version == null || version.isBlank() || version.contains("${") ? null : version.trim();
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public String getURL(String hostname, String port, String databaseName) {

    if (Utils.isEmpty(port)) {
      return "jdbc:mysql://" + hostname + "/" + databaseName;
    } else {
      return "jdbc:mysql://" + hostname + ":" + port + "/" + databaseName;
    }
  }

  /**
   * @return The extra option separator in database URL for this platform (usually this is semicolon
   *     ; )
   */
  @Override
  public String getExtraOptionSeparator() {
    return "&";
  }

  /**
   * @return This indicator separates the normal URL from the options
   */
  @Override
  public String getExtraOptionIndicator() {
    return "?";
  }

  /**
   * @return true if the database supports transactions.
   */
  @Override
  public boolean isSupportsTransactions() {
    return false;
  }

  /**
   * @return true if the database supports bitmap indexes
   */
  @Override
  public boolean isSupportsBitmapIndex() {
    return false;
  }

  /**
   * @return true if the database supports views
   */
  @Override
  public boolean isSupportsViews() {
    return true;
  }

  /**
   * @return true if the database supports synonyms
   */
  @Override
  public boolean isSupportsSynonyms() {
    return false;
  }

  /**
   * Generates the SQL statement to add a column to the specified table
   *
   * @param tableName The table to add
   * @param v The column defined as a value
   * @param tk the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk the name of the primary key field
   * @param semicolon whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to add a column to the specified table
   */
  @Override
  public String getAddColumnStatement(
      String tableName, IValueMeta v, String tk, boolean useAutoinc, String pk, boolean semicolon) {
    return "ALTER TABLE "
        + tableName
        + " ADD "
        + getColumnDefinition(v, tk, pk, useAutoinc, true, false, ColumnContext.Purpose.ADD_COLUMN);
  }

  /**
   * Generates the SQL statement to modify a column in the specified table
   *
   * @param tableName The table to add
   * @param v The column defined as a value
   * @param tk the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk the name of the primary key field
   * @param semicolon whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to modify a column in the specified table
   */
  @Override
  public String getModifyColumnStatement(
      String tableName, IValueMeta v, String tk, boolean useAutoinc, String pk, boolean semicolon) {
    return "ALTER TABLE "
        + tableName
        + " MODIFY "
        + getColumnDefinition(
            v, tk, pk, useAutoinc, true, false, ColumnContext.Purpose.MODIFY_COLUMN);
  }

  @Override
  public String getFieldDefinition(
      IValueMeta v, String tk, String pk, boolean useAutoinc, boolean addFieldName, boolean addCR) {
    String retval = "";

    String fieldname = v.getName();
    if (v.getLength() == DatabaseMeta.CLOB_LENGTH) {
      v.setLength(getMaxTextFieldLength());
    }
    int length = v.getLength();
    int precision = v.getPrecision();

    if (addFieldName) {
      retval += fieldname + " ";
    }

    int type = v.getType();
    switch (type) {
      case IValueMeta.TYPE_TIMESTAMP, IValueMeta.TYPE_DATE:
        retval += "DATETIME";
        break;
      case IValueMeta.TYPE_BOOLEAN:
        if (isSupportsBooleanDataType()) {
          retval += "BOOLEAN";
        } else {
          retval += "CHAR(1)";
        }
        break;

      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_BIGNUMBER:
        if (fieldname.equalsIgnoreCase(tk)
            || // Technical key
            fieldname.equalsIgnoreCase(pk) // Primary key
        ) {
          if (useAutoinc) {
            retval += "BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY";
          } else {
            retval += "BIGINT NOT NULL PRIMARY KEY";
          }
        } else {
          // Integer values...
          if (precision == 0) {
            if (length > 9) {
              if (length < 19) {
                // can hold signed values between -9223372036854775808 and 9223372036854775807
                // 18 significant digits
                retval += "BIGINT";
              } else {
                retval += "DECIMAL(" + length + ")";
              }
            } else {
              retval += "INT";
            }
          } else {
            // Floating point values...
            if (length > 15) {
              retval += "DECIMAL(" + length;
              if (precision > 0) {
                retval += ", " + precision;
              }
              retval += ")";
            } else {
              // A double-precision floating-point number is accurate to approximately 15 decimal
              // places.
              // http://mysql.mirrors-r-us.net/doc/refman/5.1/en/numeric-type-overview.html
              retval += "DOUBLE";
            }
          }
        }
        break;
      case IValueMeta.TYPE_STRING:
        if (length > 0) {
          if (length == 1) {
            retval += "CHAR(1)";
          } else if (length < 256) {
            retval += "VARCHAR(" + length + ")";
          } else if (length < 65536) {
            retval += "TEXT";
          } else if (length < 16777216) {
            retval += "MEDIUMTEXT";
          } else {
            retval += "LONGTEXT";
          }
        } else {
          retval += "TINYTEXT";
        }
        break;
      case IValueMeta.TYPE_BINARY:
        retval += "LONGBLOB";
        break;
      default:
        retval += " UNKNOWN";
        break;
    }

    if (addCR) {
      retval += Const.CR;
    }

    return retval;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.database.IDatabase#getReservedWords()
   */
  @Override
  public String[] getReservedWords() {
    return new String[] {
      "ADD",
      "ALL",
      "ALTER",
      "ANALYZE",
      "AND",
      "AS",
      "ASC",
      "ASENSITIVE",
      "BEFORE",
      "BETWEEN",
      "BIGINT",
      "BINARY",
      "BLOB",
      "BOTH",
      "BY",
      "CALL",
      "CASCADE",
      "CASE",
      "CHANGE",
      "CHAR",
      "CHARACTER",
      "CHECK",
      "COLLATE",
      "COLUMN",
      "CONDITION",
      "CONNECTION",
      "CONSTRAINT",
      "CONTINUE",
      "CONVERT",
      "CREATE",
      "CROSS",
      "CURRENT_DATE",
      "CURRENT_TIME",
      "CURRENT_TIMESTAMP",
      "CURRENT_USER",
      "CURSOR",
      "DATABASE",
      "DATABASES",
      "DAY_HOUR",
      "DAY_MICROSECOND",
      "DAY_MINUTE",
      "DAY_SECOND",
      "DEC",
      "DECIMAL",
      "DECLARE",
      "DEFAULT",
      "DELAYED",
      "DELETE",
      "DESC",
      "DESCRIBE",
      "DETERMINISTIC",
      "DISTINCT",
      "DISTINCTROW",
      "DIV",
      "DOUBLE",
      "DROP",
      "DUAL",
      "EACH",
      "ELSE",
      "ELSEIF",
      "ENCLOSED",
      "ESCAPED",
      "EXISTS",
      "EXIT",
      "EXPLAIN",
      "FALSE",
      "FETCH",
      "FLOAT",
      "FOR",
      "FORCE",
      "FOREIGN",
      "FROM",
      "FULLTEXT",
      "GOTO",
      "GRANT",
      "GROUP",
      "HAVING",
      "HIGH_PRIORITY",
      "HOUR_MICROSECOND",
      "HOUR_MINUTE",
      "HOUR_SECOND",
      "IF",
      "IGNORE",
      "IN",
      "INDEX",
      "INFILE",
      "INNER",
      "INOUT",
      "INSENSITIVE",
      "INSERT",
      "INT",
      "INTEGER",
      "INTERVAL",
      "INTO",
      "IS",
      "ITERATE",
      "JOIN",
      "KEY",
      "KEYS",
      "KILL",
      "LEADING",
      "LEAVE",
      "LEFT",
      "LIKE",
      "LIMIT",
      "LINES",
      "LOAD",
      "LOCALTIME",
      "LOCALTIMESTAMP",
      "LOCATE",
      "LOCK",
      "LONG",
      "LONGBLOB",
      "LONGTEXT",
      "LOOP",
      "LOW_PRIORITY",
      "MATCH",
      "MEDIUMBLOB",
      "MEDIUMINT",
      "MEDIUMTEXT",
      "MIDDLEINT",
      "MINUTE_MICROSECOND",
      "MINUTE_SECOND",
      "MOD",
      "MODIFIES",
      "NATURAL",
      "NOT",
      "NO_WRITE_TO_BINLOG",
      "NULL",
      "NUMERIC",
      "ON",
      "OPTIMIZE",
      "OPTION",
      "OPTIONALLY",
      "OR",
      "ORDER",
      "OUT",
      "OUTER",
      "OUTFILE",
      "POSITION",
      "PRECISION",
      "PRIMARY",
      "PROCEDURE",
      "PURGE",
      "READ",
      "READS",
      "REAL",
      "REFERENCES",
      "REGEXP",
      "RENAME",
      "REPEAT",
      "REPLACE",
      "REQUIRE",
      "RESTRICT",
      "RETURN",
      "REVOKE",
      "RIGHT",
      "RLIKE",
      "SCHEMA",
      "SCHEMAS",
      "SECOND_MICROSECOND",
      "SELECT",
      "SENSITIVE",
      "SEPARATOR",
      "SET",
      "SHOW",
      "SMALLINT",
      "SONAME",
      "SPATIAL",
      "SPECIFIC",
      "SQL",
      "SQLEXCEPTION",
      "SQLSTATE",
      "SQLWARNING",
      "SQL_BIG_RESULT",
      "SQL_CALC_FOUND_ROWS",
      "SQL_SMALL_RESULT",
      "SSL",
      "STARTING",
      "STRAIGHT_JOIN",
      "TABLE",
      "TERMINATED",
      "THEN",
      "TINYBLOB",
      "TINYINT",
      "TINYTEXT",
      "TO",
      "TRAILING",
      "TRIGGER",
      "TRUE",
      "UNDO",
      "UNION",
      "UNIQUE",
      "UNLOCK",
      "UNSIGNED",
      "UPDATE",
      "USAGE",
      "USE",
      "USING",
      "UTC_DATE",
      "UTC_TIME",
      "UTC_TIMESTAMP",
      "VALUES",
      "VARBINARY",
      "VARCHAR",
      "VARCHARACTER",
      "VARYING",
      "WHEN",
      "WHERE",
      "WHILE",
      "WITH",
      "WRITE",
      "XOR",
      "YEAR_MONTH",
      "ZEROFILL"
    };
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.database.IDatabase#getStartQuote()
   */
  @Override
  public String getStartQuote() {
    return "`";
  }

  /**
   * Simply add an underscore in the case of MySQL!
   *
   * @see IDatabase#getEndQuote()
   */
  @Override
  public String getEndQuote() {
    return "`";
  }

  /**
   * @param tableNames The names of the tables to lock
   * @return The SQL command to lock database tables for write purposes.
   */
  @Override
  public String getSqlLockTables(String[] tableNames) {
    String sql = "LOCK TABLES ";
    for (int i = 0; i < tableNames.length; i++) {
      if (i > 0) {
        sql += ", ";
      }
      sql += tableNames[i] + " WRITE";
    }
    sql += ";" + Const.CR;

    return sql;
  }

  /**
   * @param tableName The name of the table to unlock
   * @return The SQL command to unlock a database table.
   */
  @Override
  public String getSqlUnlockTables(String[] tableName) {
    return "UNLOCK TABLES"; // This unlocks all tables
  }

  /**
   * @return extra help text on the supported options on the selected database platform.
   */
  @Override
  public String getExtraOptionsHelpText() {
    return "http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-configuration-properties.html";
  }

  /**
   * @param tableName
   * @return true if the specified table is a system table
   */
  @Override
  public boolean isSystemTable(String tableName) {
    return (tableName.startsWith("sys") || tableName.equals("dtproperties"));
  }

  /**
   * Get the SQL to insert a new empty unknown record in a dimension.
   *
   * @param schemaTable the schema-table name to insert into
   * @param keyField The key field
   * @param versionField the version field
   * @return the SQL to insert the unknown record into the SCD.
   */
  @Override
  public String getSqlInsertAutoIncUnknownDimensionRow(
      String schemaTable, String keyField, String versionField) {
    return "insert into " + schemaTable + "(" + keyField + ", " + versionField + ") values (1, 1)";
  }

  /**
   * @param string
   * @return A string that is properly quoted for use in a SQL statement (insert, update, delete,
   *     etc)
   */
  @Override
  public String quoteSqlString(String string) {
    string = string.replace("'", "\\\\'");
    string = string.replace("\\n", "\\\\n");
    string = string.replace("\\r", "\\\\r");
    return "'" + string + "'";
  }

  /**
   * @return true if the database is a MySQL variant, like MySQL 5.1, InfiniDB, InfoBright, and so
   *     on.
   */
  @Override
  public boolean isMySqlVariant() {
    return true;
  }

  /** By default MySQL reads 'O\'Brien' as one literal (unless NO_BACKSLASH_ESCAPES is set). */
  @Override
  public boolean isBackslashEscapingStrings() {
    return true;
  }

  /** Returns a false as Oracle does not allow for the releasing of savepoints. */
  @Override
  public boolean isReleaseSavepoint() {
    return false;
  }

  @Override
  public boolean IsSupportsErrorHandlingOnBatchUpdates() {
    return true;
  }

  @Override
  public boolean isRequiringTransactionsOnQueries() {
    return false;
  }

  @Override
  public boolean isFullExceptionLog(Exception e) {
    Throwable cause = (e == null ? null : e.getCause());
    return !(cause != null && SHORT_MESSAGE_EXCEPTIONS.contains(cause.getClass().getName()));
  }

  @Override
  public void addDefaultOptions() {
    addExtraOption(getPluginId(), "defaultFetchSize", "500");
    addExtraOption(getPluginId(), "useCursorFetch", "true");
    addExtraOption(getPluginId(), "zeroDateTimeBehaviorValue", "CONVERT_TO_NULL");
    setSupportsTimestampDataType(true);
    setSupportsBooleanDataType(true);
  }

  @Override
  public int getMaxVARCHARLength() {
    return VARCHAR_LIMIT;
  }

  @Override
  public int getMaxTextFieldLength() {
    return Integer.MAX_VALUE;
  }

  /**
   * Returns the column name for a MySQL field checking if the driver major version is "greater
   * than" or "lower or equal" to 3.
   *
   * @param dbMetaData
   * @param rsMetaData
   * @param index
   * @return The column label if version is greater than 3 or the column name if version is lower or
   *     equal to 3.
   * @throws HopDatabaseException
   */
  @Override
  public String getLegacyColumnName(
      DatabaseMetaData dbMetaData, ResultSetMetaData rsMetaData, int index)
      throws HopDatabaseException {
    if (dbMetaData == null) {
      throw new HopDatabaseException(
          BaseMessages.getString(
              PKG, "MySQLDatabaseMeta.Exception.LegacyColumnNameNoDBMetaDataException"));
    }

    if (rsMetaData == null) {
      throw new HopDatabaseException(
          BaseMessages.getString(
              PKG, "MySQLDatabaseMeta.Exception.LegacyColumnNameNoRSMetaDataException"));
    }

    try {
      return dbMetaData.getDriverMajorVersion() > 3
          ? rsMetaData.getColumnLabel(index)
          : rsMetaData.getColumnName(index);
    } catch (Exception e) {
      throw new HopDatabaseException(
          String.format(
              "%s: %s",
              BaseMessages.getString(PKG, "MySQLDatabaseMeta.Exception.LegacyColumnNameException"),
              e.getMessage()),
          e);
    }
  }

  /**
   * Used to generate the list that is shown in the mySqlDriverClass GuiWidget
   *
   * @param log Logging object
   * @param metadataProvider If shared metadata is needed to get the values
   * @return The list of driver type names shown in the GUI
   */
  public List<String> getDriverClassNames(ILogChannel log, IHopMetadataProvider metadataProvider) {
    List<String> names = new ArrayList<>();
    names.add(CONST_MYSQL_8);
    names.add("Mysql");
    return names;
  }

  /**
   * Measured with MySQL Connector/J 9.7 against MySQL 8.4. With the driver's defaults it runs every
   * row of a failed batch and marks the failed ones; with {@code continueBatchOnError=false} it
   * stops at the first failed row. With {@code rewriteBatchedStatements=true} the counts don't say
   * which rows were kept, so no contract is declared then.
   *
   * <p>Only declared for this dialect itself with the Connector/J driver, not for the dialects that
   * extend it (MariaDB's driver behaves differently).
   */
  @Override
  public BatchFailureContract getBatchFailureContract() {
    if (getClass() != MySqlDatabaseMeta.class
        || !"com.mysql.cj.jdbc.Driver".equals(getDriverClass())) {
      return null;
    }
    String rewrite = getConnectionOption("rewriteBatchedStatements");
    if (rewrite != null && !"false".equalsIgnoreCase(rewrite.trim())) {
      return null;
    }
    String continueOnError = getConnectionOption("continueBatchOnError");
    if (continueOnError == null || "true".equalsIgnoreCase(continueOnError.trim())) {
      return BatchFailureContract.MARKS_FAILED_ROWS;
    }
    if ("false".equalsIgnoreCase(continueOnError.trim())) {
      return BatchFailureContract.KEEPS_PREFIX;
    }
    return null;
  }

  /**
   * MySQL upserts with {@code INSERT ... ON DUPLICATE KEY UPDATE}, which reacts to every unique
   * index of the table: it is only used when the key columns are the primary key or a unique index
   * and the table has no other. From 8.1 on (8.0.19 has it too, but the patch level is not checked)
   * the inserted row is named with a row alias, {@code AS new ... c = new.c}; older servers get the
   * {@code VALUES(c)} function, deprecated since 8.0.20. MySQL leaves a row whose values don't
   * change alone on its own, which is what "update only changed" asks for. Checked against MySQL
   * 8.4 and 5.7 with the Connector/J Hop downloads by {@code MySqlMergeIT}.
   *
   * <p>An INSERT carries several rows in one {@code VALUES} list. A statement takes at most 65535
   * parameters when the server prepares it ({@code useServerPrepStmts=true}). Checked against MySQL
   * 9 by {@code MySqlDmlIT}. In strict mode, the default since 5.7, a row that breaks a NOT NULL
   * constraint fails its statement of an InnoDB table; without strict mode, or into a table without
   * transactions, MySQL writes the column's implicit default into a row of a multi-row INSERT and
   * only warns, where a one-row INSERT fails, see {@link #getMultiRowDmlRefusal}. A statement has
   * to fit in {@code max_allowed_packet}, see {@link #getMaxDmlStatementBytes}.
   *
   * <p>Only declared for this dialect itself, not for the ones that extend it: MariaDB has no row
   * alias, and the others have not been run.
   */
  @Override
  public DmlSyntax getDmlSyntax() {
    if (getClass() != MySqlDatabaseMeta.class) {
      return null;
    }
    return DmlSyntax.builder()
        .upsert(
            DmlSyntax.Upsert.of(UpsertForm.ON_DUPLICATE_KEY)
                .withRowAlias(true)
                .withMinimumMajorVersion(8)
                .withMinimumMinorVersion(1))
        .upsert(DmlSyntax.Upsert.of(UpsertForm.ON_DUPLICATE_KEY))
        .insertForm(DmlSyntax.InsertForm.VALUES_LIST)
        .maxParameters(65535)
        .build();
  }

  /**
   * Several rows per INSERT only in strict SQL mode ({@code STRICT_TRANS_TABLES} or {@code
   * STRICT_ALL_TABLES} in the session's {@code sql_mode}), and only into a table of a transactional
   * storage engine (InnoDB, NDB). Without strict mode MySQL writes the column's implicit default
   * for a NULL in a NOT NULL column of a multi-row INSERT and only warns, where the same row in an
   * INSERT of its own fails: the row would be changed silently instead of rejected. A table without
   * transactions (MyISAM, MEMORY, ARCHIVE, ...) does the same in {@code STRICT_TRANS_TABLES} mode
   * for any row but the first of a statement, and keeps the rows before a bad one in {@code
   * STRICT_ALL_TABLES} mode. The engine is read with {@code SHOW CREATE TABLE}, which also finds a
   * temporary table. When the mode or the engine can't be read, one row per statement is written.
   */
  @Override
  public String getMultiRowDmlRefusal(Connection connection, DmlSpec spec) {
    if (connection == null || spec == null || spec.getOperation() != DmlOperation.INSERT) {
      return null;
    }
    String sqlMode;
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT @@SESSION.sql_mode")) {
      sqlMode = resultSet.next() ? resultSet.getString(1) : null;
    } catch (SQLException e) {
      return BaseMessages.getString(PKG, "MySQLDatabaseMeta.MultiRow.ModeUnknown", e.getMessage());
    }
    if (!isStrictSqlMode(sqlMode)) {
      return BaseMessages.getString(
          PKG, "MySQLDatabaseMeta.MultiRow.NotStrict", Const.NVL(sqlMode, ""));
    }
    String table = spec.getQuotedTable();
    String engine;
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW CREATE TABLE " + table)) {
      engine = resultSet.next() ? tableEngine(resultSet.getString(2)) : null;
    } catch (SQLException e) {
      return BaseMessages.getString(
          PKG, "MySQLDatabaseMeta.MultiRow.EngineUnknown", table, e.getMessage());
    }
    if (engine == null) {
      return BaseMessages.getString(
          PKG, "MySQLDatabaseMeta.MultiRow.EngineUnknown", table, "no ENGINE in its definition");
    }
    if (TRANSACTIONAL_ENGINES.contains(engine.toUpperCase(Locale.ROOT))) {
      return null;
    }
    return BaseMessages.getString(
        PKG, "MySQLDatabaseMeta.MultiRow.NotTransactional", table, engine);
  }

  /** The storage engines whose tables refuse a bad row of a multi-row INSERT as a whole. */
  private static final Set<String> TRANSACTIONAL_ENGINES = Set.of("INNODB", "NDBCLUSTER", "NDB");

  private static final java.util.regex.Pattern ENGINE_OPTION =
      java.util.regex.Pattern.compile("\\bENGINE\\s*=\\s*([A-Za-z0-9_]+)");

  /**
   * The storage engine of a table, from what {@code SHOW CREATE TABLE} reports: the {@code ENGINE}
   * option after the list of column definitions. Quoted names, comments and defaults are skipped,
   * so an "ENGINE=" inside one doesn't count.
   *
   * @param createTable the CREATE TABLE statement; a view's CREATE VIEW has no engine
   * @return the engine as MySQL names it, or null when the statement names none
   */
  static String tableEngine(String createTable) {
    if (createTable == null) {
      return null;
    }
    int depth = 0;
    char quote = 0;
    for (int i = 0; i < createTable.length(); i++) {
      char c = createTable.charAt(i);
      if (quote != 0) {
        if (c == '\\' && quote != '`') {
          i++;
        } else if (c == quote) {
          if (i + 1 < createTable.length() && createTable.charAt(i + 1) == quote) {
            i++;
          } else {
            quote = 0;
          }
        }
      } else if (c == '\'' || c == '"' || c == '`') {
        quote = c;
      } else if (c == '(') {
        depth++;
      } else if (c == ')' && --depth == 0) {
        java.util.regex.Matcher matcher =
            ENGINE_OPTION.matcher(createTable).region(i + 1, createTable.length());
        return matcher.find() ? matcher.group(1) : null;
      }
    }
    return null;
  }

  /**
   * The server's {@code max_allowed_packet}, or the connection's {@code maxAllowedPacket} when it
   * is lower: Connector/J refuses a statement larger than that before it sends it
   * (PacketTooBigException), with and without server-side prepared statements, where a batch of
   * one-row statements only needs each row to fit. 4 MB, the default of MySQL 5.7 and of many
   * hosted servers, against 100 rows of a few large values, would otherwise fail a load that one
   * row per statement writes. Checked by {@code MySqlDmlIT}. When it can't be read, the 4 MB is
   * assumed.
   */
  @Override
  public long getMaxDmlStatementBytes(Connection connection) {
    long limit = 0;
    if (connection != null) {
      try (Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery("SELECT @@SESSION.max_allowed_packet")) {
        limit = resultSet.next() ? resultSet.getLong(1) : 0;
      } catch (SQLException e) {
        limit = 0;
      }
    }
    if (limit <= 0) {
      limit = DEFAULT_MAX_ALLOWED_PACKET;
    }
    long client = byteSize(getConnectionOption("maxAllowedPacket"));
    return client > 0 && client < limit ? client : limit;
  }

  /** MySQL 5.7's default max_allowed_packet. */
  static final long DEFAULT_MAX_ALLOWED_PACKET = 4L * 1024 * 1024;

  /**
   * @param value a size as Connector/J reads it: a number of bytes, with an optional k, m or g
   * @return the bytes, or 0 when it is not set or not a size
   */
  static long byteSize(String value) {
    if (Utils.isEmpty(value)) {
      return 0;
    }
    String size = value.trim().toLowerCase(Locale.ROOT);
    long multiplier = 1;
    char unit = size.isEmpty() ? 0 : size.charAt(size.length() - 1);
    if (unit == 'k' || unit == 'm' || unit == 'g') {
      multiplier = unit == 'k' ? 1024L : unit == 'm' ? 1024L * 1024 : 1024L * 1024 * 1024;
      size = size.substring(0, size.length() - 1).trim();
    }
    try {
      return Math.max(0, Long.parseLong(size) * multiplier);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /**
   * @param sqlMode the value of {@code @@sql_mode}: a comma separated list of modes
   * @return true if it holds STRICT_TRANS_TABLES or STRICT_ALL_TABLES
   */
  static boolean isStrictSqlMode(String sqlMode) {
    if (sqlMode == null) {
      return false;
    }
    for (String mode : sqlMode.split(",")) {
      String name = mode.trim().toUpperCase(Locale.ROOT);
      if (name.equals("STRICT_TRANS_TABLES") || name.equals("STRICT_ALL_TABLES")) {
        return true;
      }
    }
    return false;
  }
}
