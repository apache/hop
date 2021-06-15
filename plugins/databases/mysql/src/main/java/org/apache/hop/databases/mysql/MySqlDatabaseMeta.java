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
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;

import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Contains MySQL specific information through static final members
 *
 * @author Matt
 * @since 11-mrt-2005
 */
@DatabaseMetaPlugin(
        type = "MYSQL",
        typeDescription = "MySQL"
)
@GuiPlugin(id = "GUI-MySQLDatabaseMeta")
public class MySqlDatabaseMeta extends BaseDatabaseMeta implements IDatabase {
    private static final Class<?> PKG = MySqlDatabaseMeta.class; // For Translator

    @GuiWidgetElement(
            id = "resultStreaming",
            order = "10",
            parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            type = GuiElementType.CHECKBOX,          
            label = "i18n:org.apache.hop.ui.core.database:DatabaseDialog.label.MySQLStreamResults"
    )
    private boolean resultStreaming;

    //TODO: add internationalization
    @GuiWidgetElement(
            id = "mySqlDriverClass",
            order = "20",
            parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            type = GuiElementType.COMBO,
            comboValuesMethod = "getDriverClassNames",
            label = "Select database type:"
    )
    @HopMetadataProperty(key = "driverClassName")
    private String driverClassName = "";

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

    private static final Set<String>
            SHORT_MESSAGE_EXCEPTIONS =
            Sets.newHashSet("com.mysql.jdbc.PacketTooBigException", "com.mysql.jdbc.MysqlDataTruncation");

    @Override
    public int[] getAccessTypeList() {
        return new int[]{DatabaseMeta.TYPE_ACCESS_NATIVE};
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
     * Returns the minimal SQL to launch in order to determine the layout of the resultset for a given database table
     *
     * @param tableName The name of the table to determine the layout for
     * @return The SQL to launch.
     */
    @Override
    public String getSqlQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " LIMIT 0";
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
        if (supportsAutoInc() && useAutoinc) {
            return 1;
        }
        return super.getNotFoundTK(useAutoinc);
    }

    @Override
    public String getDriverClass() {

        switch (driverClassName) {
            case "Mysql":
                return "org.gjt.mm.mysql.Driver";
            case "Mysql 8+":
                return "com.mysql.jdbc.Driver";
            default:
                return "org.gjt.mm.mysql.Driver";
        }
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
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
     * @return The extra option separator in database URL for this platform (usually this is semicolon ; )
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
    public boolean supportsTransactions() {
        return false;
    }

    /**
     * @return true if the database supports bitmap indexes
     */
    @Override
    public boolean supportsBitmapIndex() {
        return false;
    }

    /**
     * @return true if the database supports views
     */
    @Override
    public boolean supportsViews() {
        return true;
    }

    /**
     * @return true if the database supports synonyms
     */
    @Override
    public boolean supportsSynonyms() {
        return false;
    }

    /**
     * Generates the SQL statement to add a column to the specified table
     *
     * @param tableName  The table to add
     * @param v          The column defined as a value
     * @param tk         the name of the technical key field
     * @param useAutoinc whether or not this field uses auto increment
     * @param pk         the name of the primary key field
     * @param semicolon  whether or not to add a semi-colon behind the statement.
     * @return the SQL statement to add a column to the specified table
     */
    @Override
    public String getAddColumnStatement(String tableName, IValueMeta v, String tk, boolean useAutoinc,
                                        String pk, boolean semicolon) {
        return "ALTER TABLE " + tableName + " ADD " + getFieldDefinition(v, tk, pk, useAutoinc, true, false);
    }

    /**
     * Generates the SQL statement to modify a column in the specified table
     *
     * @param tableName  The table to add
     * @param v          The column defined as a value
     * @param tk         the name of the technical key field
     * @param useAutoinc whether or not this field uses auto increment
     * @param pk         the name of the primary key field
     * @param semicolon  whether or not to add a semi-colon behind the statement.
     * @return the SQL statement to modify a column in the specified table
     */
    @Override
    public String getModifyColumnStatement(String tableName, IValueMeta v, String tk,
                                           boolean useAutoinc, String pk, boolean semicolon) {
        return "ALTER TABLE " + tableName + " MODIFY " + getFieldDefinition(v, tk, pk, useAutoinc, true, false);
    }

    @Override
    public String getFieldDefinition( IValueMeta v, String tk, String pk, boolean useAutoinc,
                                      boolean addFieldName, boolean addCR) {
        String retval = "";

        String fieldname = v.getName();
        if (v.getLength() == DatabaseMeta.CLOB_LENGTH) {
            v.setLength(getMaxTextFieldLength());
        }
        int length = v.getLength();
        int precision = v.getPrecision();

        if ( addFieldName ) {
            retval += fieldname + " ";
        }

        int type = v.getType();
        switch (type) {
            case IValueMeta.TYPE_TIMESTAMP:
            case IValueMeta.TYPE_DATE:
                retval += "DATETIME";
                break;
            case IValueMeta.TYPE_BOOLEAN:
                if (supportsBooleanDataType()) {
                    retval += "BOOLEAN";
                } else {
                    retval += "CHAR(1)";
                }
                break;

            case IValueMeta.TYPE_NUMBER:
            case IValueMeta.TYPE_INTEGER:
            case IValueMeta.TYPE_BIGNUMBER:
                if (fieldname.equalsIgnoreCase(tk) || // Technical key
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
                            // A double-precision floating-point number is accurate to approximately 15 decimal places.
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
        return new String[]{"ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE", "BEFORE", "BETWEEN",
                "BIGINT", "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK",
                "COLLATE", "COLUMN", "CONDITION", "CONNECTION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS",
                "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DATABASES",
                "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED",
                "DELETE", "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL", "EACH",
                "ELSE", "ELSEIF", "ENCLOSED", "ESCAPED", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FLOAT", "FOR", "FORCE",
                "FOREIGN", "FROM", "FULLTEXT", "GOTO", "GRANT", "GROUP", "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND",
                "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE", "INSERT",
                "INT", "INTEGER", "INTERVAL", "INTO", "IS", "ITERATE", "JOIN", "KEY", "KEYS", "KILL", "LEADING", "LEAVE", "LEFT",
                "LIKE", "LIMIT", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCATE", "LOCK", "LONG", "LONGBLOB", "LONGTEXT",
                "LOOP", "LOW_PRIORITY", "MATCH", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND",
                "MINUTE_SECOND", "MOD", "MODIFIES", "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC", "ON", "OPTIMIZE",
                "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE", "POSITION", "PRECISION", "PRIMARY", "PROCEDURE",
                "PURGE", "READ", "READS", "REAL", "REFERENCES", "REGEXP", "RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESTRICT",
                "RETURN", "REVOKE", "RIGHT", "RLIKE", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT", "SENSITIVE",
                "SEPARATOR", "SET", "SHOW", "SMALLINT", "SONAME", "SPATIAL", "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE",
                "SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STRAIGHT_JOIN",
                "TABLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE", "UNDO",
                "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING", "UTC_DATE", "UTC_TIME",
                "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "WHEN", "WHERE", "WHILE", "WITH",
                "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL"};
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
     * @param schemaTable  the schema-table name to insert into
     * @param keyField     The key field
     * @param versionField the version field
     * @return the SQL to insert the unknown record into the SCD.
     */
    @Override
    public String getSqlInsertAutoIncUnknownDimensionRow(String schemaTable, String keyField,
                                                         String versionField) {
        return "insert into " + schemaTable + "(" + keyField + ", " + versionField + ") values (1, 1)";
    }

    /**
     * @param string
     * @return A string that is properly quoted for use in a SQL statement (insert, update, delete, etc)
     */
    @Override
    public String quoteSqlString(String string) {
        string = string.replace("'", "\\\\'");
        string = string.replace("\\n", "\\\\n");
        string = string.replace("\\r", "\\\\r");
        return "'" + string + "'";
    }

    /**
     * @return true if the database is a MySQL variant, like MySQL 5.1, InfiniDB, InfoBright, and so on.
     */
    @Override
    public boolean isMySqlVariant() {
        return true;
    }

    /**
     * Returns a false as Oracle does not allow for the releasing of savepoints.
     */
    @Override
    public boolean releaseSavepoint() {
        return false;
    }

    @Override
    public boolean supportsErrorHandlingOnBatchUpdates() {
        return true;
    }

    @Override
    public boolean isRequiringTransactionsOnQueries() {
        return false;
    }

    @Override
    public boolean fullExceptionLog(Exception e) {
        Throwable cause = (e == null ? null : e.getCause());
        return !(cause != null && SHORT_MESSAGE_EXCEPTIONS.contains(cause.getClass().getName()));
    }

    @Override
    public void addDefaultOptions() {
        addExtraOption(getPluginId(), "defaultFetchSize", "500");
        addExtraOption(getPluginId(), "useCursorFetch", "true");
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
     * Returns the column name for a MySQL field checking if the driver major version is "greater than" or "lower or equal" to 3.
     *
     * @param dbMetaData
     * @param rsMetaData
     * @param index
     * @return The column label if version is greater than 3 or the column name if version is lower or equal to 3.
     * @throws HopDatabaseException
     */
    @Override
    public String getLegacyColumnName(DatabaseMetaData dbMetaData, ResultSetMetaData rsMetaData, int index) throws HopDatabaseException {
        if (dbMetaData == null) {
            throw new HopDatabaseException(BaseMessages.getString(PKG, "MySQLDatabaseMeta.Exception.LegacyColumnNameNoDBMetaDataException"));
        }

        if (rsMetaData == null) {
            throw new HopDatabaseException(BaseMessages.getString(PKG, "MySQLDatabaseMeta.Exception.LegacyColumnNameNoRSMetaDataException"));
        }

        try {
            return dbMetaData.getDriverMajorVersion() > 3 ? rsMetaData.getColumnLabel(index) : rsMetaData.getColumnName(index);
        } catch (Exception e) {
            throw new HopDatabaseException(String.format("%s: %s", BaseMessages.getString(PKG, "MySQLDatabaseMeta.Exception.LegacyColumnNameException"), e.getMessage()), e);
        }
    }

    /**
     * Used to generate the list that is shown in the mySqlDriverClass GuiWidget
     *
     * @param log              Logging object
     * @param metadataProvider If shared metadata is needed to get the values
     * @return The list of driver type names shown in the GUI
     */
    public List<String> getDriverClassNames(ILogChannel log, IHopMetadataProvider metadataProvider) {
        List<String> names = new ArrayList<>();
        names.add("Mysql");
        names.add("Mysql 8+");
        return names;
    }

}
