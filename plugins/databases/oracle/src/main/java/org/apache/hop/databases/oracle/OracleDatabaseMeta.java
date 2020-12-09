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

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.SqlScriptParser;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;

import java.sql.ResultSet;

/**
 * Contains Oracle specific information through static final members
 *
 * @author Matt
 * @since 11-mrt-2005
 */
@DatabaseMetaPlugin(
        type = "ORACLE",
        typeDescription = "Oracle"
)
@GuiPlugin(id = "GUI-OracleDatabaseMeta")
public class OracleDatabaseMeta extends BaseDatabaseMeta implements IDatabase {

    private static final String STRICT_BIGNUMBER_INTERPRETATION = "STRICT_NUMBER_38_INTERPRETATION";

    @Override
    public int[] getAccessTypeList() {
        return new int[]{
                DatabaseMeta.TYPE_ACCESS_NATIVE};
    }

    @Override
    public int getDefaultDatabasePort() {
        if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
            return 1521;
        }
        return -1;
    }

    /**
     * @return Whether or not the database can use auto increment type of fields (pk)
     */
    @Override
    public boolean supportsAutoInc() {
        return false;
    }

    /**
     * @see IDatabase#getLimitClause(int)
     */
    @Override
    public String getLimitClause(int nrRows) {
        return " WHERE ROWNUM <= " + nrRows;
    }

    /**
     * Returns the minimal SQL to launch in order to determine the layout of the resultset for a given database table
     *
     * @param tableName The name of the table to determine the layout for
     * @return The SQL to launch.
     */
    @Override
    public String getSqlQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " WHERE 1=0";
    }

    @Override
    public String getSqlTableExists(String tableName) {
        return getSqlQueryFields(tableName);
    }

    @Override
    public String getSqlColumnExists(String columnName, String tableName) {
        return getSqlQueryColumnFields(columnName, tableName);
    }

    public String getSqlQueryColumnFields(String columnName, String tableName) {
        return "SELECT " + columnName + " FROM " + tableName + " WHERE 1=0";
    }

    @Override
    public String getDriverClass() {
        return "oracle.jdbc.driver.OracleDriver";
    }

    @Override
    public String getURL(String hostname, String port, String databaseName) throws HopDatabaseException {
        if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
            // the database name can be a SID (starting with :) or a Service (starting with /)
            // <host>:<port>/<service>
            // <host>:<port>:<SID>
            if (!Utils.isEmpty(databaseName) && (databaseName.startsWith("/") || databaseName.startsWith(":"))) {
                return "jdbc:oracle:thin:@" + hostname + ":" + port + databaseName;
            } else if (Utils.isEmpty(hostname) && (Utils.isEmpty(port) || port.equals("-1"))) {
                // -1 when file based stored                                                                                                    // connection
                // support RAC with a self defined URL in databaseName like
                // (DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = host1-vip)(PORT = 1521))(ADDRESS = (PROTOCOL = TCP)(HOST =
                // host2-vip)(PORT = 1521))(LOAD_BALANCE = yes)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME =
                // db-service)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 180)(DELAY = 5))))
                // or
                // (DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)
                // (HOST=PRIMARY_NODE_HOSTNAME)(PORT=1521))
                // (ADDRESS=(PROTOCOL=TCP)(HOST=SECONDARY_NODE_HOSTNAME)(PORT=1521)))
                // (CONNECT_DATA=(SERVICE_NAME=DATABASE_SERVICENAME)))
                // or
                // (DESCRIPTION=(FAILOVER=ON)(ADDRESS_LIST=(LOAD_BALANCE=ON)
                // (ADDRESS=(PROTOCOL=TCP)(HOST=xxxxx)(PORT=1526))
                // (ADDRESS=(PROTOCOL=TCP)(HOST=xxxx)(PORT=1526)))(CONNECT_DATA=(SERVICE_NAME=somesid)))
                return "jdbc:oracle:thin:@" + databaseName;
            } else {
                // by default we assume a SID
                return "jdbc:oracle:thin:@" + hostname + ":" + port + ":" + databaseName;
            }
        } else {
            // OCI
            // Let's see if we have an database name
            if (databaseName != null && databaseName.length() > 0) {
                // Has the user specified hostname & port number?
                if (hostname != null && hostname.length() > 0 && port != null && port.length() > 0) {
                    // User wants the full url
                    return "jdbc:oracle:oci:@(description=(address=(host=" + hostname + ")(protocol=tcp)(port=" + port
                            + "))(connect_data=(sid=" + databaseName + ")))";
                } else {
                    // User wants the shortcut url
                    return "jdbc:oracle:oci:@" + databaseName;
                }
            } else {
                throw new HopDatabaseException(
                        "Unable to construct a JDBC URL: at least the database name must be specified");
            }
        }
    }

    /**
     * Oracle doesn't support options in the URL, we need to put these in a Properties object at connection time...
     */
    @Override
    public boolean supportsOptionsInURL() {
        return false;
    }

    /**
     * @return true if the database supports sequences
     */
    @Override
    public boolean supportsSequences() {
        return true;
    }

    /**
     * Check if a sequence exists.
     *
     * @param sequenceName The sequence to check
     * @return The SQL to get the name of the sequence back from the databases data dictionary
     */
    @Override
    public String getSqlSequenceExists(String sequenceName) {
        int dotPos = sequenceName.indexOf('.');
        String sql = "";
        if (dotPos == -1) {
            // if schema is not specified try to get sequence which belongs to current user
            sql = "SELECT * FROM USER_SEQUENCES WHERE SEQUENCE_NAME = '" + sequenceName.toUpperCase() + "'";
        } else {
            String schemaName = sequenceName.substring(0, dotPos);
            String seqName = sequenceName.substring(dotPos + 1);
            sql =
                    "SELECT * FROM ALL_SEQUENCES WHERE SEQUENCE_NAME = '"
                            + seqName.toUpperCase() + "' AND SEQUENCE_OWNER = '" + schemaName.toUpperCase() + "'";
        }
        return sql;
    }

    /**
     * Get the current value of a database sequence
     *
     * @param sequenceName The sequence to check
     * @return The current value of a database sequence
     */
    @Override
    public String getSqlCurrentSequenceValue(String sequenceName) {
        return "SELECT " + sequenceName + ".currval FROM DUAL";
    }

    /**
     * Get the SQL to get the next value of a sequence. (Oracle only)
     *
     * @param sequenceName The sequence name
     * @return the SQL to get the next value of a sequence. (Oracle only)
     */
    @Override
    public String getSqlNextSequenceValue(String sequenceName) {
        return "SELECT " + sequenceName + ".nextval FROM DUAL";
    }

    @Override
    public boolean supportsSequenceNoMaxValueOption() {
        return true;
    }

    /**
     * @return true if we need to supply the schema-name to getTables in order to get a correct list of items.
     */
    @Override
    public boolean useSchemaNameForTableList() {
        return true;
    }

    /**
     * @return true if the database supports synonyms
     */
    @Override
    public boolean supportsSynonyms() {
        return true;
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
        return "ALTER TABLE "
                + tableName + " ADD ( " + getFieldDefinition(v, tk, pk, useAutoinc, true, false) + " ) ";
    }

    /**
     * Generates the SQL statement to drop a column from the specified table
     *
     * @param tableName  The table to add
     * @param v          The column defined as a value
     * @param tk         the name of the technical key field
     * @param useAutoinc whether or not this field uses auto increment
     * @param pk         the name of the primary key field
     * @param semicolon  whether or not to add a semi-colon behind the statement.
     * @return the SQL statement to drop a column from the specified table
     */
    @Override
    public String getDropColumnStatement(String tableName, IValueMeta v, String tk, boolean useAutoinc,
                                         String pk, boolean semicolon) {
        return "ALTER TABLE " + tableName + " DROP ( " + v.getName() + " ) " + Const.CR;
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
    public String getModifyColumnStatement(String tableName, IValueMeta v, String tk, boolean useAutoinc,
                                           String pk, boolean semicolon) {
        IValueMeta tmpColumn = v.clone();
        String tmpName = v.getName();
        boolean isQuoted = tmpName.startsWith("\"") && tmpName.endsWith("\"");
        if (isQuoted) {
            // remove the quotes first.
            //
            tmpName = tmpName.substring(1, tmpName.length() - 1);
        }

        int threeoh = tmpName.length() >= 30 ? 30 : tmpName.length();
        tmpName = tmpName.substring(0, threeoh);

        tmpName += "_KTL"; // should always be shorter than 35 positions

        // put the quotes back if needed.
        //
        if (isQuoted) {
            tmpName = "\"" + tmpName + "\"";
        }
        tmpColumn.setName(tmpName);

        // Read to go.
        //
        String sql = "";

        // Create a new tmp column
        sql += getAddColumnStatement(tableName, tmpColumn, tk, useAutoinc, pk, semicolon) + ";" + Const.CR;
        // copy the old data over to the tmp column
        sql += "UPDATE " + tableName + " SET " + tmpColumn.getName() + "=" + v.getName() + ";" + Const.CR;
        // drop the old column
        sql += getDropColumnStatement(tableName, v, tk, useAutoinc, pk, semicolon) + ";" + Const.CR;
        // create the wanted column
        sql += getAddColumnStatement(tableName, v, tk, useAutoinc, pk, semicolon) + ";" + Const.CR;
        // copy the data from the tmp column to the wanted column (again)
        // All this to avoid the rename clause as this is not supported on all Oracle versions
        sql += "UPDATE " + tableName + " SET " + v.getName() + "=" + tmpColumn.getName() + ";" + Const.CR;
        // drop the temp column
        sql += getDropColumnStatement(tableName, tmpColumn, tk, useAutoinc, pk, semicolon);

        return sql;
    }

    @Override
    public String getFieldDefinition( IValueMeta v, String tk, String pk, boolean useAutoinc,
                                      boolean addFieldName, boolean addCR) {
        StringBuilder retval = new StringBuilder(128);

        String fieldname = v.getName();
        int length = v.getLength();
        int precision = v.getPrecision();

        if ( addFieldName ) {
            retval.append(fieldname).append(' ');
        }

        int type = v.getType();
        switch (type) {
            case IValueMeta.TYPE_TIMESTAMP:
                if (supportsTimestampDataType()) {
                    retval.append("TIMESTAMP");
                } else {
                    retval.append("DATE");
                }
                break;
            case IValueMeta.TYPE_DATE:
                retval.append("DATE");
                break;
            case IValueMeta.TYPE_BOOLEAN:
                retval.append("CHAR(1)");
                break;
            case IValueMeta.TYPE_NUMBER:
            case IValueMeta.TYPE_BIGNUMBER:
                retval.append("NUMBER");
                if (length > 0) {
                    retval.append('(').append(length);
                    if (precision > 0) {
                        retval.append(", ").append(precision);
                    }
                    retval.append(')');
                }
                break;
            case IValueMeta.TYPE_INTEGER:
                retval.append("INTEGER");
                break;
            case IValueMeta.TYPE_STRING:
                if (length >= DatabaseMeta.CLOB_LENGTH) {
                    retval.append("CLOB");
                } else {
                    if (length == 1) {
                        retval.append("CHAR(1)");
                    } else if (length > 0 && length <= getMaxVARCHARLength()) {
                        retval.append("VARCHAR2(").append(length).append(')');
                    } else {
                        if (length <= 0) {
                            retval.append("VARCHAR2(2000)"); // We don't know, so we just use the maximum...
                        } else {
                            retval.append("CLOB");
                        }
                    }
                }
                break;
            case IValueMeta.TYPE_BINARY: // the BLOB can contain binary data.
                retval.append("BLOB");
                break;
            default:
                retval.append(" UNKNOWN");
                break;
        }

        if (addCR) {
            retval.append(Const.CR);
        }

        return retval.toString();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.ibridge.hop.core.database.IDatabase#getReservedWords()
     */
    @Override
    public String[] getReservedWords() {
        return new String[]{
                "ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "ARRAYLEN", "AS", "ASC", "AUDIT", "BETWEEN", "BY", "CHAR",
                "CHECK", "CLUSTER", "COLUMN", "COMMENT", "COMPRESS", "CONNECT", "CREATE", "CURRENT", "DATE", "DECIMAL",
                "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE", "EXCLUSIVE", "EXISTS", "FILE", "FLOAT", "FOR",
                "FROM", "GRANT", "GROUP", "HAVING", "IDENTIFIED", "IMMEDIATE", "IN", "INCREMENT", "INDEX", "INITIAL",
                "INSERT", "INTEGER", "INTERSECT", "INTO", "IS", "LEVEL", "LIKE", "LOCK", "LONG", "MAXEXTENTS", "MINUS",
                "MODE", "MODIFY", "NOAUDIT", "NOCOMPRESS", "NOT", "NOTFOUND", "NOWAIT", "NULL", "NUMBER", "OF", "OFFLINE",
                "ON", "ONLINE", "OPTION", "OR", "ORDER", "PCTFREE", "PRIOR", "PRIVILEGES", "PUBLIC", "RAW", "RENAME",
                "RESOURCE", "REVOKE", "ROW", "ROWID", "ROWLABEL", "ROWNUM", "ROWS", "SELECT", "SESSION", "SET", "SHARE",
                "SIZE", "SMALLINT", "SQLBUF", "START", "SUCCESSFUL", "SYNONYM", "SYSDATE", "TABLE", "THEN", "TO",
                "TRIGGER", "UID", "UNION", "UNIQUE", "UPDATE", "USER", "VALIDATE", "VALUES", "VARCHAR", "VARCHAR2",
                "VIEW", "WHENEVER", "WHERE", "WITH"};
    }

    /**
     * @return The SQL on this database to get a list of stored procedures.
     */
    @Override
    public String getSqlListOfProcedures() {
        return "SELECT DISTINCT DECODE(package_name, NULL, '', package_name||'.') || object_name "
                + "FROM user_arguments "
                + "ORDER BY 1";
    }

    @Override
    public String getSqlLockTables(String[] tableNames) {
        StringBuilder sql = new StringBuilder(128);
        for (int i = 0; i < tableNames.length; i++) {
            sql.append("LOCK TABLE ").append(tableNames[i]).append(" IN EXCLUSIVE MODE;").append(Const.CR);
        }
        return sql.toString();
    }

    @Override
    public String getSqlUnlockTables(String[] tableNames) {
        return null; // commit handles the unlocking!
    }

    /**
     * @return extra help text on the supported options on the selected database platform.
     */
    @Override
    public String getExtraOptionsHelpText() {
        return "http://download.oracle.com/docs/cd/B19306_01/java.102/b14355/urls.htm#i1006362";
    }

    /**
     * Verifies on the specified database connection if an index exists on the fields with the specified name.
     *
     * @param database   a connected database
     * @param schemaName
     * @param tableName
     * @param idxFields
     * @return true if the index exists, false if it doesn't.
     * @throws HopDatabaseException
     */
    @Override
    public boolean checkIndexExists(Database database, String schemaName, String tableName, String[] idxFields) throws HopDatabaseException {

        String schemaTable = database.getDatabaseMeta().getQuotedSchemaTableCombination(database, schemaName, tableName);

        boolean[] exists = new boolean[idxFields.length];
        for (int i = 0; i < exists.length; i++) {
            exists[i] = false;
        }

        try {
            //
            // Get the info from the data dictionary...
            //
            String sql = "SELECT * FROM USER_IND_COLUMNS WHERE TABLE_NAME = '" + schemaTable + "'";
            ResultSet res = null;
            try {
                res = database.openQuery(sql);
                if (res != null) {
                    Object[] row = database.getRow(res);
                    while (row != null) {
                        String column = database.getReturnRowMeta().getString(row, "COLUMN_NAME", "");
                        int idx = Const.indexOfString(column, idxFields);
                        if (idx >= 0) {
                            exists[idx] = true;
                        }

                        row = database.getRow(res);
                    }

                } else {
                    return false;
                }
            } finally {
                if (res != null) {
                    database.closeQuery(res);
                }
            }

            // See if all the fields are indexed...
            boolean all = true;
            for (int i = 0; i < exists.length && all; i++) {
                if (!exists[i]) {
                    all = false;
                }
            }

            return all;
        } catch (Exception e) {
            throw new HopDatabaseException("Unable to determine if indexes exists on table [" + schemaTable + "]", e);
        }
    }

    @Override
    public boolean requiresCreateTablePrimaryKeyAppend() {
        return true;
    }

    /**
     * Most databases allow you to retrieve result metadata by preparing a SELECT statement.
     *
     * @return true if the database supports retrieval of query metadata from a prepared statement. False if the query
     * needs to be executed first.
     */
    @Override
    public boolean supportsPreparedStatementMetadataRetrieval() {
        return false;
    }

    /**
     * @return The maximum number of columns in a database, <=0 means: no known limit
     */
    @Override
    public int getMaxColumnsInIndex() {
        return 32;
    }

    /**
     * @return The SQL on this database to get a list of sequences.
     */
    @Override
    public String getSqlListOfSequences() {
        return "SELECT SEQUENCE_NAME FROM all_sequences";
    }

    /**
     * @param string
     * @return A string that is properly quoted for use in an Oracle SQL statement (insert, update, delete, etc)
     */
    @Override
    public String quoteSqlString(String string) {
        string = string.replaceAll("'", "''");
        string = string.replaceAll("\\n", "'||chr(13)||'");
        string = string.replaceAll("\\r", "'||chr(10)||'");
        return "'" + string + "'";
    }

    /**
     * Returns a false as Oracle does not allow for the releasing of savepoints.
     */
    @Override
    public boolean releaseSavepoint() {
        return false;
    }

    /**
     * Returns an empty string as most databases do not support tablespaces. Subclasses can override this method to
     * generate the DDL.
     *
     * @param variables    variables needed for variable substitution.
     * @param databaseMeta databaseMeta needed for it's quoteField method. Since we are doing variable substitution we need to meta
     *                     so that we can act on the variable substitution first and then the creation of the entire string that will
     *                     be retuned.
     * @param tablespace   tablespaceName name of the tablespace.
     * @return String the TABLESPACE tablespaceName section of an Oracle CREATE DDL statement.
     */
    @Override
    public String getTablespaceDDL(IVariables variables, DatabaseMeta databaseMeta, String tablespace) {
        if (!Utils.isEmpty(tablespace)) {
            return "TABLESPACE " + databaseMeta.quoteField(variables.resolve(tablespace));
        } else {
            return "";
        }
    }

    @Override
    public boolean supportsErrorHandlingOnBatchUpdates() {
        return false;
    }

    @Override
    public int getMaxVARCHARLength() {
        return 2000;
    }

    /**
     * Oracle does not support a construct like 'drop table if exists',
     * which is apparently legal syntax in many other RDBMSs.
     * So we need to implement the same behavior and avoid throwing 'table does not exist' exception.
     *
     * @param tableName Name of the table to drop
     * @return 'drop table if exists'-like statement for Oracle
     */
    @Override
    public String getDropTableIfExistsStatement(String tableName) {
        return "BEGIN EXECUTE IMMEDIATE 'DROP TABLE " + tableName
                + "'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
    }

    @Override
    public SqlScriptParser createSqlScriptParser() {
        return new SqlScriptParser(false);
    }

    /**
     * @return true if using strict number(38) interpretation
     */
    @Override
    public boolean isStrictBigNumberInterpretation() {
        return "Y".equalsIgnoreCase(getAttributeProperty(STRICT_BIGNUMBER_INTERPRETATION, "N"));
    }

    /**
     * @param strictBigNumberInterpretation true if use strict number(38) interpretation
     */
    public void setStrictBigNumberInterpretation(boolean strictBigNumberInterpretation) {
        getAttributes().put(STRICT_BIGNUMBER_INTERPRETATION, strictBigNumberInterpretation ? "Y" : "N");
    }

    @Override
    public boolean isOracleVariant() {
        return true;
    }

    @Override
    public String getStartQuote() {
        return "";
    }

    @Override
    public String getEndQuote() {
        return "";
    }
}
