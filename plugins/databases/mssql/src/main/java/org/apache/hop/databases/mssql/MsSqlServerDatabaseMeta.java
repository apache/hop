/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.databases.mssql;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.HopMetadataProperty;

import java.sql.ResultSet;

/**
 * Contains MS SQL specific information through static final members
 *
 * @author Matt
 * @since 11-mrt-2005
 */
@DatabaseMetaPlugin(
        type = "MSSQL",
        typeDescription = "MS SQL Server"
)
@GuiPlugin(id = "GUI-MSSQLServerDatabaseMeta")
public class MsSqlServerDatabaseMeta extends BaseDatabaseMeta implements IDatabase {


    @GuiWidgetElement(
            id = "instanceName",
            order = "20",
            parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            type = GuiElementType.TEXT,
            i18nPackage = "org.apache.hop.ui.core.database",
            label = "DatabaseDialog.label.SQLServerInstance"
    )
    @HopMetadataProperty
    private String instanceName;

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    @GuiWidgetElement(
            id = "usingDoubleDigit",
            order = "22",
            parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            type = GuiElementType.CHECKBOX,
            i18nPackage = "org.apache.hop.ui.core.database",
            label = "DatabaseDialog.label.UseDoubleDecimalSeparator"
    )
    @HopMetadataProperty
    private boolean usingDoubleDigit;

    /**
     * Gets usingDoubleDigit
     *
     * @return value of usingDoubleDigit
     */
    public boolean isUsingDoubleDigit() {
        return usingDoubleDigit;
    }

    /**
     * @param usingDoubleDigit The usingDoubleDigit to set
     */
    public void setUsingDoubleDigit(boolean usingDoubleDigit) {
        this.usingDoubleDigit = usingDoubleDigit;
    }

    @Override
    public boolean supportsCatalogs() {
        return false;
    }

    @Override
    public int[] getAccessTypeList() {
        return new int[]{
                DatabaseMeta.TYPE_ACCESS_NATIVE};
    }

    @Override
    public int getDefaultDatabasePort() {
        if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
            return 1433;
        }
        return -1;
    }

    @Override
    public String getDriverClass() {
        return "net.sourceforge.jtds.jdbc.Driver";
    }


    /**
     * The URL format for jTDS is:
     * <p>
     * jdbc:jtds:<server_type>://<server>[:<port>][/<database>][;<property>=<value>[;...]]
     */
    @Override
    public String getURL(String hostname, String port, String databaseName) {
        StringBuilder sb = new StringBuilder("jdbc:jtds:sqlserver://");
        sb.append(hostname);

        if (!Utils.isEmpty(port) && Const.toInt(port, -1) > 0) {
            sb.append(':');
            sb.append(port);
        }

        if (!Utils.isEmpty(databaseName)) {
            sb.append('/');
            sb.append(databaseName);
        }

        // With jTDS you will have to use the instance name as a property.
        if (!Utils.isEmpty(this.instanceName)) {
            sb.append(";instance=");
            sb.append(instanceName);
        }

        return sb.toString();
    }

    @Override
    public String getSchemaTableCombination(String schemaName, String tablePart) {
        // Something special for MSSQL
        //
        if (isUsingDoubleDecimalAsSchemaTableSeparator()) {
            return schemaName + ".." + tablePart;
        } else {
            return schemaName + "." + tablePart;
        }
    }

    /**
     * @return true if the database supports bitmap indexes
     */
    @Override
    public boolean supportsBitmapIndex() {
        return false;
    }

    /**
     * @return true if the database supports synonyms
     */
    @Override
    public boolean supportsSynonyms() {
        return false;
    }

    @Override
    public String getSqlQueryFields(String tableName) {
        return "SELECT TOP 1 * FROM " + tableName;
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
        return "SELECT TOP 1 " + columnname + " FROM " + tableName;
    }

    /**
     * @param tableNames The names of the tables to lock
     * @return The SQL command to lock database tables for write purposes. null is returned in case locking is not
     * supported on the target database. null is the default value
     */
    @Override
    public String getSqlLockTables(String[] tableNames) {
        StringBuilder sql = new StringBuilder(128);
        for (int i = 0; i < tableNames.length; i++) {
            sql.append("SELECT top 0 * FROM ").append(tableNames[i]).append(" WITH (UPDLOCK, HOLDLOCK);").append(
                    Const.CR);
        }
        return sql.toString();
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
    public String getModifyColumnStatement(String tableName, IValueMeta v, String tk, boolean useAutoinc,
                                           String pk, boolean semicolon) {
        return "ALTER TABLE "
                + tableName + " ALTER COLUMN " + getFieldDefinition(v, tk, pk, useAutoinc, true, false);
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
        return "ALTER TABLE " + tableName + " DROP COLUMN " + v.getName() + Const.CR;
    }

    @Override
    public String getFieldDefinition(IValueMeta v, String tk, String pk, boolean useAutoinc,
                                     boolean addFieldname, boolean addCr) {
        String retval = "";

        String fieldname = v.getName();
        int length = v.getLength();
        int precision = v.getPrecision();

        if (addFieldname) {
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
                    retval += "BIT";
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
                        retval += "BIGINT PRIMARY KEY IDENTITY(0,1)";
                    } else {
                        retval += "BIGINT PRIMARY KEY";
                    }
                } else {
                    if (precision == 0) {
                        if (length > 18) {
                            retval += "DECIMAL(" + length + ",0)";
                        } else {
                            if (length > 9) {
                                retval += "BIGINT";
                            } else {
                                retval += "INT";
                            }
                        }
                    } else {
                        if (precision > 0 && length > 0) {
                            retval += "DECIMAL(" + length + "," + precision + ")";
                        } else {
                            retval += "FLOAT(53)";
                        }
                    }
                }
                break;
            case IValueMeta.TYPE_STRING:
                if (length < getMaxVARCHARLength()) {
                    // Maybe use some default DB String length in case length<=0
                    if (length > 0) {
                        retval += "VARCHAR(" + length + ")";
                    } else {
                        retval += "VARCHAR(100)";
                    }
                } else {
                    retval += "TEXT"; // Up to 2bilion characters.
                }
                break;
            case IValueMeta.TYPE_BINARY:
                retval += "VARBINARY(MAX)";
                break;
            default:
                retval += " UNKNOWN";
                break;
        }

        if (addCr) {
            retval += Const.CR;
        }

        return retval;
    }

    /**
     * @return The SQL on this database to get a list of stored procedures.
     */
    public String getSqlListOfProcedures() {
        return "select o.name "
                + "from sysobjects o, sysusers u "
                + "where  xtype in ( 'FN', 'P' ) and o.uid = u.uid "
                + "order by o.name";
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hop.core.database.IDatabase#getReservedWords()
     */
    @Override
    public String[] getReservedWords() {
        return new String[]{
                /*
                 * Transact-SQL Reference: Reserved Keywords Includes future keywords: could be reserved in future releases of SQL
                 * Server as new features are implemented. REMARK: When SET QUOTED_IDENTIFIER is ON (default), identifiers can be
                 * delimited by double quotation marks, and literals must be delimited by single quotation marks. When SET
                 * QUOTED_IDENTIFIER is OFF, identifiers cannot be quoted and must follow all Transact-SQL rules for identifiers.
                 */
                "ABSOLUTE", "ACTION", "ADD", "ADMIN", "AFTER", "AGGREGATE", "ALIAS", "ALL", "ALLOCATE", "ALTER", "AND",
                "ANY", "ARE", "ARRAY", "AS", "ASC", "ASSERTION", "AT", "AUTHORIZATION", "BACKUP", "BEFORE", "BEGIN",
                "BETWEEN", "BINARY", "BIT", "BLOB", "BOOLEAN", "BOTH", "BREADTH", "BREAK", "BROWSE", "BULK", "BY", "CALL",
                "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CHAR", "CHARACTER", "CHECK", "CHECKPOINT", "CLASS",
                "CLOB", "CLOSE", "CLUSTERED", "COALESCE", "COLLATE", "COLLATION", "COLUMN", "COMMIT", "COMPLETION",
                "COMPUTE", "CONNECT", "CONNECTION", "CONSTRAINT", "CONSTRAINTS", "CONSTRUCTOR", "CONTAINS",
                "CONTAINSTABLE", "CONTINUE", "CONVERT", "CORRESPONDING", "CREATE", "CROSS", "CUBE", "CURRENT",
                "CURRENT_DATE", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
                "CURSOR", "CYCLE", "DATA", "DATABASE", "DATE", "DAY", "DBCC", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE",
                "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DENY", "DEPTH", "DEREF", "DESC", "DESCRIBE", "DESCRIPTOR",
                "DESTROY", "DESTRUCTOR", "DETERMINISTIC", "DIAGNOSTICS", "DICTIONARY", "DISCONNECT", "DISK", "DISTINCT",
                "DISTRIBUTED", "DOMAIN", "DOUBLE", "DROP", "DUMMY", "DUMP", "DYNAMIC", "EACH", "ELSE", "END", "END-EXEC",
                "EQUALS", "ERRLVL", "ESCAPE", "EVERY", "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS", "EXIT",
                "EXTERNAL", "FALSE", "FETCH", "FILE", "FILLFACTOR", "FIRST", "FLOAT", "FOR", "FOREIGN", "FOUND", "FREE",
                "FREETEXT", "FREETEXTTABLE", "FROM", "FULL", "FUNCTION", "GENERAL", "GET", "GLOBAL", "GO", "GOTO",
                "GRANT", "GROUP", "GROUPING", "HAVING", "HOLDLOCK", "HOST", "HOUR", "IDENTITY", "IDENTITY_INSERT",
                "IDENTITYCOL", "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDICATOR", "INITIALIZE", "INITIALLY",
                "INNER", "INOUT", "INPUT", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ISOLATION",
                "ITERATE", "JOIN", "KEY", "KILL", "LANGUAGE", "LARGE", "LAST", "LATERAL", "LEADING", "LEFT", "LESS",
                "LEVEL", "LIKE", "LIMIT", "LINENO", "LOAD", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOCATOR", "MAP",
                "MATCH", "MINUTE", "MODIFIES", "MODIFY", "MODULE", "MONTH", "NAMES", "NATIONAL", "NATURAL", "NCHAR",
                "NCLOB", "NEW", "NEXT", "NO", "NOCHECK", "NONCLUSTERED", "NONE", "NOT", "NULL", "NULLIF", "NUMERIC",
                "OBJECT", "OF", "OFF", "OFFSETS", "OLD", "ON", "ONLY", "OPEN", "OPENDATASOURCE", "OPENQUERY",
                "OPENROWSET", "OPENXML", "OPERATION", "OPTION", "OR", "ORDER", "ORDINALITY", "OUT", "OUTER", "OUTPUT",
                "OVER", "PAD", "PARAMETER", "PARAMETERS", "PARTIAL", "PATH", "PERCENT", "PLAN", "POSTFIX", "PRECISION",
                "PREFIX", "PREORDER", "PREPARE", "PRESERVE", "PRIMARY", "PRINT", "PRIOR", "PRIVILEGES", "PROC",
                "PROCEDURE", "PUBLIC", "RAISERROR", "READ", "READS", "READTEXT", "REAL", "RECONFIGURE", "RECURSIVE",
                "REF", "REFERENCES", "REFERENCING", "RELATIVE", "REPLICATION", "RESTORE", "RESTRICT", "RESULT", "RETURN",
                "RETURNS", "REVOKE", "RIGHT", "ROLE", "ROLLBACK", "ROLLUP", "ROUTINE", "ROW", "ROWCOUNT", "ROWGUIDCOL",
                "ROWS", "RULE", "SAVE", "SAVEPOINT", "SCHEMA", "SCOPE", "SCROLL", "SEARCH", "SECOND", "SECTION", "SELECT",
                "SEQUENCE", "SESSION", "SESSION_USER", "SET", "SETS", "SETUSER", "SHUTDOWN", "SIZE", "SMALLINT", "SOME",
                "SPACE", "SPECIFIC", "SPECIFICTYPE", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "START", "STATE",
                "STATEMENT", "STATIC", "STATISTICS", "STRUCTURE", "SYSTEM_USER", "TABLE", "TEMPORARY", "TERMINATE",
                "TEXTSIZE", "THAN", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TOP",
                "TRAILING", "TRAN", "TRANSACTION", "TRANSLATION", "TREAT", "TRIGGER", "TRUE", "TRUNCATE", "TSEQUAL",
                "UNDER", "UNION", "UNIQUE", "UNKNOWN", "UNNEST", "UPDATE", "UPDATETEXT", "USAGE", "USE", "USER", "USING",
                "VALUE", "VALUES", "VARCHAR", "VARIABLE", "VARYING", "VIEW", "WAITFOR", "WHEN", "WHENEVER", "WHERE",
                "WHILE", "WITH", "WITHOUT", "WORK", "WRITE", "WRITETEXT", "YEAR", "ZONE"};
    }

    @Override
    public String getExtraOptionsHelpText() {
        return "http://jtds.sourceforge.net/faq.html#urlFormat";
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

        String schemaTable = database.getDatabaseMeta().getQuotedSchemaTableCombination(schemaName, tableName);

        boolean[] exists = new boolean[idxFields.length];
        for (int i = 0; i < exists.length; i++) {
            exists[i] = false;
        }

        try {
            //
            // Get the info from the data dictionary...
            //
            StringBuilder sql = new StringBuilder(128);
            sql.append("select i.name table_name, c.name column_name ");
            sql.append("from     sysindexes i, sysindexkeys k, syscolumns c ");
            sql.append("where    i.name = '" + schemaTable + "' ");
            sql.append("AND      i.id = k.id ");
            sql.append("AND      i.id = c.id ");
            sql.append("AND      k.colid = c.colid ");

            ResultSet res = null;
            try {
                res = database.openQuery(sql.toString());
                if (res != null) {
                    Object[] row = database.getRow(res);
                    while (row != null) {
                        String column = database.getReturnRowMeta().getString(row, "column_name", "");
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
    public String getSqlListOfSchemas() {
        return "select name from sys.schemas";
    }

    @Override
    public boolean supportsSchemas() {
        return true;
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
    public String getSqlInsertAutoIncUnknownDimensionRow(String schemaTable, String keyField, String versionField) {
        return "insert into " + schemaTable + "(" + versionField + ") values (1)";
    }

    @Override
    public String getSqlNextSequenceValue(String sequenceName) {
        return String.format("SELECT NEXT VALUE FOR %s", sequenceName);
    }

    @Override
    public String getSqlCurrentSequenceValue(String sequenceName) {
        return String.format("SELECT current_value FROM sys.sequences WHERE name = '%s'", sequenceName);
    }

    @Override
    public String getSqlSequenceExists(String sequenceName) {
        return String.format("SELECT 1 FROM sys.sequences WHERE name = '%s'", sequenceName);
    }

    @Override
    public boolean supportsSequences() {
        return true;
    }

    @Override
    public boolean supportsSequenceNoMaxValueOption() {
        return true;
    }

    @Override
    public String getSqlListOfSequences() {
        return "SELECT name FROM sys.sequences";
    }

    /**
     * @param string
     * @return A string that is properly quoted for use in an Oracle SQL statement (insert, update, delete, etc)
     */
    @Override
    public String quoteSqlString(String string) {
        string = string.replace("'", "''");
        string = string.replace("\\n", "'+char(13)+'");
        string = string.replace("\\r", "'+char(10)+'");
        return "'" + string + "'";
    }

    @Override
    public Long getNextBatchIdUsingLockTables(DatabaseMeta dbm, Database ldb, String schemaName, String tableName,
                                              String fieldName) throws HopDatabaseException {
        Long rtn = null;
        // Make sure we lock that table to avoid concurrency issues
        ldb.lockTables(new String[]{dbm.getQuotedSchemaTableCombination(schemaName, tableName),});
        try {
            rtn = ldb.getNextValue(null, schemaName, tableName, fieldName);
        } finally {
            ldb.unlockTables(new String[]{tableName,});
        }
        return rtn;
    }

    @Override
    public boolean useSafePoints() {
        return false;
    }

    @Override
    public boolean supportsErrorHandlingOnBatchUpdates() {
        return true;
    }

    @Override
    public int getMaxVARCHARLength() {
        return 8000;
    }

    @Override
    public boolean isMsSqlServerVariant() {
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
