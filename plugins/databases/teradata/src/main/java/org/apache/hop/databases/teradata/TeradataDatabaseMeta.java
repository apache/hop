/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.databases.teradata;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;

import java.util.Map;

/**
 * Contains NCR Teradata specific information through static final members
 *
 * @author Matt
 * @since 26-jul-2006
 */
@DatabaseMetaPlugin(
  type = "TERADATA",
  typeDescription = "Teradata"
)
@GuiPlugin( id = "GUI-TeradataDatabaseMeta" )
public class TeradataDatabaseMeta extends BaseDatabaseMeta implements IDatabase {
  @Override
  public int[] getAccessTypeList() {
    return new int[] {
      DatabaseMeta.TYPE_ACCESS_NATIVE };
  }


  @Override
  public boolean isTeradataVariant() {
    return true;
  }

  /**
   * @see IDatabase#getNotFoundTK(boolean)
   */
  @Override
  public int getNotFoundTK( boolean useAutoinc ) {
    if ( supportsAutoInc() && useAutoinc ) {
      return 1;
    }
    return super.getNotFoundTK( useAutoinc );
  }

  @Override
  public String getDriverClass() {
    return "com.teradata.jdbc.TeraDriver";
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) {
      String url = "jdbc:teradata://" + hostname;

      // port is not appended here; instead it is appended via the DBS_PORT extra option

      if ( !StringUtils.isEmpty( databaseName ) ) {
        url += "/DATABASE=" + databaseName;
      }
      return url;

  }

  /**
   * Checks whether or not the command setFetchSize() is supported by the JDBC driver...
   *
   * @return true is setFetchSize() is supported!
   */
  @Override
  public boolean isFetchSizeSupported() {
    return false;
  }

  /**
   * @return true if the database supports bitmap indexes
   */
  @Override
  public boolean supportsBitmapIndex() {
    return false;
  }

  @Override
  public String getSqlTableExists( String tableName ) {
    return "show table " + tableName;
  }

  @Override
  public String getSqlColumnExists( String columnname, String tableName ) {
    return "SELECT * FROM DBC.columns WHERE tablename =" + tableName + " AND columnname =" + columnname;
  }

  /**
   * @param tableName The table to be truncated.
   * @return The SQL statement to truncate a table: remove all rows from it without a transaction
   */
  @Override
  public String getTruncateTableStatement( String tableName ) {
    return "DELETE FROM " + tableName;
  }

  /**
   * Generates the SQL statement to add a column to the specified table For this generic type, i set it to the most
   * common possibility.
   *
   * @param tableName   The table to add
   * @param v           The column defined as a value
   * @param tk          the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk          the name of the primary key field
   * @param semicolon   whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to add a column to the specified table
   */
  @Override
  public String getAddColumnStatement( String tableName, IValueMeta v, String tk, boolean useAutoinc,
                                       String pk, boolean semicolon ) {
    return "ALTER TABLE " + tableName + " ADD " + getFieldDefinition( v, tk, pk, useAutoinc, true, false );
  }

  /**
   * Generates the SQL statement to modify a column in the specified table
   *
   * @param tableName   The table to add
   * @param v           The column defined as a value
   * @param tk          the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk          the name of the primary key field
   * @param semicolon   whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to modify a column in the specified table
   */
  @Override
  public String getModifyColumnStatement( String tableName, IValueMeta v, String tk, boolean useAutoinc,
                                          String pk, boolean semicolon ) {
    return "ALTER TABLE " + tableName + " MODIFY " + getFieldDefinition( v, tk, pk, useAutoinc, true, false );
  }

  @Override
  public String getFieldDefinition( IValueMeta v, String tk, String pk, boolean useAutoinc,
                                    boolean addFieldname, boolean addCr ) {
    String retval = "";

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    if ( addFieldname ) {
      retval += fieldname + " ";
    }

    int type = v.getType();
    switch ( type ) {
      case IValueMeta.TYPE_TIMESTAMP:
      case IValueMeta.TYPE_DATE:
        retval += "TIMESTAMP";
        break;
      case IValueMeta.TYPE_BOOLEAN:
        retval += "CHAR(1)";
        break;
      case IValueMeta.TYPE_NUMBER:
      case IValueMeta.TYPE_INTEGER:
      case IValueMeta.TYPE_BIGNUMBER:
        if ( fieldname.equalsIgnoreCase( tk ) || // Technical key
          fieldname.equalsIgnoreCase( pk ) // Primary key
        ) {
          retval += "INTEGER"; // TERADATA has no Auto-increment functionality nor Sequences!
        } else {
          if ( length > 0 ) {
            if ( precision > 0 || length > 9 ) {
              retval += "DECIMAL(" + length + ", " + precision + ")";
            } else {
              if ( length > 5 ) {
                retval += "INTEGER";
              } else {
                if ( length < 3 ) {
                  retval += "BYTEINT";
                } else {
                  retval += "SMALLINT";
                }
              }
            }

          } else {
            retval += "DOUBLE PRECISION";
          }
        }
        break;
      case IValueMeta.TYPE_STRING:
        if ( length > 64000 ) {
          retval += "CLOB";
        } else {
          retval += "VARCHAR";
          if ( length > 0 ) {
            retval += "(" + length + ")";
          } else {
            retval += "(64000)"; // Maybe use some default DB String length?
          }
        }
        break;
      default:
        retval += " UNKNOWN";
        break;
    }

    if ( addCr ) {
      retval += Const.CR;
    }

    return retval;
  }

  @Override
  public String getExtraOptionSeparator() {
    return ",";
  }

  @Override
  public String getExtraOptionIndicator() {
    return "/";
  }

  @Override
  public String getExtraOptionsHelpText() {
    return "http://www.info.ncr.com/eTeradata-BrowseBy-Results.cfm?pl=&PID=&title=%25&release="
      + "&kword=CJDBC&sbrn=7&nm=Teradata+Tools+and+Utilities+-+Java+Database+Connectivity+(JDBC)";
  }

  @Override
  public int getDefaultDatabasePort() {
    return 1025;
  }

  /**
   * @return an array of reserved words for the database type...
   */
  @Override
  public String[] getReservedWords() {
    return new String[] {
      "ABORT", "ABORTSESSION", "ABS", "ACCESS_LOCK", "ACCOUNT", "ACOS", "ACOSH", "ADD", "ADD_MONTHS", "ADMIN",
      "AFTER", "AGGREGATE", "ALL", "ALTER", "AMP", "AND", "ANSIDATE", "ANY", "ARGLPAREN", "AS", "ASC", "ASIN",
      "ASINH", "AT", "ATAN", "ATAN2", "ATANH", "ATOMIC", "AUTHORIZATION", "AVE", "AVERAGE", "AVG", "BEFORE",
      "BEGIN", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BT", "BUT", "BY", "BYTE", "BYTEINT", "BYTES",
      "CALL", "CASE", "CASE_N", "CASESPECIFIC", "CAST", "CD", "CHAR", "CHAR_LENGTH", "CHAR2HEXINT", "CHARACTER",
      "CHARACTER_LENGTH", "CHARACTERS", "CHARS", "CHECK", "CHECKPOINT", "CLASS", "CLOB", "CLOSE", "CLUSTER",
      "CM", "COALESCE", "COLLATION", "COLLECT", "COLUMN", "COMMENT", "COMMIT", "COMPRESS", "CONSTRAINT",
      "CONSTRUCTOR", "CONSUME", "CONTAINS", "CONTINUE", "CONVERT_TABLE_HEADER", "CORR", "COS", "COSH", "COUNT",
      "COVAR_POP", "COVAR_SAMP", "CREATE", "CROSS", "CS", "CSUM", "CT", "CUBE", "CURRENT", "CURRENT_DATE",
      "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURSOR", "CV", "CYCLE", "DATABASE", "DATABLOCKSIZE", "DATE",
      "DATEFORM", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRED", "DEGREES", "DEL",
      "DELETE", "DESC", "DETERMINISTIC", "DIAGNOSTIC", "DISABLED", "DISTINCT", "DO", "DOMAIN", "DOUBLE", "DROP",
      "DUAL", "DUMP", "DYNAMIC", "EACH", "ECHO", "ELSE", "ELSEIF", "ENABLED", "END", "EQ", "EQUALS", "ERROR",
      "ERRORFILES", "ERRORTABLES", "ESCAPE", "ET", "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXIT", "EXP",
      "EXPLAIN", "EXTERNAL", "EXTRACT", "FALLBACK", "FASTEXPORT", "FETCH", "FIRST", "FLOAT", "FOR", "FOREIGN",
      "FORMAT", "FOUND", "FREESPACE", "FROM", "FULL", "FUNCTION", "GE", "GENERATED", "GIVE", "GRANT", "GRAPHIC",
      "GROUP", "GROUPING", "GT", "HANDLER", "HASH", "HASHAMP", "HASHBAKAMP", "HASHBUCKET", "HASHROW", "HAVING",
      "HELP", "HOUR", "IDENTITY", "IF", "IMMEDIATE", "IN", "INCONSISTENT", "INDEX", "INITIATE", "INNER",
      "INOUT", "INPUT", "INS", "INSERT", "INSTANCE", "INSTEAD", "INT", "INTEGER", "INTEGERDATE", "INTERSECT",
      "INTERVAL", "INTO", "IS", "ITERATE", "JAR", "JOIN", "JOURNAL", "KEY", "KURTOSIS", "LANGUAGE", "LARGE",
      "LE", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT", "LN", "LOADING", "LOCAL", "LOCATOR", "LOCK", "LOCKING",
      "LOG", "LOGGING", "LOGON", "LONG", "LOOP", "LOWER", "LT", "MACRO", "MAP", "MAVG", "MAX", "MAXIMUM",
      "MCHARACTERS", "MDIFF", "MERGE", "METHOD", "MIN", "MINDEX", "MINIMUM", "MINUS", "MINUTE", "MLINREG",
      "MLOAD", "MOD", "MODE", "MODIFIES", "MODIFY", "MONITOR", "MONRESOURCE", "MONSESSION", "MONTH", "MSUBSTR",
      "MSUM", "MULTISET", "NAMED", "NATURAL", "NE", "NEW", "NEW_TABLE", "NEXT", "NO", "NONE", "NOT", "NOWAIT",
      "NULL", "NULLIF", "NULLIFZERO", "NUMERIC", "OBJECT", "OBJECTS", "OCTET_LENGTH", "OF", "OFF", "OLD",
      "OLD_TABLE", "ON", "ONLY", "OPEN", "OPTION", "OR", "ORDER", "ORDERING", "OUT", "OUTER", "OUTPUT", "OVER",
      "OVERLAPS", "OVERRIDE", "PARAMETER", "PASSWORD", "PERCENT", "PERCENT_RANK", "PERM", "PERMANENT",
      "POSITION", "PRECISION", "PREPARE", "PRESERVE", "PRIMARY", "PRIVILEGES", "PROCEDURE", "PROFILE",
      "PROTECTION", "PUBLIC", "QUALIFIED", "QUALIFY", "QUANTILE", "QUEUE", "RADIANS", "RANDOM", "RANGE_N",
      "RANK", "READS", "REAL", "RECURSIVE", "REFERENCES", "REFERENCING", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT",
      "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY", "RELATIVE", "RELEASE",
      "RENAME", "REPEAT", "REPLACE", "REPLCONTROL", "REPLICATION", "REQUEST", "RESTART", "RESTORE", "RESULT",
      "RESUME", "RET", "RETRIEVE", "RETURN", "RETURNS", "REVALIDATE", "REVOKE", "RIGHT", "RIGHTS", "ROLE",
      "ROLLBACK", "ROLLFORWARD", "ROLLUP", "ROW", "ROW_NUMBER", "ROWID", "ROWS", "SAMPLE", "SAMPLEID", "SCROLL",
      "SECOND", "SEL", "SELECT", "SESSION", "SET", "SETRESRATE", "SETS", "SETSESSRATE", "SHOW", "SIN", "SINH",
      "SKEW", "SMALLINT", "SOME", "SOUNDEX", "SPECIFIC", "SPOOL", "SQL", "SQLEXCEPTION", "SQLTEXT",
      "SQLWARNING", "SQRT", "SS", "START", "STARTUP", "STATEMENT", "STATISTICS", "STDDEV_POP", "STDDEV_SAMP",
      "STEPINFO", "STRING_CS", "SUBSCRIBER", "SUBSTR", "SUBSTRING", "SUM", "SUMMARY", "SUSPEND", "TABLE", "TAN",
      "TANH", "TBL_CS", "TEMPORARY", "TERMINATE", "THEN", "THRESHOLD", "TIME", "TIMESTAMP", "TIMEZONE_HOUR",
      "TIMEZONE_MINUTE", "TITLE", "TO", "TOP", "TRACE", "TRAILING", "TRANSACTION", "TRANSFORM", "TRANSLATE",
      "TRANSLATE_CHK", "TRIGGER", "TRIM", "TRUE", "TYPE", "UC", "UDTCASTAS", "UDTCASTLPAREN", "UDTMETHOD",
      "UDTTYPE", "UDTUSAGE", "UESCAPE", "UNDEFINED", "UNDO", "UNION", "UNIQUE", "UNTIL", "UPD", "UPDATE",
      "UPPER", "UPPERCASE", "USER", "USING", "VALUE", "VALUES", "VAR_POP", "VAR_SAMP", "VARBYTE", "VARCHAR",
      "VARGRAPHIC", "VARYING", "VIEW", "VOLATILE", "WHEN", "WHERE", "WHILE", "WIDTH_BUCKET", "WITH", "WITHOUT",
      "WORK", "YEAR", "ZEROIFNULL", "ZONE" };
  }

  /**
   * Overrides parent behavior to allow <code>getDatabasePortNumberString</code> value to override value of
   * <code>DBS_PORT</code> extra option.
   */
  @Override
  public Map<String, String> getExtraOptions() {
    Map<String, String> map = super.getExtraOptions();

    if ( !Utils.isEmpty( getPort() ) ) {
      map.put( getPluginId() + ".DBS_PORT", getPort() );
    }

    return map;
  }

}
