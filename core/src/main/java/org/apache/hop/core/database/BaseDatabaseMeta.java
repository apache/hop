// CHECKSTYLE:FileLength:OFF
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

package org.apache.hop.core.database;

import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * This class contains the basic information on a database connection. It is not intended to be used other than the
 * inheriting classes such as OracleDatabaseInfo, ...
 *
 * @author Matt
 * @since 11-mrt-2005
 */
public abstract class BaseDatabaseMeta implements Cloneable, IDatabase {

  /**
   * The SQL to execute at connect time (right after connecting)
   */
  public static final String ATTRIBUTE_SQL_CONNECT = "SQL_CONNECT";

  /**
   * The prefix for all the extra options attributes
   */
  public static final String ATTRIBUTE_PREFIX_EXTRA_OPTION = "EXTRA_OPTION_";

  /**
   * A flag to determine if we should use result streaming on MySQL
   */
  public static final String ATTRIBUTE_USE_RESULT_STREAMING = "STREAM_RESULTS";

  /**
   * A flag to determine if we should use a double decimal separator to specify schema/table combinations on MS-SQL
   * server
   */
  public static final String ATTRIBUTE_MSSQL_DOUBLE_DECIMAL_SEPARATOR = "MSSQL_DOUBLE_DECIMAL_SEPARATOR";

  /**
   * A flag to determine if we should quote all fields
   */
  public static final String ATTRIBUTE_QUOTE_ALL_FIELDS = "QUOTE_ALL_FIELDS";

  /**
   * A flag to determine if we should force all identifiers to lower case
   */
  public static final String ATTRIBUTE_FORCE_IDENTIFIERS_TO_LOWERCASE = "FORCE_IDENTIFIERS_TO_LOWERCASE";

  /**
   * A flag to determine if we should force all identifiers to UPPER CASE
   */
  public static final String ATTRIBUTE_FORCE_IDENTIFIERS_TO_UPPERCASE = "FORCE_IDENTIFIERS_TO_UPPERCASE";

  /**
   * The preferred schema to use if no other has been specified.
   */
  public static final String ATTRIBUTE_PREFERRED_SCHEMA_NAME = "PREFERRED_SCHEMA_NAME";

  /**
   * Checkbox to allow you to configure if the database supports the boolean data type or not. Defaults to "false" for
   * backward compatibility!
   */
  public static final String ATTRIBUTE_SUPPORTS_BOOLEAN_DATA_TYPE = "SUPPORTS_BOOLEAN_DATA_TYPE";

  /**
   * Checkbox to allow you to configure if the database supports the Timestamp data type or not. Defaults to "false" for
   * backward compatibility!
   */
  public static final String ATTRIBUTE_SUPPORTS_TIMESTAMP_DATA_TYPE = "SUPPORTS_TIMESTAMP_DATA_TYPE";

  /**
   * Checkbox to allow you to configure if the reserved words will have their case changed during the handleCase call
   */
  public static final String ATTRIBUTE_PRESERVE_RESERVED_WORD_CASE = "PRESERVE_RESERVED_WORD_CASE";

  public static final String SEQUENCE_FOR_BATCH_ID = "SEQUENCE_FOR_BATCH_ID";
  public static final String AUTOINCREMENT_SQL_FOR_BATCH_ID = "AUTOINCREMENT_SQL_FOR_BATCH_ID";

  public static final String ID_USERNAME_LABEL = "username-label";
  public static final String ID_USERNAME_WIDGET = "username-widget";

  public static final String ID_PASSWORD_LABEL = "password-label";
  public static final String ID_PASSWORD_WIDGET = "password-widget";

  /**
   * Boolean to indicate if savepoints can be released Most databases do, so we set it to true. Child classes can
   * overwrite with false if need be.
   */
  protected boolean releaseSavepoint = true;

  /**
   * The SQL, minus the table name, to select the number of rows from a table
   */
  public static final String SELECT_COUNT_STATEMENT = "select count(*) FROM";


  private static final String FIELDNAME_PROTECTOR = "_";

  @HopMetadataProperty
  protected int accessType; // NATIVE / OCI

  @HopMetadataProperty
  @GuiWidgetElement(
    id = "hostname",
    order = "01",
    label = "i18n:org.apache.hop.ui.core.database:DatabaseDialog.label.ServerHostname",
    type = GuiElementType.TEXT,
    variables = true,
    parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID )
  protected String hostname;

  @HopMetadataProperty
  @GuiWidgetElement(
    id = "port",
    order = "02",
    label = "i18n:org.apache.hop.ui.core.database:DatabaseDialog.label.PortNumber",
    type = GuiElementType.TEXT,
    variables = true,
    parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID )
  protected String port;

  @HopMetadataProperty
  @GuiWidgetElement(
    id = "databaseName",
    order = "03",
    label = "i18n:org.apache.hop.ui.core.database:DatabaseDialog.label.DatabaseName",
    type = GuiElementType.TEXT,
    variables = true,
    parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID )
  protected String databaseName;

  @HopMetadataProperty
  protected String username;

  @HopMetadataProperty(password = true)
  protected String password;

  /**
   * Available for ALL database types
   */
  @HopMetadataProperty
  protected String manualUrl;

  @HopMetadataProperty
  protected String servername; // Informix only!

  @HopMetadataProperty
  protected String dataTablespace; // data storage location, For Oracle & perhaps others
  @HopMetadataProperty
  protected String indexTablespace; // index storage location, For Oracle & perhaps others

  private boolean changed;

  @HopMetadataProperty
  protected Map<String,String> attributes;

  @HopMetadataProperty
  protected String pluginId;
  @HopMetadataProperty
  protected String pluginName;


  public BaseDatabaseMeta() {
    attributes = Collections.synchronizedMap( new HashMap<>() );
    changed = false;
    if ( getAccessTypeList() != null && getAccessTypeList().length > 0 ) {
      accessType = getAccessTypeList()[ 0 ];
    }
  }

  /**
   * @return plugin ID of this class
   */
  @Override
  public String getPluginId() {
    return pluginId;
  }

  /**
   * @param pluginId The plugin ID to set.
   */
  @Override
  public void setPluginId( String pluginId ) {
    this.pluginId = pluginId;
  }

  /**
   * @return plugin name of this class
   */
  @Override
  public String getPluginName() {
    return pluginName;
  }

  /**
   * @param pluginName The plugin name to set.
   */
  @Override
  public void setPluginName( String pluginName ) {
    this.pluginName = pluginName;
  }

  @Override
  public abstract int[] getAccessTypeList();

  /**
   * @return Returns the accessType.
   */
  @Override
  public int getAccessType() {
    return accessType;
  }

  /**
   * @param accessType The accessType to set.
   */
  @Override
  public void setAccessType( int accessType ) {
    this.accessType = accessType;
  }

  /**
   * @return Returns the changed.
   */
  @Override
  public boolean isChanged() {
    return changed;
  }

  /**
   * @param changed The changed to set.
   */
  @Override
  public void setChanged( boolean changed ) {
    this.changed = changed;
  }

  /**
   * @return Returns the databaseName.
   */
  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * @param databaseName The databaseName to set.
   */
  @Override
  public void setDatabaseName( String databaseName ) {
    this.databaseName = databaseName;
  }

  /**
   * @return Returns the hostname.
   */
  @Override
  public String getHostname() {
    return hostname;
  }

  /**
   * @param hostname The hostname to set.
   */
  @Override
  public void setHostname( String hostname ) {
    this.hostname = hostname;
  }

  /**
   * Gets port
   *
   * @return value of port
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port The port to set
   */
  public void setPort( String port ) {
    this.port = port;
  }

  /**
   * @return Returns the password.
   */
  @Override
  public String getPassword() {
    return password;
  }

  /**
   * @param password The password to set.
   */
  @Override
  public void setPassword( String password ) {
    this.password = password;
  }


  /**
   * @return Returns the servername.
   */
  @Override
  public String getServername() {
    return servername;
  }

  /**
   * @param servername The servername to set.
   */
  @Override
  public void setServername( String servername ) {
    this.servername = servername;
  }

  /**
   * @return Returns the tablespaceData.
   */
  @Override
  public String getDataTablespace() {
    return dataTablespace;
  }

  /**
   * @param dataTablespace The data tablespace to set.
   */
  @Override
  public void setDataTablespace( String dataTablespace ) {
    this.dataTablespace = dataTablespace;
  }

  /**
   * @return Returns the index tablespace.
   */
  @Override
  public String getIndexTablespace() {
    return indexTablespace;
  }

  /**
   * @param indexTablespace The index tablespace to set.
   */
  @Override
  public void setIndexTablespace( String indexTablespace ) {
    this.indexTablespace = indexTablespace;
  }

  /**
   * @return Returns the username.
   */
  @Override
  public String getUsername() {
    return username;
  }

  /**
   * @param username The username to set.
   */
  @Override
  public void setUsername( String username ) {
    this.username = username;
  }

  /**
   * @return The extra attributes for this database connection
   */
  @Override
  public Map<String, String> getAttributes() {
    return attributes;
  }

  /**
   * Set extra attributes on this database connection
   *
   * @param attributes The extra attributes to set on this database connection.
   */
  @Override
  public void setAttributes( Map<String,String> attributes ) {
    this.attributes = attributes;
  }

  /**
   * Gets manualUrl
   *
   * @return A manually entered URL which will be used over the internally generated one
   */
  @Override public String getManualUrl() {
    return manualUrl;
  }

  /**
   * @param manualUrl A manually entered URL which will be used over the internally generated one
   */
  @Override public void setManualUrl( String manualUrl ) {
    this.manualUrl = manualUrl;
  }

  /**
   * Clone the basic settings for this connection!
   */
  @Override
  public Object clone() {
    BaseDatabaseMeta retval = null;
    try {
      retval = (BaseDatabaseMeta) super.clone();

      // CLone the attributes as well...
      retval.attributes = Collections.synchronizedMap( new HashMap<>() );
      for (String key : attributes.keySet()) {
        retval.attributes.put(key, attributes.get(key));
      }
    } catch ( CloneNotSupportedException e ) {
      throw new RuntimeException( e );
    }
    return retval;
  }

  /*
   * *******************************************************************************
   * DEFAULT SETTINGS FOR ALL DATABASES ********************************************************************************
   */

  /**
   * @return the default database port number
   */
  @Override
  public int getDefaultDatabasePort() {
    return -1; // No default port or not used.
  }

  @Override public Map<String, String> getDefaultOptions() {
    return Collections.emptyMap();
  }

  /**
   * See if this database supports the setCharacterStream() method on a PreparedStatement.
   *
   * @return true if we can set a Stream on a field in a PreparedStatement. False if not.
   */
  @Override
  public boolean supportsSetCharacterStream() {
    return true;
  }

  /**
   * @return Whether or not the database can use auto increment type of fields (pk)
   */
  @Override
  public boolean supportsAutoInc() {
    return true;
  }

  @Override
  public String getLimitClause( int nrRows ) {
    return "";
  }

  @Override
  public int getNotFoundTK( boolean useAutoIncrement ) {
    return 0;
  }

  /**
   * Get the SQL to get the next value of a sequence. (Oracle/PGSQL only)
   *
   * @param sequenceName The sequence name
   * @return the SQL to get the next value of a sequence. (Oracle/PGSQL only)
   */
  @Override
  public String getSqlNextSequenceValue( String sequenceName ) {
    return "";
  }

  /**
   * Get the current value of a database sequence
   *
   * @param sequenceName The sequence to check
   * @return The current value of a database sequence
   */
  @Override
  public String getSqlCurrentSequenceValue( String sequenceName ) {
    return "";
  }

  /**
   * Check if a sequence exists.
   *
   * @param sequenceName The sequence to check
   * @return The SQL to get the name of the sequence back from the databases data dictionary
   */
  @Override
  public String getSqlSequenceExists( String sequenceName ) {
    return "";
  }

  /**
   * Checks whether or not the command setFetchSize() is supported by the JDBC driver...
   *
   * @return true is setFetchSize() is supported!
   */
  @Override
  public boolean isFetchSizeSupported() {
    return true;
  }

  /**
   * Indicates the need to insert a placeholder (0) for auto increment fields.
   *
   * @return true if we need a placeholder for auto increment fields in insert statements.
   */
  @Override
  public boolean needsPlaceHolder() {
    return false;
  }

  /**
   * @return true if the database supports schemas
   */
  @Override
  public boolean supportsSchemas() {
    return true;
  }

  /**
   * @return true if the database supports catalogs
   */
  @Override
  public boolean supportsCatalogs() {
    return true;
  }

  /**
   * @return true when the database engine supports empty transaction. (for example Informix does not on a non-ANSI
   * database type!)
   */
  @Override
  public boolean supportsEmptyTransactions() {
    return true;
  }

  /**
   * @return the function for SUM agrregate
   */
  @Override
  public String getFunctionSum() {
    return "SUM";
  }

  /**
   * @return the function for Average agrregate
   */
  @Override
  public String getFunctionAverage() {
    return "AVG";
  }

  /**
   * @return the function for Minimum agrregate
   */
  @Override
  public String getFunctionMinimum() {
    return "MIN";
  }

  /**
   * @return the function for Maximum agrregate
   */
  @Override
  public String getFunctionMaximum() {
    return "MAX";
  }

  /**
   * @return the function for Count agrregate
   */
  @Override
  public String getFunctionCount() {
    return "COUNT";
  }

  /**
   * Get the schema-table combination to query the right table. Usually that is SCHEMA.TABLENAME, however there are
   * exceptions to this rule...
   *
   * @param schemaName The schema name
   * @param tablePart  The table name
   * @return the schema-table combination to query the right table.
   */
  @Override
  public String getSchemaTableCombination( String schemaName, String tablePart ) {
    return schemaName + "." + tablePart;
  }

  /**
   * Checks for quotes before quoting schema and table. Many dialects had hardcoded quotes, they probably didn't get
   * updated properly when quoteFields() was introduced to DatabaseMeta.
   *
   * @param schemaPart
   * @param tablePart
   * @return quoted schema and table
   * @deprecated we should phase this out in 5.0, but it's there to keep backwards compatibility in the 4.x releases.
   */
  @Deprecated
  public String getBackwardsCompatibleSchemaTableCombination( String schemaPart, String tablePart ) {
    String schemaTable = "";
    if ( schemaPart != null && ( schemaPart.contains( getStartQuote() ) || schemaPart.contains( getEndQuote() ) ) ) {
      schemaTable += schemaPart;
    } else {
      schemaTable += getStartQuote() + schemaPart + getEndQuote();
    }
    schemaTable += ".";
    if ( tablePart != null && ( tablePart.contains( getStartQuote() ) || tablePart.contains( getEndQuote() ) ) ) {
      schemaTable += tablePart;
    } else {
      schemaTable += getStartQuote() + tablePart + getEndQuote();
    }
    return schemaTable;
  }

  /**
   * Checks for quotes before quoting table. Many dialects had hardcoded quotes, they probably didn't get updated
   * properly when quoteFields() was introduced to DatabaseMeta.
   *
   * @param tablePart
   * @return quoted table
   * @deprecated we should phase this out in 5.0, but it's there to keep backwards compatibility in the 4.x releases.
   */
  @Deprecated
  public String getBackwardsCompatibleTable( String tablePart ) {
    if ( tablePart != null && ( tablePart.contains( getStartQuote() ) || tablePart.contains( getEndQuote() ) ) ) {
      return tablePart;
    } else {
      return getStartQuote() + tablePart + getEndQuote();
    }
  }

  /**
   * Get the maximum length of a text field for this database connection. This includes optional CLOB, Memo and Text
   * fields. (the maximum!)
   *
   * @return The maximum text field length for this database type. (mostly CLOB_LENGTH)
   */
  @Override
  public int getMaxTextFieldLength() {
    return DatabaseMeta.CLOB_LENGTH;
  }

  /**
   * Get the maximum length of a text field (VARCHAR) for this database connection. If this size is exceeded use a CLOB.
   *
   * @return The maximum VARCHAR field length for this database type. (mostly identical to getMaxTextFieldLength() -
   * CLOB_LENGTH)
   */
  @Override
  public int getMaxVARCHARLength() {
    return DatabaseMeta.CLOB_LENGTH;
  }

  /**
   * @return true if the database supports transactions.
   */
  @Override
  public boolean supportsTransactions() {
    return true;
  }

  /**
   * @return true if the database supports sequences
   */
  @Override
  public boolean supportsSequences() {
    return false;
  }

  /**
   * @return true if the database supports bitmap indexes
   */
  @Override
  public boolean supportsBitmapIndex() {
    return true;
  }

  /**
   * @return true if the database JDBC driver supports the setLong command
   */
  @Override
  public boolean supportsSetLong() {
    return true;
  }

  /**
   * Generates the SQL statement to drop a column from the specified table
   *
   * @param tableName   The table to add
   * @param v           The column defined as a value
   * @param tk          the name of the technical key field
   * @param useAutoIncrement whether or not this field uses auto increment
   * @param pk          the name of the primary key field
   * @param semicolon   whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to drop a column from the specified table
   */
  @Override
  public String getDropColumnStatement( String tableName, IValueMeta v, String tk, boolean useAutoIncrement,
                                        String pk, boolean semicolon ) {
    return "ALTER TABLE " + tableName + " DROP " + v.getName() + Const.CR;
  }

  /**
   * @return an array of reserved words for the database type...
   */
  @Override
  public String[] getReservedWords() {
    return new String[] {};
  }

  /**
   * @return true if reserved words need to be double quoted ("password", "select", ...)
   */
  @Override
  public boolean quoteReservedWords() {
    return true;
  }

  /**
   * @return The start quote sequence, mostly just double quote, but sometimes [, ...
   */
  @Override
  public String getStartQuote() {
    return "\"";
  }

  /**
   * @return The end quote sequence, mostly just double quote, but sometimes ], ...
   */
  @Override
  public String getEndQuote() {
    return "\"";
  }

  /**
   * @return a list of table types to retrieve tables for the database
   */
  @Override
  public String[] getTableTypes() {
    return new String[] { "TABLE" };
  }

  /**
   * @return a list of table types to retrieve views for the database
   */
  @Override
  public String[] getViewTypes() {
    return new String[] { "VIEW" };
  }

  /**
   * @return a list of table types to retrieve synonyms for the database
   */
  @Override
  public String[] getSynonymTypes() {
    return new String[] { "SYNONYM" };
  }

  /**
   * @return true if we need to supply the schema-name to getTables in order to get a correct list of items.
   */
  @Override
  public boolean useSchemaNameForTableList() {
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
   * @return The SQL on this database to get a list of stored procedures.
   */
  @Override
  public String getSqlListOfProcedures() {
    return null;
  }

  /**
   * @return The SQL on this database to get a list of sequences.
   */
  @Override
  public String getSqlListOfSequences() {
    return null;
  }

  /**
   * @param tableName The table to be truncated.
   * @return The SQL statement to truncate a table: remove all rows from it without a transaction
   */
  @Override
  public String getTruncateTableStatement( String tableName ) {
    return "TRUNCATE TABLE " + tableName;
  }

  /**
   * Returns the minimal SQL to launch in order to determine the layout of the resultset for a given database table
   *
   * @param tableName The name of the table to determine the layout for
   * @return The SQL to launch.
   */
  @Override
  public String getSqlQueryFields( String tableName ) {
    return "SELECT * FROM " + tableName;
  }

  /**
   * Most databases round number(7,2) 17.29999999 to 17.30, but some don't.
   *
   * @return true if the database supports roundinf of floating point data on update/insert
   */
  @Override
  public boolean supportsFloatRoundingOnUpdate() {
    return true;
  }

  /**
   * @param tableNames The names of the tables to lock
   * @return The SQL command to lock database tables for write purposes. null is returned in case locking is not
   * supported on the target database. null is the default value
   */
  @Override
  public String getSqlLockTables( String[] tableNames ) {
    return null;
  }

  /**
   * @param tableNames The names of the tables to unlock
   * @return The SQL command to unlock database tables. null is returned in case locking is not supported on the target
   * database. null is the default value
   */
  @Override
  public String getSqlUnlockTables( String[] tableNames ) {
    return null;
  }

  /**
   * @return true if the database supports timestamp to date conversion. For example Interbase doesn't support this!
   */
  @Override
  public boolean supportsTimeStampToDateConversion() {
    return true;
  }

  /**
   * @return true if the database JDBC driver supports batch updates For example Interbase doesn't support this!
   */
  @Override
  public boolean supportsBatchUpdates() {
    return true;
  }

  public String getAttributeProperty(String key, String defaultValue) {
    String value = attributes.get(key);
    if (value==null) {
      return defaultValue;
    }
    return value;
  }

  public String getAttributeProperty(String key) {
    return attributes.get(key);
  }

  /**
   * @return true if the database supports a boolean, bit, logical, ... datatype The default is false: map to a string.
   */
  @Override
  public boolean supportsBooleanDataType() {
    String supportsBooleanString = getAttributeProperty( ATTRIBUTE_SUPPORTS_BOOLEAN_DATA_TYPE, "N" );
    return "Y".equalsIgnoreCase( supportsBooleanString );
  }

  /**
   * @param b Set to true if the database supports a boolean, bit, logical, ... datatype
   */
  @Override
  public void setSupportsBooleanDataType( boolean b ) {
    attributes.put( ATTRIBUTE_SUPPORTS_BOOLEAN_DATA_TYPE, b ? "Y" : "N" );
  }

  /**
   * @return true if the database supports the Timestamp data type (nanosecond precision and all)
   */
  @Override
  public boolean supportsTimestampDataType() {
    String supportsTimestamp = getAttributeProperty( ATTRIBUTE_SUPPORTS_TIMESTAMP_DATA_TYPE, "N" );
    return "Y".equalsIgnoreCase( supportsTimestamp );
  }

  /**
   * @param b Set to true if the database supports the Timestamp data type (nanosecond precision and all)
   */
  @Override
  public void setSupportsTimestampDataType( boolean b ) {
    attributes.put( ATTRIBUTE_SUPPORTS_TIMESTAMP_DATA_TYPE, b ? "Y" : "N" );
  }

  /**
   * @return true if reserved words' case should be preserved
   */
  @Override
  public boolean preserveReservedCase() {
    String usePool = getAttributeProperty( ATTRIBUTE_PRESERVE_RESERVED_WORD_CASE, "Y" );
    return "Y".equalsIgnoreCase( usePool );
  }

  /**
   * @param b Set to true if reserved words' case should be preserved
   */
  @Override
  public void setPreserveReservedCase( boolean b ) {
    attributes.put( ATTRIBUTE_PRESERVE_RESERVED_WORD_CASE, b ? "Y" : "N" );
  }

  /**
   * @return true if the database defaults to naming tables and fields in uppercase. True for most databases except for
   * stuborn stuff like Postgres ;-)
   */
  @Override
  public boolean isDefaultingToUppercase() {
    return true;
  }

  /**
   * @return all the extra options that are set to be used for the database URL
   */
  @Override
  public Map<String, String> getExtraOptions() {
    Map<String, String> map = new Hashtable<>();

    for ( String attribute : attributes.keySet() ) {
      if ( attribute.startsWith( ATTRIBUTE_PREFIX_EXTRA_OPTION ) ) {
        String value = getAttributeProperty( attribute, "" );

        // Add to the map...
        map.put( attribute.substring( ATTRIBUTE_PREFIX_EXTRA_OPTION.length() ), value );
      }
    }

    return map;
  }

  /**
   * Add an extra option to the attributes list
   *
   * @param databaseTypeCode The database type code for which the option applies
   * @param option           The option to set
   * @param value            The value of the option
   */
  @Override
  public void addExtraOption( String databaseTypeCode, String option, String value ) {
    attributes.put( ATTRIBUTE_PREFIX_EXTRA_OPTION + databaseTypeCode + "." + option, value );
  }

  /**
   * @return The extra option separator in database URL for this platform (usually this is semicolon ; )
   */
  @Override
  public String getExtraOptionSeparator() {
    return ";";
  }

  /**
   * @return The extra option value separator in database URL for this platform (usually this is the equal sign = )
   */
  @Override
  public String getExtraOptionValueSeparator() {
    return "=";
  }

  /**
   * @return This indicator separates the normal URL from the options
   */
  @Override
  public String getExtraOptionIndicator() {
    return ";";
  }

  /**
   * @return true if the database supports connection options in the URL, false if they are put in a Properties object.
   */
  @Override
  public boolean supportsOptionsInURL() {
    return true;
  }

  /**
   * @return extra help text on the supported options on the selected database platform.
   */
  @Override
  public String getExtraOptionsHelpText() {
    return null;
  }

  /**
   * @return true if the database JDBC driver supports getBlob on the resultset. If not we must use getBytes() to get
   * the data.
   */
  @Override
  public boolean supportsGetBlob() {
    return true;
  }

  /**
   * @return The SQL to execute right after connecting
   */
  @Override
  public String getConnectSql() {
    return getAttributeProperty( ATTRIBUTE_SQL_CONNECT );
  }

  /**
   * @param sql The SQL to execute right after connecting
   */
  @Override
  public void setConnectSql(String sql ) {
    attributes.put( ATTRIBUTE_SQL_CONNECT, sql );
  }

  /**
   * @return true if the database supports setting the maximum number of return rows in a resultset.
   */
  @Override
  public boolean supportsSetMaxRows() {
    return true;
  }

  @Override
  public String getSqlTableExists( String tableName ) {
    return "SELECT 1 FROM " + tableName;
  }

  @Override
  public String getSqlColumnExists( String columnname, String tableName ) {
    return "SELECT " + columnname + " FROM " + tableName;
  }

  /**
   * @return true if the database is streaming results (normally this is an option just for MySQL).
   */
  @Override
  public boolean isStreamingResults() {
    String usePool = getAttributeProperty( ATTRIBUTE_USE_RESULT_STREAMING, "Y" ); // DEFAULT TO YES!!
    return "Y".equalsIgnoreCase( usePool );
  }

  /**
   * @param useStreaming true if we want the database to stream results (normally this is an option just for MySQL).
   */
  @Override
  public void setStreamingResults( boolean useStreaming ) {
    attributes.put( ATTRIBUTE_USE_RESULT_STREAMING, useStreaming ? "Y" : "N" );
  }

  /**
   * @return true if all fields should always be quoted in db
   */
  @Override
  public boolean isQuoteAllFields() {
    String quoteAllFields = getAttributeProperty( ATTRIBUTE_QUOTE_ALL_FIELDS, "N" ); // DEFAULT TO NO!!
    return "Y".equalsIgnoreCase( quoteAllFields );
  }

  /**
   * @param quoteAllFields true if we want the database to stream results (normally this is an option just for MySQL).
   */
  @Override
  public void setQuoteAllFields( boolean quoteAllFields ) {
    attributes.put( ATTRIBUTE_QUOTE_ALL_FIELDS, quoteAllFields ? "Y" : "N" );
  }

  /**
   * @return true if all identifiers should be forced to lower case
   */
  @Override
  public boolean isForcingIdentifiersToLowerCase() {
    String forceLowerCase = getAttributeProperty( ATTRIBUTE_FORCE_IDENTIFIERS_TO_LOWERCASE, "N" ); // DEFAULT TO NO!!
    return "Y".equalsIgnoreCase( forceLowerCase );
  }

  /**
   * @param forceLowerCase true if all identifiers should be forced to lower case
   */
  @Override
  public void setForcingIdentifiersToLowerCase( boolean forceLowerCase ) {
    attributes.put( ATTRIBUTE_FORCE_IDENTIFIERS_TO_LOWERCASE, forceLowerCase ? "Y" : "N" );
  }

  /**
   * @return true if all identifiers should be forced to upper case
   */
  @Override
  public boolean isForcingIdentifiersToUpperCase() {
    String forceUpperCase = getAttributeProperty( ATTRIBUTE_FORCE_IDENTIFIERS_TO_UPPERCASE, "N" ); // DEFAULT TO NO!!
    return "Y".equalsIgnoreCase( forceUpperCase );
  }

  /**
   * @param forceUpperCase true if all identifiers should be forced to upper case
   */
  @Override
  public void setForcingIdentifiersToUpperCase( boolean forceUpperCase ) {
    attributes.put( ATTRIBUTE_FORCE_IDENTIFIERS_TO_UPPERCASE, forceUpperCase ? "Y" : "N" );
  }

  /**
   * @return true if we use a double decimal separator to specify schema/table combinations on MS-SQL server
   */
  @Override
  public boolean isUsingDoubleDecimalAsSchemaTableSeparator() {
    String usePool = getAttributeProperty( ATTRIBUTE_MSSQL_DOUBLE_DECIMAL_SEPARATOR, "N" ); // DEFAULT TO YES!!
    return "Y".equalsIgnoreCase( usePool );
  }

  /**
   * @param useDoubleDecimalSeparator true if we should use a double decimal separator to specify schema/table combinations on MS-SQL server
   */
  @Override
  public void setUsingDoubleDecimalAsSchemaTableSeparator( boolean useDoubleDecimalSeparator ) {
    attributes.put( ATTRIBUTE_MSSQL_DOUBLE_DECIMAL_SEPARATOR, useDoubleDecimalSeparator ? "Y" : "N" );
  }

  /**
   * @return true if this database needs a transaction to perform a query (auto-commit turned off).
   */
  @Override
  public boolean isRequiringTransactionsOnQueries() {
    return true;
  }

  @Override
  public boolean isStrictBigNumberInterpretation() {
    return false;
  }
  
  /**
   * You can use this method to supply an alternate factory for the test method in the dialogs. This is useful for
   * plugins like SAP/R3 and PALO.
   *
   * @return the name of the database test factory to use.
   */
  @Override
  public String getDatabaseFactoryName() {
    return DatabaseFactory.class.getName();
  }

  /**
   * @return The preferred schema name of this database connection.
   */
  @Override
  public String getPreferredSchemaName() {
    return getAttributeProperty( ATTRIBUTE_PREFERRED_SCHEMA_NAME );
  }

  /**
   * @param preferredSchemaName The preferred schema name of this database connection.
   */
  @Override
  public void setPreferredSchemaName( String preferredSchemaName ) {
    attributes.put( ATTRIBUTE_PREFERRED_SCHEMA_NAME, preferredSchemaName );
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
  public boolean checkIndexExists( Database database, String schemaName, String tableName, String[] idxFields ) throws HopDatabaseException {

    String schemaTable = database.getDatabaseMeta().getQuotedSchemaTableCombination( database, schemaName, tableName );

    boolean[] exists = new boolean[ idxFields.length ];
    for ( int i = 0; i < exists.length; i++ ) {
      exists[ i ] = false;
    }

    try {
      // Get a list of all the indexes for this table
      ResultSet indexList = null;
      try {
        indexList = database.getDatabaseMetaData().getIndexInfo( null, null, schemaTable, false, true );
        while ( indexList.next() ) {
          // String tablen = indexList.getString("TABLE_NAME");
          // String indexn = indexList.getString("INDEX_NAME");
          String column = indexList.getString( "COLUMN_NAME" );
          // int pos = indexList.getShort("ORDINAL_POSITION");
          // int type = indexList.getShort("TYPE");

          int idx = Const.indexOfString( column, idxFields );
          if ( idx >= 0 ) {
            exists[ idx ] = true;
          }
        }
      } finally {
        if ( indexList != null ) {
          indexList.close();
        }
      }

      // See if all the fields are indexed...
      boolean all = true;
      for ( int i = 0; i < exists.length && all; i++ ) {
        if ( !exists[ i ] ) {
          all = false;
        }
      }

      return all;
    } catch ( Exception e ) {
      throw new HopDatabaseException( "Unable to determine if indexes exists on table [" + schemaTable + "]", e );
    }

  }

  /**
   * @return true if the database supports the NOMAXVALUE sequence option. The default is false, AS/400 and DB2 support
   * this.
   */
  @Override
  public boolean supportsSequenceNoMaxValueOption() {
    return false;
  }

  /**
   * @return true if we need to append the PRIMARY KEY block in the create table block after the fields, required for
   * Cache.
   */
  @Override
  public boolean requiresCreateTablePrimaryKeyAppend() {
    return false;
  }

  /**
   * @return true if the database requires you to cast a parameter to varchar before comparing to null. Only required
   * for DB2 and Vertica
   */
  @Override
  public boolean requiresCastToVariousForIsNull() {
    return false;
  }

  /**
   * @return Handles the special case of DB2 where the display size returned is twice the precision. In that case, the
   * length is the precision.
   */
  @Override
  public boolean isDisplaySizeTwiceThePrecision() {
    return false;
  }

  /**
   * Most databases allow you to retrieve result metadata by preparing a SELECT statement.
   *
   * @return true if the database supports retrieval of query metadata from a prepared statement. False if the query
   * needs to be executed first.
   */
  @Override
  public boolean supportsPreparedStatementMetadataRetrieval() {
    return true;
  }

  /**
   * @return true if this database only supports metadata retrieval on a result set, never on a statement (even if the
   * statement has been executed)
   */
  @Override
  public boolean supportsResultSetMetadataRetrievalOnly() {
    return false;
  }

  /**
   * @param tableName
   * @return true if the specified table is a system table
   */
  @Override
  public boolean isSystemTable( String tableName ) {
    return false;
  }

  /**
   * @return true if the database supports newlines in a SQL statements.
   */
  @Override
  public boolean supportsNewLinesInSql() {
    return true;
  }

  /**
   * @return the SQL to retrieve the list of schemas or null if the JDBC metadata needs to be used.
   */
  @Override
  public String getSqlListOfSchemas() {
    return null;
  }

  /**
   * @return The maximum number of columns in a database, <=0 means: no known limit
   */
  @Override
  public int getMaxColumnsInIndex() {
    return 0;
  }

  /**
   * @return true if the database supports error handling (recovery of failure) while doing batch updates.
   */
  @Override
  public boolean supportsErrorHandlingOnBatchUpdates() {
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
  public String getSqlInsertAutoIncUnknownDimensionRow( String schemaTable, String keyField, String versionField ) {
    return "insert into " + schemaTable + "(" + keyField + ", " + versionField + ") values (0, 1)";
  }

  /**
   * @return true if this is a relational database you can explore. Return false for SAP, PALO, etc.
   */
  @Override
  public boolean isExplorable() {
    return true;
  }

  /**
   * @param string
   * @return A string that is properly quoted for use in a SQL statement (insert, update, delete, etc)
   */
  @Override
  public String quoteSqlString(String string ) {
    string = string.replaceAll( "'", "''" );
    string = string.replaceAll( "\\n", "\\\\n" );
    string = string.replaceAll( "\\r", "\\\\r" );
    return "'" + string + "'";
  }

  /**
   * Build the SQL to count the number of rows in the passed table.
   *
   * @param tableName
   * @return
   */
  @Override
  public String getSelectCountStatement( String tableName ) {
    return SELECT_COUNT_STATEMENT + " " + tableName;
  }

  @Override
  public String generateColumnAlias( int columnIndex, String suggestedName ) {
    return "COL" + Integer.toString( columnIndex );
  }

  /**
   * Parse all possible statements from the provided SQL script.
   *
   * @param sqlScript Raw SQL Script to be parsed into executable statements.
   * @return List of parsed SQL statements to be executed separately.
   */
  @Override
  public List<String> parseStatements( String sqlScript ) {

    List<SqlScriptStatement> scriptStatements = getSqlScriptStatements( sqlScript );
    List<String> statements = new ArrayList<>();
    for ( SqlScriptStatement scriptStatement : scriptStatements ) {
      statements.add( scriptStatement.getStatement() );
    }
    return statements;
  }

  /**
   * Parse the statements in the provided SQL script, provide more information about where each was found in the script.
   *
   * @param sqlScript Raw SQL Script to be parsed into executable statements.
   * @return List of SQL script statements to be executed separately.
   */
  @Override
  public List<SqlScriptStatement> getSqlScriptStatements( String sqlScript ) {
    List<SqlScriptStatement> statements = new ArrayList<>();
    String all = sqlScript;
    int from = 0;
    int to = 0;
    int length = all.length();

    while ( to < length ) {
      char c = all.charAt( to );

      // Skip comment lines...
      //
      while ( all.substring( from ).startsWith( "--" ) ) {
        int nextLineIndex = all.indexOf( Const.CR, from );
        from = nextLineIndex + Const.CR.length();
        if ( to >= length ) {
          break;
        }
        c = all.charAt( c );
      }
      if ( to >= length ) {
        break;
      }

      // Skip over double quotes...
      //
      if ( c == '"' ) {
        int nextDQuoteIndex = all.indexOf( '"', to + 1 );
        if ( nextDQuoteIndex >= 0 ) {
          to = nextDQuoteIndex + 1;
        }
      }

      // Skip over back-ticks
      if ( c == '`' ) {
        int nextBacktickIndex = all.indexOf( '`', to + 1 );
        if ( nextBacktickIndex >= 0 ) {
          to = nextBacktickIndex + 1;
        }
      }

      c = all.charAt( to );
      if ( c == '\'' ) {
        boolean skip = true;

        // Don't skip over \' or ''
        //
        if ( to > 0 ) {
          char prevChar = all.charAt( to - 1 );
          if ( prevChar == '\\' || prevChar == '\'' ) {
            skip = false;
          }
        }

        // Jump to the next quote and continue from there.
        //
        while ( skip ) {
          int nextQuoteIndex = all.indexOf( '\'', to + 1 );
          if ( nextQuoteIndex >= 0 ) {
            to = nextQuoteIndex + 1;

            skip = false;

            if ( to < all.length() ) {
              char nextChar = all.charAt( to );
              if ( nextChar == '\'' ) {
                skip = true;
                to++;
              }
            }
            if ( to > 0 ) {
              char prevChar = all.charAt( to - 2 );
              if ( prevChar == '\\' ) {
                skip = true;
                to++;
              }
            }
          }
        }
      }

      c = all.charAt( to );

      // end of statement
      if ( c == ';' || to >= length - 1 ) {
        if ( to >= length - 1 ) {
          to++; // grab last char also!
        }

        String stat = all.substring( from, to );
        if ( !onlySpaces( stat ) ) {
          String s = Const.trim( stat );
          statements.add( new SqlScriptStatement(
            s, from, to, s.toUpperCase().startsWith( "SELECT" ) || s.toLowerCase().startsWith( "show" ) ) );
        }
        to++;
        from = to;
      } else {
        to++;
      }
    }
    return statements;
  }

  /**
   * @param str
   * @return True if {@code str} contains only spaces.
   */
  protected boolean onlySpaces( String str ) {
    for ( int i = 0; i < str.length(); i++ ) {
      int c = str.charAt( i );
      if ( c != ' ' && c != '\t' && c != '\n' && c != '\r' ) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return true if the database is a MySQL variant, like MySQL 5.1, InfiniDB, InfoBright, and so on.
   */
  @Override
  public boolean isMySqlVariant() {
    return false;
  }

  /**
   * @return true if the database is a Postgres variant like Postgres, Greenplum, Redshift and so on.
   */
  @Override
  public boolean isPostgresVariant() {
    return false;
  }

  /**
   * @return true if the database is a Teradata variant.
   */
  @Override
  public boolean isTeradataVariant() {
    return false;
  }

  /**
   * @return true if the database is a Sybase variant.
   */
  @Override
  public boolean isSybaseVariant() {
    return false;
  }

  /**
   * @return true if the database is a Sybase variant.
   */
  @Override
  public boolean isSybaseIQVariant() {
    return false;
  }

  /**
   * @return true if the database is a Neoview variant.
   */
  @Override
  public boolean isNeoviewVariant() {
    return false;
  }

  /**
   * @return true if the database is a Exasol variant.
   */
  @Override
  public boolean isExasolVariant() {
    return false;
  }

  /**
   * @return true if the database is a Informix variant.
   */
  @Override
  public boolean isInformixVariant() {
    return false;
  }

  /**
   * @return true if the database is a MS SQL Server (native) variant.
   */
  @Override
  public boolean isMsSqlServerNativeVariant() {
    return false;
  }

  /**
   * @return true if the database is a MS SQL Server variant.
   */
  @Override
  public boolean isMsSqlServerVariant() {
    return false;
  }

  /**
   * @return true if the database is an Oracle variant.
   */
  public boolean isOracleVariant() {
    return false;
  }

  /**
   * @return true if the database is an Netezza variant.
   */
  public boolean isNetezzaVariant() {
    return false;
  }

  /**
   * @return true if the database is a SQLite variant.
   */
  @Override
  public boolean isSqliteVariant() {
    return false;
  }


  /**
   * @return true if the database type can be tested against a database instance
   */
  public boolean canTest() {
    return true;
  }

  /**
   * @return true if the database name is a required parameter
   */
  public boolean requiresName() {
    return true;
  }

  /**
   * Returns a true of savepoints can be released, false if not.
   *
   * @return
   */
  @Override
  public boolean releaseSavepoint() {
    return releaseSavepoint;
  }

  /**
   * Returns the tablespace DDL fragment for a "Data" tablespace. In most databases that use tablespaces this is where
   * the tables are to be created.
   *
   * @param variables    variables used for possible substitution
   * @param databaseMeta databaseMeta the database meta used for possible string enclosure of the tablespace. This method needs
   *                     this as this is done after environmental substitution.
   * @return String the tablespace name for tables in the format "tablespace TABLESPACE_NAME". The TABLESPACE_NAME and
   * the passed DatabaseMata determines if TABLESPACE_NAME is to be enclosed in quotes.
   */
  @Override
  public String getDataTablespaceDDL( IVariables variables, DatabaseMeta databaseMeta ) {
    return getTablespaceDDL( variables, databaseMeta, databaseMeta.getIDatabase().getDataTablespace() );
  }

  /**
   * Returns the tablespace DDL fragment for a "Index" tablespace.
   *
   * @param variables    variables used for possible substitution
   * @param databaseMeta databaseMeta the database meta used for possible string enclosure of the tablespace. This method needs
   *                     this as this is done after environmental substitution.
   * @return String the tablespace name for indices in the format "tablespace TABLESPACE_NAME". The TABLESPACE_NAME and
   * the passed DatabaseMata determines if TABLESPACE_NAME is to be enclosed in quotes.
   */
  @Override
  public String getIndexTablespaceDDL( IVariables variables, DatabaseMeta databaseMeta ) {
    return getTablespaceDDL( variables, databaseMeta, databaseMeta.getIDatabase().getIndexTablespace() );
  }

  /**
   * Returns an empty string as most databases do not support tablespaces. Subclasses can override this method to
   * generate the DDL.
   *
   * @param variables      variables needed for variable substitution.
   * @param databaseMeta   databaseMeta needed for it's quoteField method. Since we are doing variable substitution we need to meta
   *                       so that we can act on the variable substitution first and then the creation of the entire string that will
   *                       be retuned.
   * @param tablespaceName tablespaceName name of the tablespace.
   * @return String an empty String as most databases do not use tablespaces.
   */
  public String getTablespaceDDL( IVariables variables, DatabaseMeta databaseMeta, String tablespaceName ) {
    return "";
  }

  /**
   * This method allows a database dialect to convert database specific data types to Hop data types.
   *
   * @param rs  The result set to use
   * @param val The description of the value to retrieve
   * @param i   the index on which we need to retrieve the value, 0-based.
   * @return The correctly converted Hop data type corresponding to the valueMeta description.
   * @throws HopDatabaseException
   */
  @Override
  public Object getValueFromResultSet( ResultSet rs, IValueMeta val, int i ) throws HopDatabaseException {

    return val.getValueFromResultSet( this, rs, i );

  }

  /**
   * @return true if the database supports the use of safe-points and if it is appropriate to ever use it (default to
   * false)
   */
  @Override
  public boolean useSafePoints() {
    return false;
  }

  /**
   * @return true if the database supports error handling (the default). Returns false for certain databases (SQLite)
   * that invalidate a prepared statement or even the complete connection when an error occurs.
   */
  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public String getSqlValue( IValueMeta valueMeta, Object valueData, String dateFormat ) throws HopValueException {

    StringBuilder ins = new StringBuilder();

    if ( valueMeta.isNull( valueData ) ) {
      ins.append( "null" );
    } else {
      // Normal cases...
      //
      switch ( valueMeta.getType() ) {
        case IValueMeta.TYPE_BOOLEAN:
        case IValueMeta.TYPE_STRING:
          String string = valueMeta.getString( valueData );
          // Have the database dialect do the quoting.
          // This also adds the single quotes around the string (thanks to PostgreSQL)
          //
          string = quoteSqlString( string );
          ins.append( string );
          break;
        case IValueMeta.TYPE_DATE:
          Date date = valueMeta.getDate( valueData );

          if ( Utils.isEmpty( dateFormat ) ) {
            ins.append( "'" + valueMeta.getString( valueData ) + "'" );
          } else {
            try {
              java.text.SimpleDateFormat formatter = new java.text.SimpleDateFormat( dateFormat );
              ins.append( "'" + formatter.format( date ) + "'" );
            } catch ( Exception e ) {
              throw new HopValueException( "Error : ", e );
            }
          }
          break;
        default:
          ins.append( valueMeta.getString( valueData ) );
          break;
      }
    }

    return ins.toString();
  }

  protected String getFieldnameProtector() {
    return FIELDNAME_PROTECTOR;
  }

  /**
   * Sanitize a string for usage as a field name
   * <ul>
   * <li>Append an underscore to any field name that matches a reserved word</li>
   * <li>Replaces spaces with underscores</li>
   * <li>Prefixes a string with underscore that begins with a number</li>
   * </ul>
   *
   * @param fieldname value to sanitize
   * @return
   */
  @Override
  public String getSafeFieldname( String fieldname ) {
    StringBuilder newName = new StringBuilder( fieldname.length() );

    char[] protectors = getFieldnameProtector().toCharArray();

    // alpha numerics , underscores, field protectors only
    for ( int idx = 0; idx < fieldname.length(); idx++ ) {
      char c = fieldname.charAt( idx );
      if ( ( c >= 'a' && c <= 'z' ) || ( c >= 'A' && c <= 'Z' ) || ( c >= '0' && c <= '9' ) || ( c == '_' ) ) {
        newName.append( c );
      } else if ( c == ' ' ) {
        newName.append( '_' );
      } else {
        // allow protectors
        for ( char protector : protectors ) {
          if ( c == protector ) {
            newName.append( c );
          }
        }
      }
      // else {
      // swallow this character
      // }
    }
    fieldname = newName.toString();

    // don't allow reserved words
    for ( String reservedWord : getReservedWords() ) {
      if ( fieldname.equalsIgnoreCase( reservedWord ) ) {
        fieldname = fieldname + getFieldnameProtector();
      }
    }

    fieldname = fieldname.replace( " ", getFieldnameProtector() );

    // can't start with a number
    if ( fieldname.matches( "^[0-9].*" ) ) {
      fieldname = getFieldnameProtector() + fieldname;
    }
    return fieldname;
  }

  /**
   * @return string with the no max value sequence option.
   */
  @Override
  public String getSequenceNoMaxValueOption() {
    return "NOMAXVALUE";
  }

  /**
   * @return true if the database supports autoGeneratedKeys
   */
  @Override
  public boolean supportsAutoGeneratedKeys() {
    return true;
  }


  /**
   * Customizes the IValueMeta defined in the base
   *
   * @param v     the determined iValueMeta
   * @param rm    the sql result
   * @param index the index to the column
   * @return IValueMeta customized with the data base specific types
   */
  @Override
  public IValueMeta customizeValueFromSqlType(IValueMeta v, java.sql.ResultSetMetaData rm, int index )
    throws SQLException {
    return null;
  }

  /**
   * Customizes the IValueMeta defined in the base
   *
   * @return String the create table statement
   */
  @Override
  public String getCreateTableStatement() {
    return "CREATE TABLE ";
  }

  /**
   * Forms drop table statement.
   * This standard construct syntax is not legal for certain RDBMSs,
   * and should be overridden according to their specifics.
   *
   * @param tableName Name of the table to drop
   * @return Standard drop table statement
   */
  @Override
  public String getDropTableIfExistsStatement( String tableName ) {
    return "DROP TABLE IF EXISTS " + tableName;
  }

  @Override
  public boolean fullExceptionLog( Exception e ) {
    return true;
  }

  @Override
  public void addDefaultOptions() {
  }

  @Override
  public void addAttribute( String attributeId, String value ) {
    attributes.put( attributeId, value );
  }

  @Override
  public String getAttribute( String attributeId, String defaultValue ) {
    return getAttributeProperty( attributeId, defaultValue );
  }
}
