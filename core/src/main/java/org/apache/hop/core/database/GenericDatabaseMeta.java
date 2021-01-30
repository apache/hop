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
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IValueMeta;

import java.sql.SQLException;
import java.util.Map;

/**
 * Contains Generic Database Connection information through static final members
 *
 * @author Matt
 * @since 11-mrt-2005
 */
@DatabaseMetaPlugin(
  type = "GENERIC",
  typeDescription = "Generic database"
)
@GuiPlugin(description="Generic database GUI Plugin")
public class GenericDatabaseMeta extends BaseDatabaseMeta implements IDatabase {
  public static final String ATRRIBUTE_CUSTOM_DRIVER_CLASS = "CUSTOM_DRIVER_CLASS";
  public static final String DATABASE_DIALECT_ID = "DATABASE_DIALECT_ID";
  private IDatabase databaseDialect = null;

  @GuiWidgetElement( id = "hostname", type = GuiElementType.NONE, parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, ignored = true )
  protected String hostname;
  @GuiWidgetElement( id = "port", type = GuiElementType.NONE, parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, ignored = true )
  protected String port;
  @GuiWidgetElement( id = "databaseName", type = GuiElementType.NONE, parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, ignored = true )
  protected String databaseName;

  @GuiWidgetElement(
    id = "driverClass",
    order = "10",
    label = "i18n:org.apache.hop.ui.core.database:DatabaseDialog.label.DriverClass",
    type = GuiElementType.TEXT,
    variables = true,
    parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID )
  protected String driverClass;

  /**
   * @param driverClass The driverClass to set
   */
  public void setDriverClass( String driverClass ) {
    getAttributes().put( ATRRIBUTE_CUSTOM_DRIVER_CLASS, driverClass );
  }

  @Override
  public String getDriverClass() {
    return getAttributeProperty( ATRRIBUTE_CUSTOM_DRIVER_CLASS, "" );
  }

  @Override
  public void addAttribute( String attributeId, String value ) {
    super.addAttribute( attributeId, value );
    if ( DATABASE_DIALECT_ID.equals( attributeId ) ) {
      resolveDialect( value );
    }
  }

  @Override
  public int[] getAccessTypeList() {
    return new int[] {
      DatabaseMeta.TYPE_ACCESS_NATIVE };
  }

  /**
   * @see IDatabase#getNotFoundTK(boolean)
   */
  @Override
  public int getNotFoundTK( boolean useAutoIncrement ) {
    if ( supportsAutoInc() && useAutoIncrement ) {
      return 1;
    }
    return super.getNotFoundTK( useAutoIncrement );
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) {
    return manualUrl;
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

  /**
   * @param tableName The table to be truncated.
   * @return The SQL statement to truncate a table: remove all rows from it without a transaction
   */
  @Override
  public String getTruncateTableStatement( String tableName ) {
    if ( databaseDialect != null ) {
      return databaseDialect.getTruncateTableStatement( tableName );
    }
    return "DELETE FROM " + tableName;
  }

  /**
   * Generates the SQL statement to add a column to the specified table For this generic type, i set it to the most
   * common possibility.
   *
   * @param tableName   The table to add
   * @param v           The column defined as a value
   * @param tk          the name of the technical key field
   * @param useAutoIncrement whether or not this field uses auto increment
   * @param pk          the name of the primary key field
   * @param semicolon   whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to add a column to the specified table
   */
  @Override
  public String getAddColumnStatement( String tableName, IValueMeta v, String tk, boolean useAutoIncrement,
                                       String pk, boolean semicolon ) {
    if ( databaseDialect != null ) {
      return databaseDialect.getAddColumnStatement( tableName, v, tk, useAutoIncrement, pk, semicolon );
    }

    return "ALTER TABLE " + tableName + " ADD " + getFieldDefinition( v, tk, pk, useAutoIncrement, true, false );
  }

  /**
   * Generates the SQL statement to modify a column in the specified table
   *
   * @param tableName   The table to add
   * @param v           The column defined as a value
   * @param tk          the name of the technical key field
   * @param useAutoIncrement whether or not this field uses auto increment
   * @param pk          the name of the primary key field
   * @param semicolon   whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to modify a column in the specified table
   */
  @Override
  public String getModifyColumnStatement( String tableName, IValueMeta v, String tk, boolean useAutoIncrement,
                                          String pk, boolean semicolon ) {
    if ( databaseDialect != null ) {
      return databaseDialect.getModifyColumnStatement( tableName, v, tk, useAutoIncrement, pk, semicolon );
    }
    return "ALTER TABLE " + tableName + " MODIFY " + getFieldDefinition( v, tk, pk, useAutoIncrement, true, false );
  }

  @Override
  public String getFieldDefinition( IValueMeta v, String tk, String pk, boolean useAutoIncrement,
                                    boolean addFieldName, boolean addCr ) {

    if ( databaseDialect != null ) {
      return databaseDialect.getFieldDefinition( v, tk, pk, useAutoIncrement, addFieldName, addCr );
    }

    String retval = "";

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    if ( addFieldName ) {
      retval += fieldname + " ";
    }

    int type = v.getType();
    switch ( type ) {
      case IValueMeta.TYPE_TIMESTAMP:
      case IValueMeta.TYPE_DATE:
        retval += "TIMESTAMP";
        break;
      case IValueMeta.TYPE_BOOLEAN:
        if ( supportsBooleanDataType() ) {
          retval += "BOOLEAN";
        } else {
          retval += "CHAR(1)";
        }
        break;
      case IValueMeta.TYPE_NUMBER:
      case IValueMeta.TYPE_INTEGER:
      case IValueMeta.TYPE_BIGNUMBER:
        if ( fieldname.equalsIgnoreCase( tk ) || // Technical key
          fieldname.equalsIgnoreCase( pk ) // Primary key
        ) {
          retval += "BIGSERIAL";
        } else {
          if ( length > 0 ) {
            if ( precision > 0 || length > 18 ) {
              retval += "NUMERIC(" + length + ", " + precision + ")";
            } else {
              if ( length > 9 ) {
                retval += "BIGINT";
              } else {
                if ( length < 5 ) {
                  retval += "SMALLINT";
                } else {
                  retval += "INTEGER";
                }
              }
            }

          } else {
            retval += "DOUBLE PRECISION";
          }
        }
        break;
      case IValueMeta.TYPE_STRING:
        if ( length >= DatabaseMeta.CLOB_LENGTH ) {
          retval += "TEXT";
        } else {
          retval += "VARCHAR";
          if ( length > 0 ) {
            retval += "(" + length;
          } else {
            retval += "("; // Maybe use some default DB String length?
          }
          retval += ")";
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

  /**
   * Most databases allow you to retrieve result metadata by preparing a SELECT statement.
   *
   * @return true if the database supports retrieval of query metadata from a prepared statement. False if the query
   * needs to be executed first.
   */
  @Override
  public boolean supportsPreparedStatementMetadataRetrieval() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsPreparedStatementMetadataRetrieval();
    }
    return false;
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
    if ( databaseDialect != null ) {
      return databaseDialect.getSqlInsertAutoIncUnknownDimensionRow( schemaTable, keyField, versionField );
    }
    return "insert into " + schemaTable + "(" + versionField + ") values (1)";
  }

  public void setDatabaseDialect( String databaseDialect ) {
    super.addAttribute( DATABASE_DIALECT_ID, databaseDialect );
    resolveDialect( databaseDialect );
  }

  public String getDatabaseDialect() {
    return super.getAttribute( DATABASE_DIALECT_ID, getPluginName() );
  }

  private void resolveDialect( String dialectName ) {
    if ( dialectName == null ) {
      return;
    }
    if ( dialectName.equals( getPluginName() ) ) {
      databaseDialect = null;
    } else {
      IDatabase[] dialects = DatabaseMeta.getDatabaseInterfaces();
      for ( IDatabase dialect : dialects ) {
        if ( dialectName.equals( dialect.getPluginName() ) ) {
          databaseDialect = dialect;
          break;
        }
      }
    }
  }

  @Override
  public String[] getReservedWords() {
    if ( databaseDialect != null ) {
      return databaseDialect.getReservedWords();
    }
    return super.getReservedWords();
  }

  @Override
  public String getEndQuote() {
    if ( databaseDialect != null ) {
      return databaseDialect.getEndQuote();
    }
    return super.getEndQuote();
  }


  @Override
  public String getFunctionSum() {
    if ( databaseDialect != null ) {
      return databaseDialect.getFunctionSum();
    }
    return super.getFunctionSum();
  }

  @Override
  public String getFunctionAverage() {
    if ( databaseDialect != null ) {
      return databaseDialect.getFunctionAverage();
    }
    return super.getFunctionAverage();
  }

  @Override
  public String getFunctionMinimum() {
    if ( databaseDialect != null ) {
      return databaseDialect.getFunctionMinimum();
    }
    return super.getFunctionMinimum();
  }

  @Override
  public String getFunctionMaximum() {
    if ( databaseDialect != null ) {
      return databaseDialect.getFunctionMaximum();
    }
    return super.getFunctionMaximum();
  }

  @Override
  public String getFunctionCount() {
    if ( databaseDialect != null ) {
      return databaseDialect.getFunctionCount();
    }
    return super.getFunctionCount();
  }

  @Override
  public String getSqlQueryFields( String tableName ) {
    if ( databaseDialect != null ) {
      return databaseDialect.getSqlQueryFields( tableName );
    }
    return super.getSqlQueryFields( tableName );
  }

  @Override
  public String getSqlColumnExists( String columnname, String tableName ) {
    if ( databaseDialect != null ) {
      return databaseDialect.getSqlColumnExists( columnname, tableName );
    }
    return super.getSqlColumnExists( columnname, tableName );
  }

  @Override
  public String getSqlTableExists( String tableName ) {
    if ( databaseDialect != null ) {
      return databaseDialect.getSqlTableExists( tableName );
    }
    return super.getSqlTableExists( tableName );
  }

  @Override
  public String getLimitClause( int nrRows ) {
    if ( databaseDialect != null ) {
      return databaseDialect.getLimitClause( nrRows );
    }
    return super.getLimitClause( nrRows );
  }

  @Override
  public String getSelectCountStatement( String tableName ) {
    if ( databaseDialect != null ) {
      return databaseDialect.getSelectCountStatement( tableName );
    }
    return super.getSelectCountStatement( tableName );
  }

  @Override
  public String getSqlUnlockTables( String[] tableName ) {
    if ( databaseDialect != null ) {
      return databaseDialect.getSqlUnlockTables( tableName );
    }
    return super.getSqlUnlockTables( tableName );
  }

  @Override
  public String getSequenceNoMaxValueOption() {
    if ( databaseDialect != null ) {
      return databaseDialect.getSequenceNoMaxValueOption();
    }
    return super.getSequenceNoMaxValueOption();
  }

  @Override
  public boolean useSchemaNameForTableList() {
    if ( databaseDialect != null ) {
      return databaseDialect.useSchemaNameForTableList();
    }
    return super.useSchemaNameForTableList();
  }

  @Override
  public boolean supportsViews() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsViews();
    }
    return super.supportsViews();
  }

  @Override
  public boolean supportsTimeStampToDateConversion() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsTimeStampToDateConversion();
    }
    return super.supportsTimeStampToDateConversion();
  }

  @Override
  public String getCreateTableStatement() {
    if ( databaseDialect != null ) {
      return databaseDialect.getCreateTableStatement();
    }
    return super.getCreateTableStatement();
  }

  @Override
  public boolean supportsAutoGeneratedKeys() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsAutoGeneratedKeys();
    }
    return super.supportsAutoGeneratedKeys();
  }

  @Override
  public String getSafeFieldname( String fieldname ) {
    if ( databaseDialect != null ) {
      return databaseDialect.getSafeFieldname( fieldname );
    }
    return super.getSafeFieldname( fieldname );
  }

  @Override
  public void setSupportsTimestampDataType( boolean b ) {
    if ( databaseDialect != null ) {
      databaseDialect.setSupportsTimestampDataType( b );
    }
    super.setSupportsTimestampDataType( b );
  }

  @Override
  public boolean supportsTimestampDataType() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsTimestampDataType();
    }
    return super.supportsTimestampDataType();
  }

  @Override
  public boolean supportsResultSetMetadataRetrievalOnly() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsResultSetMetadataRetrievalOnly();
    }
    return super.supportsResultSetMetadataRetrievalOnly();
  }

  @Override
  public String getSqlValue( IValueMeta valueMeta, Object valueData, String dateFormat ) throws
    HopValueException {
    if ( databaseDialect != null ) {
      return databaseDialect.getSqlValue( valueMeta, valueData, dateFormat );
    }
    return super.getSqlValue( valueMeta, valueData, dateFormat );
  }

  @Override
  public IValueMeta customizeValueFromSqlType(IValueMeta v, java.sql.ResultSetMetaData rm, int index )
    throws SQLException {
    if ( databaseDialect != null ) {
      return databaseDialect.customizeValueFromSqlType( v, rm, index );
    }
    return super.customizeValueFromSqlType( v, rm, index );
  }

  @Override
  public boolean isMySqlVariant() {
    if ( databaseDialect != null ) {
      return databaseDialect.isMySqlVariant();
    }
    return super.isMySqlVariant();
  }

  @Override
  public String generateColumnAlias( int columnIndex, String suggestedName ) {
    if ( databaseDialect != null ) {
      return databaseDialect.generateColumnAlias( columnIndex, suggestedName );
    }
    return super.generateColumnAlias( columnIndex, suggestedName );
  }

  @Override
  public String quoteSqlString(String string ) {
    if ( databaseDialect != null ) {
      return databaseDialect.quoteSqlString( string );
    }
    return super.quoteSqlString( string );
  }

  @Override
  public boolean isExplorable() {
    if ( databaseDialect != null ) {
      return databaseDialect.isExplorable();
    }
    return super.isExplorable();
  }

  @Override
  public int getMaxColumnsInIndex() {
    if ( databaseDialect != null ) {
      return databaseDialect.getMaxColumnsInIndex();
    }
    return super.getMaxColumnsInIndex();
  }

  @Override
  public String getSqlListOfSchemas() {
    if ( databaseDialect != null ) {
      return databaseDialect.getSqlListOfSchemas();
    }
    return super.getSqlListOfSchemas();
  }

  @Override
  public boolean supportsNewLinesInSql() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsNewLinesInSql();
    }
    return super.supportsNewLinesInSql();
  }

  @Override
  public boolean isSystemTable( String tableName ) {
    if ( databaseDialect != null ) {
      return databaseDialect.isSystemTable( tableName );
    }
    return super.isSystemTable( tableName );
  }

  @Override
  public boolean isDisplaySizeTwiceThePrecision() {
    if ( databaseDialect != null ) {
      return databaseDialect.isDisplaySizeTwiceThePrecision();
    }
    return super.isDisplaySizeTwiceThePrecision();
  }

  @Override
  public boolean requiresCastToVariousForIsNull() {
    if ( databaseDialect != null ) {
      return databaseDialect.requiresCastToVariousForIsNull();
    }
    return super.requiresCastToVariousForIsNull();
  }

  @Override
  public boolean requiresCreateTablePrimaryKeyAppend() {
    if ( databaseDialect != null ) {
      return databaseDialect.requiresCreateTablePrimaryKeyAppend();
    }
    return super.requiresCreateTablePrimaryKeyAppend();
  }

  @Override
  public boolean supportsSequenceNoMaxValueOption() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsSequenceNoMaxValueOption();
    }
    return super.supportsSequenceNoMaxValueOption();
  }

  @Override
  public void setUsingDoubleDecimalAsSchemaTableSeparator( boolean useDoubleDecimalSeparator ) {
    if ( databaseDialect != null ) {
      databaseDialect.setUsingDoubleDecimalAsSchemaTableSeparator( useDoubleDecimalSeparator );
    }
    super.setUsingDoubleDecimalAsSchemaTableSeparator( useDoubleDecimalSeparator );
  }

  @Override
  public boolean isUsingDoubleDecimalAsSchemaTableSeparator() {
    if ( databaseDialect != null ) {
      return databaseDialect.isUsingDoubleDecimalAsSchemaTableSeparator();
    }
    return super.isUsingDoubleDecimalAsSchemaTableSeparator();
  }

  @Override
  public void setForcingIdentifiersToUpperCase( boolean forceUpperCase ) {
    if ( databaseDialect != null ) {
      databaseDialect.setForcingIdentifiersToUpperCase( forceUpperCase );
    }
    super.setForcingIdentifiersToUpperCase( forceUpperCase );
  }

  @Override
  public boolean isForcingIdentifiersToUpperCase() {
    if ( databaseDialect != null ) {
      return databaseDialect.isForcingIdentifiersToUpperCase();
    }
    return super.isForcingIdentifiersToUpperCase();
  }

  @Override
  public void setForcingIdentifiersToLowerCase( boolean forceUpperCase ) {
    if ( databaseDialect != null ) {
      databaseDialect.setForcingIdentifiersToLowerCase( forceUpperCase );
    }
    super.setForcingIdentifiersToLowerCase( forceUpperCase );
  }

  @Override
  public boolean isForcingIdentifiersToLowerCase() {
    if ( databaseDialect != null ) {
      return databaseDialect.isForcingIdentifiersToLowerCase();
    }
    return super.isForcingIdentifiersToLowerCase();
  }

  @Override
  public void setQuoteAllFields( boolean quoteAllFields ) {
    if ( databaseDialect != null ) {
      databaseDialect.setQuoteAllFields( quoteAllFields );
    }
    super.setQuoteAllFields( quoteAllFields );
  }

  @Override
  public boolean isQuoteAllFields() {
    if ( databaseDialect != null ) {
      return databaseDialect.isQuoteAllFields();
    }
    return super.isQuoteAllFields();
  }

  @Override
  public void setStreamingResults( boolean useStreaming ) {
    if ( databaseDialect != null ) {
      databaseDialect.setStreamingResults( useStreaming );
    }
    super.setStreamingResults( useStreaming );
  }

  @Override
  public boolean isStreamingResults() {
    if ( databaseDialect != null ) {
      return databaseDialect.isStreamingResults();
    }
    return super.isStreamingResults();
  }

  @Override
  public boolean supportsSetMaxRows() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsSetMaxRows();
    }
    return super.supportsSetMaxRows();
  }

  @Override
  public boolean supportsGetBlob() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsGetBlob();
    }
    return super.supportsGetBlob();
  }

  @Override
  public boolean isDefaultingToUppercase() {
    if ( databaseDialect != null ) {
      return databaseDialect.isDefaultingToUppercase();
    }
    return super.isDefaultingToUppercase();
  }

  @Override
  public void setPreserveReservedCase( boolean b ) {
    if ( databaseDialect != null ) {
      databaseDialect.setPreserveReservedCase( b );
    }
    super.setPreserveReservedCase( b );
  }

  @Override
  public boolean preserveReservedCase() {
    if ( databaseDialect != null ) {
      return databaseDialect.preserveReservedCase();
    }
    return super.preserveReservedCase();
  }

  @Override
  public void setSupportsBooleanDataType( boolean b ) {
    if ( databaseDialect != null ) {
      databaseDialect.setSupportsBooleanDataType( b );
    }
    super.setSupportsBooleanDataType( b );
  }

  @Override
  public boolean supportsBooleanDataType() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsBooleanDataType();
    }
    return super.supportsBooleanDataType();
  }

  @Override
  public boolean supportsBatchUpdates() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsBatchUpdates();
    }
    return super.supportsBatchUpdates();
  }

  @Override
  public String getSqlLockTables( String[] tableNames ) {
    if ( databaseDialect != null ) {
      return databaseDialect.getSqlLockTables( tableNames );
    }
    return super.getSqlLockTables( tableNames );
  }

  @Override
  public boolean supportsFloatRoundingOnUpdate() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsFloatRoundingOnUpdate();
    }
    return super.supportsFloatRoundingOnUpdate();
  }

  @Override
  public boolean supportsSynonyms() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsSynonyms();
    }
    return super.supportsSynonyms();
  }

  @Override
  public String[] getSynonymTypes() {
    if ( databaseDialect != null ) {
      return databaseDialect.getSynonymTypes();
    }
    return super.getSynonymTypes();
  }

  @Override
  public String[] getViewTypes() {
    if ( databaseDialect != null ) {
      return databaseDialect.getViewTypes();
    }
    return super.getViewTypes();
  }

  @Override
  public String[] getTableTypes() {
    if ( databaseDialect != null ) {
      return databaseDialect.getTableTypes();
    }
    return super.getTableTypes();
  }

  @Override
  public String getStartQuote() {
    if ( databaseDialect != null ) {
      return databaseDialect.getStartQuote();
    }
    return super.getStartQuote();
  }

  @Override
  public boolean quoteReservedWords() {
    if ( databaseDialect != null ) {
      return databaseDialect.quoteReservedWords();
    }
    return super.quoteReservedWords();
  }

  @Override
  public String getDropColumnStatement( String tableName, IValueMeta v, String tk, boolean useAutoIncrement,
                                        String pk, boolean semicolon ) {
    if ( databaseDialect != null ) {
      return databaseDialect.getDropColumnStatement( tableName, v, tk, useAutoIncrement, pk, semicolon );
    }
    return super.getDropColumnStatement( tableName, v, tk, useAutoIncrement, pk, semicolon );
  }

  @Override
  public int getMaxVARCHARLength() {
    if ( databaseDialect != null ) {
      return databaseDialect.getMaxVARCHARLength();
    }
    return super.getMaxVARCHARLength();
  }

  @Override
  public int getMaxTextFieldLength() {
    if ( databaseDialect != null ) {
      return databaseDialect.getMaxTextFieldLength();
    }
    return super.getMaxTextFieldLength();
  }

  @Override
  public String getSchemaTableCombination( String schemaName, String tablePart ) {
    if ( databaseDialect != null ) {
      return databaseDialect.getSchemaTableCombination( schemaName, tablePart );
    }
    return super.getSchemaTableCombination( schemaName, tablePart );
  }

  @Override
  public Map<String, String> getDefaultOptions() {
    if ( databaseDialect != null ) {
      return databaseDialect.getDefaultOptions();
    }
    return super.getDefaultOptions();
  }

  @Override
  public Map<String, String> getExtraOptions() {
    if ( databaseDialect != null ) {
      return databaseDialect.getExtraOptions();
    }
    return super.getExtraOptions();
  }

  @Override
  public void addExtraOption( String databaseTypeCode, String option, String value ) {
    if ( databaseDialect != null ) {
      databaseDialect.addExtraOption( databaseTypeCode, option, value );
    }
    super.addExtraOption( databaseTypeCode, option, value );
  }

  @Override
  public String getExtraOptionSeparator() {
    if ( databaseDialect != null ) {
      return databaseDialect.getExtraOptionSeparator();
    }
    return super.getExtraOptionSeparator();
  }

  @Override
  public String getExtraOptionValueSeparator() {
    if ( databaseDialect != null ) {
      return databaseDialect.getExtraOptionValueSeparator();
    }
    return super.getExtraOptionValueSeparator();
  }

  @Override
  public String getExtraOptionIndicator() {
    if ( databaseDialect != null ) {
      return databaseDialect.getExtraOptionIndicator();
    }
    return super.getExtraOptionIndicator();
  }

  @Override
  public boolean supportsOptionsInURL() {
    if ( databaseDialect != null ) {
      return databaseDialect.supportsOptionsInURL();
    }
    return super.supportsOptionsInURL();
  }

  @Override
  public String getExtraOptionsHelpText() {
    if ( databaseDialect != null ) {
      return databaseDialect.getExtraOptionsHelpText();
    }
    return super.getExtraOptionsHelpText();
  }
}
