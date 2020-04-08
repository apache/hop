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

package org.apache.hop.core.logging;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseLogTable {
  public static final String XML_TAG = "field";

  public static String PROP_LOG_TABLE_CONNECTION_NAME = "_LOG_TABLE_CONNECTION_NAME";
  public static String PROP_LOG_TABLE_SCHEMA_NAME = "_LOG_TABLE_SCHEMA_NAME";
  public static String PROP_LOG_TABLE_TABLE_NAME = "_LOG_TABLE_TABLE_NAME";

  public static String PROP_LOG_TABLE_FIELD_ID = "_LOG_TABLE_FIELD_ID";
  public static String PROP_LOG_TABLE_FIELD_NAME = "_LOG_TABLE_FIELD_NAME";
  public static String PROP_LOG_TABLE_FIELD_ENABLED = "_LOG_TABLE_FIELD_ENABLED";
  public static String PROP_LOG_TABLE_FIELD_SUBJECT = "_LOG_TABLE_FIELD_SUBJECT";

  public static String PROP_LOG_TABLE_INTERVAL = "LOG_TABLE_INTERVAL";
  public static String PROP_LOG_TABLE_SIZE_LIMIT = "LOG_TABLE_SIZE_LIMIT";
  public static String PROP_LOG_TABLE_TIMEOUT_DAYS = "_LOG_TABLE_TIMEOUT_IN_DAYS";

  protected IVariables variables;
  protected IMetaStore metaStore;

  protected String connectionName;

  protected String schemaName;
  protected String tableName;
  protected String timeoutInDays;

  protected List<LogTableField> fields;

  public BaseLogTable( IVariables variables, IMetaStore metaStore, String connectionName,
                       String schemaName, String tableName ) {
    this.variables = variables;
    this.metaStore = metaStore;
    this.connectionName = connectionName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.fields = new ArrayList<LogTableField>();
  }

  public void replaceMeta( BaseLogTable baseLogTable ) {
    this.variables = baseLogTable.variables;
    this.metaStore = baseLogTable.metaStore;
    this.connectionName = baseLogTable.connectionName;
    this.schemaName = baseLogTable.schemaName;
    this.tableName = baseLogTable.tableName;
    this.timeoutInDays = baseLogTable.timeoutInDays;

    fields.clear();
    for ( LogTableField field : baseLogTable.fields ) {
      try {
        fields.add( (LogTableField) field.clone() );
      } catch ( CloneNotSupportedException e ) {
        throw new RuntimeException( "Clone problem with the base log table", e );
      }
    }
  }

  public String toString() {
    if ( isDefined() ) {
      return getDatabaseMeta().getName() + "-" + getActualTableName();
    }
    return super.toString();
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  public abstract String getLogTableCode();

  public abstract String getConnectionNameVariable();

  public abstract String getSchemaNameVariable();

  public abstract String getTableNameVariable();

  /**
   * @return the databaseMeta
   */
  public DatabaseMeta getDatabaseMeta() {

    String name = getActualConnectionName();
    if ( name == null ) {
      return null;
    }
    if ( metaStore == null ) {
      return null;
    }

    try {
      return DatabaseMeta.createFactory( metaStore ).loadElement( name );
    } catch ( MetaStoreException e ) {
      throw new RuntimeException( "Unable to load database connection '" + name + "'", e );
    }
  }

  /**
   * @return the connectionName
   */
  public String getActualConnectionName() {
    String name = variables.environmentSubstitute( connectionName );
    if ( Utils.isEmpty( name ) ) {
      name = variables.getVariable( getConnectionNameVariable() );
    }
    if ( Utils.isEmpty( name ) ) {
      return null;
    } else {
      return name;
    }
  }

  /**
   * @return the schemaName
   */
  public String getActualSchemaName() {
    if ( !Utils.isEmpty( schemaName ) ) {
      return variables.environmentSubstitute( schemaName );
    }

    String name = variables.getVariable( getSchemaNameVariable() );
    if ( Utils.isEmpty( name ) ) {
      return null;
    } else {
      return name;
    }
  }

  /**
   * @param schemaName the schemaName to set
   */
  public void setSchemaName( String schemaName ) {
    this.schemaName = schemaName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  /**
   * @return the tableName
   */
  public String getActualTableName() {
    if ( !Utils.isEmpty( tableName ) ) {
      return variables.environmentSubstitute( tableName );
    }

    String name = variables.getVariable( getTableNameVariable() );
    if ( Utils.isEmpty( name ) ) {
      return null;
    } else {
      return name;
    }
  }

  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName the tableName to set
   */
  public void setTableName( String tableName ) {
    this.tableName = tableName;
  }

  public String getQuotedSchemaTableCombination() {
    return getDatabaseMeta().getQuotedSchemaTableCombination( getActualSchemaName(), getActualTableName() );
  }

  /**
   * @return the fields
   */
  public List<LogTableField> getFields() {
    return fields;
  }

  /**
   * @param fields the fields to set
   */
  public void setFields( List<LogTableField> fields ) {
    this.fields = fields;
  }

  /**
   * Find a log table field in this log table definition. Use the id of the field to do the lookup.
   *
   * @param id the id of the field to search for
   * @return the log table field or null if nothing was found.
   */
  public LogTableField findField( String id ) {
    for ( LogTableField field : fields ) {
      if ( field.getId().equals( id ) ) {
        return field;
      }
    }
    return null;
  }

  /**
   * Get the subject of a field with the specified ID
   *
   * @param id
   * @return the subject or null if no field could be find with the specified id
   */
  public Object getSubject( String id ) {
    LogTableField field = findField( id );
    if ( field == null ) {
      return null;
    }
    return field.getSubject();
  }

  /**
   * Return the subject in the form of a string for the specified ID.
   *
   * @param id the id of the field to look for.
   * @return the string of the subject (name of transform) or null if nothing was found.
   */
  public String getSubjectString( String id ) {
    LogTableField field = findField( id );
    if ( field == null ) {
      return null;
    }
    if ( field.getSubject() == null ) {
      return null;
    }
    return field.getSubject().toString();
  }

  public boolean containsKeyField() {
    for ( LogTableField field : fields ) {
      if ( field.isKey() ) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return the field that represents the log date field or null if none was defined.
   */
  public LogTableField getLogDateField() {
    for ( LogTableField field : fields ) {
      if ( field.isLogDateField() ) {
        return field;
      }
    }
    return null;
  }

  /**
   * @return the field that represents the key to this logging table (batch id etc)
   */
  public LogTableField getKeyField() {
    for ( LogTableField field : fields ) {
      if ( field.isKey() ) {
        return field;
      }
    }
    return null;
  }

  /**
   * @return the field that represents the logging text (or null if none is found)
   */
  public LogTableField getLogField() {
    for ( LogTableField field : fields ) {
      if ( field.isLogField() ) {
        return field;
      }
    }
    return null;
  }

  /**
   * @return the field that represents the status (or null if none is found)
   */
  public LogTableField getStatusField() {
    for ( LogTableField field : fields ) {
      if ( field.isStatusField() ) {
        return field;
      }
    }
    return null;
  }

  /**
   * @return the field that represents the number of errors (or null if none is found)
   */
  public LogTableField getErrorsField() {
    for ( LogTableField field : fields ) {
      if ( field.isErrorsField() ) {
        return field;
      }
    }
    return null;
  }

  /**
   * @return the field that represents the name of the object that is being used (or null if none is found)
   */
  public LogTableField getNameField() {
    for ( LogTableField field : fields ) {
      if ( field.isNameField() ) {
        return field;
      }
    }
    return null;
  }

  protected String getFieldsXML() {
    StringBuilder retval = new StringBuilder();

    for ( LogTableField field : fields ) {
      retval.append( "        " ).append( XmlHandler.openTag( XML_TAG ) ).append( Const.CR );

      retval.append( "          " ).append( XmlHandler.addTagValue( "id", field.getId() ) );
      retval.append( "          " ).append( XmlHandler.addTagValue( "enabled", field.isEnabled() ) );
      retval.append( "          " ).append( XmlHandler.addTagValue( "name", field.getFieldName() ) );
      if ( field.isSubjectAllowed() ) {
        retval.append( "          " ).append( XmlHandler.addTagValue( "subject",
          field.getSubject() == null ? null : field.getSubject().toString() ) );
      }

      retval.append( "        " ).append( XmlHandler.closeTag( XML_TAG ) ).append( Const.CR );
    }

    return retval.toString();
  }

  public void loadFieldsXML( Node node ) {
    int nr = XmlHandler.countNodes( node, BaseLogTable.XML_TAG );
    for ( int i = 0; i < nr; i++ ) {
      Node fieldNode = XmlHandler.getSubNodeByNr( node, BaseLogTable.XML_TAG, i );
      String id = XmlHandler.getTagValue( fieldNode, "id" );
      LogTableField field = findField( id );
      if ( field == null && i < fields.size() ) {
        field = fields.get( i ); // backward compatible until we go GA
      }
      if ( field != null ) {
        field.setFieldName( XmlHandler.getTagValue( fieldNode, "name" ) );
        field.setEnabled( "Y".equalsIgnoreCase( XmlHandler.getTagValue( fieldNode, "enabled" ) ) );
      }
    }
  }

  public boolean isDefined() {
    return getDatabaseMeta() != null && !Utils.isEmpty( getActualTableName() );
  }

  /**
   * @return the timeoutInDays
   */
  public String getTimeoutInDays() {
    return timeoutInDays;
  }

  /**
   * @param timeoutInDays the timeoutInDays to set
   */
  public void setTimeoutInDays( String timeoutInDays ) {
    this.timeoutInDays = timeoutInDays;
  }

  /**
   * @return the connectionName
   */
  public String getConnectionName() {
    return connectionName;
  }

  /**
   * @param connectionName the connectionName to set
   */
  public void setConnectionName( String connectionName ) {
    this.connectionName = connectionName;
  }

  @VisibleForTesting
  protected String getLogBuffer( IVariables variables, String logChannelId, LogStatus status, String limit ) {

    LoggingBuffer loggingBuffer = HopLogStore.getAppender();
    // if workflow is starting, then remove all previous events from buffer with that workflow logChannelId.
    // Prevents recursive workflow calls logging issue.
    if ( status.getStatus().equalsIgnoreCase( String.valueOf( LogStatus.START ) ) ) {
      loggingBuffer.removeChannelFromBuffer( logChannelId );
    }

    StringBuffer buffer = loggingBuffer.getBuffer( logChannelId, true );

    if ( Utils.isEmpty( limit ) ) {
      String defaultLimit = variables.getVariable( Const.HOP_LOG_SIZE_LIMIT, null );
      if ( !Utils.isEmpty( defaultLimit ) ) {
        limit = defaultLimit;
      }
    }

    // See if we need to limit the amount of rows
    //
    int nrLines = Utils.isEmpty( limit ) ? -1 : Const.toInt( variables.environmentSubstitute( limit ), -1 );

    if ( nrLines > 0 ) {
      int start = buffer.length() - 1;
      for ( int i = 0; i < nrLines && start > 0; i++ ) {
        start = buffer.lastIndexOf( Const.CR, start - 1 );
      }
      if ( start > 0 ) {
        buffer.delete( 0, start + Const.CR.length() );
      }
    }

    return buffer.append( Const.CR + status.getStatus().toUpperCase() + Const.CR ).toString();
  }

  // PDI-7070: implement equals for comparison of workflow/pipeline log table to its parent log table
  @Override
  public boolean equals( Object obj ) {
    if ( obj == null || !( obj instanceof BaseLogTable ) ) {
      return false;
    }
    BaseLogTable blt = (BaseLogTable) obj;

    // Get actual names for comparison
    String cName = this.getActualConnectionName();
    String sName = this.getActualSchemaName();
    String tName = this.getActualTableName();

    return ( ( cName == null ? blt.getActualConnectionName() == null : cName
      .equals( blt.getActualConnectionName() ) )
      && ( sName == null ? blt.getActualSchemaName() == null : sName.equals( blt.getActualSchemaName() ) )
      && ( tName == null ? blt.getActualTableName() == null : tName.equals( blt.getActualTableName() ) ) );
  }

  public void setAllGlobalParametersToNull() {
    boolean clearGlobalVariables = Boolean.valueOf( System.getProperties().getProperty( Const.HOP_GLOBAL_LOG_VARIABLES_CLEAR_ON_EXPORT, "false" ) );
    if ( clearGlobalVariables ) {
      schemaName = isGlobalParameter( schemaName ) ? null : schemaName;
      connectionName = isGlobalParameter( connectionName ) ? null : connectionName;
      tableName = isGlobalParameter( tableName ) ? null : tableName;
      timeoutInDays = isGlobalParameter( timeoutInDays ) ? null : timeoutInDays;
    }
  }

  protected boolean isGlobalParameter( String parameter ) {
    if ( parameter == null ) {
      return false;
    }

    if ( parameter.startsWith( "${" ) && parameter.endsWith( "}" ) ) {
      return System.getProperty( parameter.substring( 2, parameter.length() - 1 ) ) != null;
    }

    return false;
  }
}
