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

package org.apache.hop.workflow.actions.tableexists;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.List;

/**
 * This defines a table exists action.
 *
 * @author Matt
 * @since 05-11-2003
 */

@Action(
  id = "TABLE_EXISTS",
  name = "i18n::ActionTableExists.Name",
  description = "i18n::ActionTableExists.Description",
  image = "TableExists.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/tableexists.html"
)
public class ActionTableExists extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionTableExists.class; // For Translator

  private String tableName;
  private String schemaname;
  private DatabaseMeta connection;

  public ActionTableExists( String n ) {
    super( n, "" );
    schemaname = null;
    tableName = null;
    connection = null;
  }

  public ActionTableExists() {
    this( "" );
  }

  public Object clone() {
    ActionTableExists je = (ActionTableExists) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXml() );

    retval.append( "      " ).append( XmlHandler.addTagValue( "tablename", tableName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "schemaname", schemaname ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "connection", connection == null ? null : connection.getName() ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );

      tableName = XmlHandler.getTagValue( entrynode, "tablename" );
      schemaname = XmlHandler.getTagValue( entrynode, "schemaname" );
      String dbname = XmlHandler.getTagValue( entrynode, "connection" );
      connection = DatabaseMeta.loadDatabase( metadataProvider, dbname );
    } catch ( HopException e ) {
      throw new HopXmlException( BaseMessages.getString( PKG, "TableExists.Meta.UnableLoadXml" ), e );
    }
  }

  public void setTablename( String tableName ) {
    this.tableName = tableName;
  }

  public String getTablename() {
    return tableName;
  }

  public String getSchemaname() {
    return schemaname;
  }

  public void setSchemaname( String schemaname ) {
    this.schemaname = schemaname;
  }

  public void setDatabase( DatabaseMeta database ) {
    this.connection = database;
  }

  public DatabaseMeta getDatabase() {
    return connection;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );

    if ( connection != null ) {
      Database db = new Database( this, this, connection );
      try {
        db.connect();
        String realTablename = resolve( tableName );
        String realSchemaname = resolve( schemaname );

        if ( db.checkTableExists( realSchemaname, realTablename ) ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "TableExists.Log.TableExists", realTablename ) );
          }
          result.setResult( true );
        } else {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "TableExists.Log.TableNotExists", realTablename ) );
          }
        }
      } catch ( HopDatabaseException dbe ) {
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "TableExists.Error.RunningAction", dbe.getMessage() ) );
      } finally {
        if ( db != null ) {
          try {
            db.disconnect();
          } catch ( Exception e ) { /* Ignore */
          }
        }
      }
    } else {
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "TableExists.Error.NoConnectionDefined" ) );
    }

    return result;
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    return new DatabaseMeta[] { connection, };
  }

  public List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( variables, workflowMeta );
    if ( connection != null ) {
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( connection.getHostname(), ResourceType.SERVER ) );
      reference.getEntries().add( new ResourceEntry( connection.getDatabaseName(), ResourceType.DATABASENAME ) );
      references.add( reference );
    }
    return references;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    ActionValidatorUtils.andValidator().validate( this, "tablename", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
  }

}
