/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.job.entries.tableexists;

import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.List;

/**
 * This defines a table exists job entry.
 *
 * @author Matt
 * @since 05-11-2003
 */

@JobEntry(
  id = "TABLE_EXISTS",
  i18nPackageName = "org.apache.hop.job.entries.tableexists",
  name = "JobEntryTableExists.Name",
  description = "JobEntryTableExists.Description",
  image = "TableExists.svg",
  categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.Conditions"
)
public class JobEntryTableExists extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryTableExists.class; // for i18n purposes, needed by Translator!!

  private String tablename;
  private String schemaname;
  private DatabaseMeta connection;

  public JobEntryTableExists( String n ) {
    super( n, "" );
    schemaname = null;
    tablename = null;
    connection = null;
  }

  public JobEntryTableExists() {
    this( "" );
  }

  public Object clone() {
    JobEntryTableExists je = (JobEntryTableExists) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXML() );

    retval.append( "      " ).append( XMLHandler.addTagValue( "tablename", tablename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "schemaname", schemaname ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "connection", connection == null ? null : connection.getName() ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );

      tablename = XMLHandler.getTagValue( entrynode, "tablename" );
      schemaname = XMLHandler.getTagValue( entrynode, "schemaname" );
      String dbname = XMLHandler.getTagValue( entrynode, "connection" );
      connection = DatabaseMeta.loadDatabase( metaStore, dbname );
    } catch ( HopException e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "TableExists.Meta.UnableLoadXml" ), e );
    }
  }

  public void setTablename( String tablename ) {
    this.tablename = tablename;
  }

  public String getTablename() {
    return tablename;
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

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );

    if ( connection != null ) {
      Database db = new Database( this, connection );
      db.shareVariablesWith( this );
      try {
        db.connect( parentJob.getTransactionId(), null );
        String realTablename = environmentSubstitute( tablename );
        String realSchemaname = environmentSubstitute( schemaname );

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
        logError( BaseMessages.getString( PKG, "TableExists.Error.RunningJobEntry", dbe.getMessage() ) );
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

  public List<ResourceReference> getResourceDependencies( JobMeta jobMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( jobMeta );
    if ( connection != null ) {
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( connection.getHostname(), ResourceType.SERVER ) );
      reference.getEntries().add( new ResourceEntry( connection.getDatabaseName(), ResourceType.DATABASENAME ) );
      references.add( reference );
    }
    return references;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
                     IMetaStore metaStore ) {
    JobEntryValidatorUtils.andValidator().validate( this, "tablename", remarks,
      AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
  }

}
