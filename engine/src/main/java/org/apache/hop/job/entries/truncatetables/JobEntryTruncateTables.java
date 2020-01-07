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

package org.apache.hop.job.entries.truncatetables;

import org.apache.hop.job.entry.validator.AbstractFileValidator;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;

import java.util.List;

import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
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
import org.apache.hop.job.entry.validator.ValidatorContext;

import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * This defines a Truncate Tables job entry.
 *
 * @author Samatar
 * @since 22-07-2008
 *
 */
public class JobEntryTruncateTables extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryTruncateTables.class; // for i18n purposes, needed by Translator2!!

  public boolean argFromPrevious;

  private DatabaseMeta connection;

  public String[] arguments;

  public String[] schemaname;

  private int nrErrors = 0;
  private int nrSuccess = 0;
  boolean continueProcess = true;

  public JobEntryTruncateTables( String n ) {
    super( n, "" );
    this.argFromPrevious = false;
    this.arguments = null;
    this.schemaname = null;
    this.connection = null;
  }

  public JobEntryTruncateTables() {
    this( "" );
  }

  public void allocate( int nrFields ) {
    this.arguments = new String[nrFields];
    this.schemaname = new String[nrFields];
  }

  public Object clone() {
    JobEntryTruncateTables je = (JobEntryTruncateTables) super.clone();
    if ( arguments != null ) {
      int nrFields = arguments.length;
      je.allocate( nrFields );
      System.arraycopy( arguments, 0, je.arguments, 0, nrFields );
      System.arraycopy( schemaname, 0, je.schemaname, 0, nrFields );
    }
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXML() );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "connection", this.connection == null ? null : this.connection.getName() ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "arg_from_previous", this.argFromPrevious ) );
    retval.append( "      <fields>" ).append( Const.CR );
    if ( arguments != null ) {
      for ( int i = 0; i < this.arguments.length; i++ ) {
        retval.append( "        <field>" ).append( Const.CR );
        retval.append( "          " ).append( XMLHandler.addTagValue( "name", this.arguments[i] ) );
        retval.append( "          " ).append( XMLHandler.addTagValue( "schemaname", this.schemaname[i] ) );
        retval.append( "        </field>" ).append( Const.CR );
      }
    }
    retval.append( "      </fields>" ).append( Const.CR );
    return retval.toString();
  }

  public void loadXML( Node entrynode, List<SlaveServer> slaveServers,
    IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode, slaveServers );

      String dbname = XMLHandler.getTagValue( entrynode, "connection" );
      this.connection = DatabaseMeta.loadDatabase( metaStore, dbname );
      this.argFromPrevious = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "arg_from_previous" ) );

      Node fields = XMLHandler.getSubNode( entrynode, "fields" );

      // How many field arguments?
      int nrFields = XMLHandler.countNodes( fields, "field" );
      allocate( nrFields );

      // Read them all...
      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        this.arguments[i] = XMLHandler.getTagValue( fnode, "name" );
        this.schemaname[i] = XMLHandler.getTagValue( fnode, "schemaname" );
      }
    } catch ( HopException e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "JobEntryTruncateTables.UnableLoadXML" ), e );
    }
  }

  public void setDatabase( DatabaseMeta database ) {
    this.connection = database;
  }

  public DatabaseMeta getDatabase() {
    return this.connection;
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return true;
  }

  private boolean truncateTables( String tablename, String schemaname, Database db ) {
    boolean retval = false;
    try {
      // check if table exists!
      if ( db.checkTableExists( schemaname, tablename ) ) {
        if ( !Utils.isEmpty( schemaname ) ) {
          db.truncateTable( schemaname, tablename );
        } else {
          db.truncateTable( tablename );
        }

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobEntryTruncateTables.Log.TableTruncated", tablename ) );
        }

        retval = true;
      } else {
        logError( BaseMessages.getString( PKG, "JobEntryTruncateTables.Error.CanNotFindTable", tablename ) );
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JobEntryTruncateTables.Error.CanNotTruncateTables", tablename, e
        .toString() ) );
    }
    return retval;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;

    result.setResult( true );
    nrErrors = 0;
    continueProcess = true;
    nrSuccess = 0;

    if ( argFromPrevious ) {
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobEntryTruncateTables.FoundPreviousRows", String
          .valueOf( ( rows != null ? rows.size() : 0 ) ) ) );
      }
      if ( rows.size() == 0 ) {
        return result;
      }
    }
    if ( connection != null ) {
      Database db = new Database( this, connection );
      db.shareVariablesWith( this );
      try {
        db.connect( parentJob.getTransactionId(), null );
        if ( argFromPrevious && rows != null ) { // Copy the input row to the (command line) arguments

          for ( int iteration = 0; iteration < rows.size() && !parentJob.isStopped() && continueProcess; iteration++ ) {
            resultRow = rows.get( iteration );

            // Get values from previous result
            String tablename_previous = resultRow.getString( 0, null );
            String schemaname_previous = resultRow.getString( 1, null );

            if ( !Utils.isEmpty( tablename_previous ) ) {
              if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString(
                  PKG, "JobEntryTruncateTables.ProcessingRow", tablename_previous, schemaname_previous ) );
              }

              // let's truncate table
              if ( truncateTables( tablename_previous, schemaname_previous, db ) ) {
                updateSuccess();
              } else {
                updateErrors();
              }
            } else {
              logError( BaseMessages.getString( PKG, "JobEntryTruncateTables.RowEmpty" ) );
            }
          }

        } else if ( arguments != null ) {
          for ( int i = 0; i < arguments.length && !parentJob.isStopped() && continueProcess; i++ ) {
            String realTablename = environmentSubstitute( arguments[i] );
            String realSchemaname = environmentSubstitute( schemaname[i] );
            if ( !Utils.isEmpty( realTablename ) ) {
              if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString(
                  PKG, "JobEntryTruncateTables.ProcessingArg", arguments[i], schemaname[i] ) );
              }

              // let's truncate table
              if ( truncateTables( realTablename, realSchemaname, db ) ) {
                updateSuccess();
              } else {
                updateErrors();
              }
            } else {
              logError( BaseMessages.getString(
                PKG, "JobEntryTruncateTables.ArgEmpty", arguments[i], schemaname[i] ) );
            }
          }
        }
      } catch ( Exception dbe ) {
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "JobEntryTruncateTables.Error.RunningEntry", dbe.getMessage() ) );
      } finally {
        if ( db != null ) {
          db.disconnect();
        }
      }
    } else {
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "JobEntryTruncateTables.NoDbConnection" ) );
    }

    result.setNrErrors( nrErrors );
    result.setNrLinesDeleted( nrSuccess );
    result.setResult( nrErrors == 0 );
    return result;
  }

  private void updateErrors() {
    nrErrors++;
    continueProcess = false;
  }

  private void updateSuccess() {
    nrSuccess++;
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    return new DatabaseMeta[] { connection, };
  }

  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
    IMetaStore metaStore ) {
    boolean res = JobEntryValidatorUtils.andValidator().validate( this, "arguments", remarks,
        AndValidator.putValidators( JobEntryValidatorUtils.notNullValidator() ) );

    if ( res == false ) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, JobEntryValidatorUtils.notNullValidator(),
        JobEntryValidatorUtils.fileExistsValidator() );

    for ( int i = 0; i < arguments.length; i++ ) {
      JobEntryValidatorUtils.andValidator().validate( this, "arguments[" + i + "]", remarks, ctx );
    }
  }

  public List<ResourceReference> getResourceDependencies( JobMeta jobMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( jobMeta );
    if ( arguments != null ) {
      ResourceReference reference = null;
      for ( int i = 0; i < arguments.length; i++ ) {
        String filename = jobMeta.environmentSubstitute( arguments[i] );
        if ( reference == null ) {
          reference = new ResourceReference( this );
          references.add( reference );
        }
        reference.getEntries().add( new ResourceEntry( filename, ResourceType.FILE ) );
      }
    }
    return references;
  }

}
