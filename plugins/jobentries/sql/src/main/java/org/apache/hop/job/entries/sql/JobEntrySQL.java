/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.job.entries.sql;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

/**
 * This defines an SQL job entry.
 *
 * @author Matt
 * @since 05-11-2003
 */

@JobEntry(
  id = "SQL",
  i18nPackageName = "org.apache.hop.job.entries.sql",
  name = "JobEntrySQL.Name",
  description = "JobEntrySQL.Description",
  image = "SQL.svg",
  categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.Scripting"
)
public class JobEntrySQL extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntrySQL.class; // for i18n purposes, needed by Translator2!!

  private String sql;
  private DatabaseMeta connection;
  private boolean useVariableSubstitution = false;
  private boolean sqlfromfile = false;
  private String sqlfilename;
  private boolean sendOneStatement = false;

  public JobEntrySQL( String n ) {
    super( n, "" );
    sql = null;
    connection = null;
  }

  public JobEntrySQL() {
    this( "" );
  }

  public Object clone() {
    JobEntrySQL je = (JobEntrySQL) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXML() );

    retval.append( "      " ).append( XMLHandler.addTagValue( "sql", sql ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "useVariableSubstitution", useVariableSubstitution ? "T" : "F" ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "sqlfromfile", sqlfromfile ? "T" : "F" ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "sqlfilename", sqlfilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "sendOneStatement", sendOneStatement ? "T" : "F" ) );

    retval.append( "      " ).append(
      XMLHandler.addTagValue( "connection", connection == null ? null : connection.getName() ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      sql = XMLHandler.getTagValue( entrynode, "sql" );
      String dbname = XMLHandler.getTagValue( entrynode, "connection" );
      String sSubs = XMLHandler.getTagValue( entrynode, "useVariableSubstitution" );

      if ( sSubs != null && sSubs.equalsIgnoreCase( "T" ) ) {
        useVariableSubstitution = true;
      }
      connection = DatabaseMeta.loadDatabase( metaStore, dbname );

      String ssql = XMLHandler.getTagValue( entrynode, "sqlfromfile" );
      if ( ssql != null && ssql.equalsIgnoreCase( "T" ) ) {
        sqlfromfile = true;
      }

      sqlfilename = XMLHandler.getTagValue( entrynode, "sqlfilename" );

      String sOneStatement = XMLHandler.getTagValue( entrynode, "sendOneStatement" );
      if ( sOneStatement != null && sOneStatement.equalsIgnoreCase( "T" ) ) {
        sendOneStatement = true;
      }

    } catch ( HopException e ) {
      throw new HopXMLException( "Unable to load job entry of type 'sql' from XML node", e );
    }
  }

  public void setSQL( String sql ) {
    this.sql = sql;
  }

  public String getSQL() {
    return sql;
  }

  public String getSQLFilename() {
    return sqlfilename;
  }

  public void setSQLFilename( String sqlfilename ) {
    this.sqlfilename = sqlfilename;
  }

  public boolean getUseVariableSubstitution() {
    return useVariableSubstitution;
  }

  public void setUseVariableSubstitution( boolean subs ) {
    useVariableSubstitution = subs;
  }

  public void setSQLFromFile( boolean sqlfromfilein ) {
    sqlfromfile = sqlfromfilein;
  }

  public boolean getSQLFromFile() {
    return sqlfromfile;
  }

  public boolean isSendOneStatement() {
    return sendOneStatement;
  }

  public void setSendOneStatement( boolean sendOneStatementin ) {
    sendOneStatement = sendOneStatementin;
  }

  public void setDatabase( DatabaseMeta database ) {
    this.connection = database;
  }

  public DatabaseMeta getDatabase() {
    return connection;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;

    if ( connection != null ) {
      Database db = new Database( this, connection );
      FileObject SQLfile = null;
      db.shareVariablesWith( this );
      try {
        String theSQL = null;
        db.connect( parentJob.getTransactionId(), null );

        if ( sqlfromfile ) {
          if ( sqlfilename == null ) {
            throw new HopDatabaseException( BaseMessages.getString( PKG, "JobSQL.NoSQLFileSpecified" ) );
          }

          try {
            String realfilename = environmentSubstitute( sqlfilename );
            SQLfile = HopVFS.getFileObject( realfilename, this );
            if ( !SQLfile.exists() ) {
              logError( BaseMessages.getString( PKG, "JobSQL.SQLFileNotExist", realfilename ) );
              throw new HopDatabaseException( BaseMessages.getString(
                PKG, "JobSQL.SQLFileNotExist", realfilename ) );
            }
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobSQL.SQLFileExists", realfilename ) );
            }

            InputStream IS = HopVFS.getInputStream( SQLfile );
            try {
              InputStreamReader BIS = new InputStreamReader( new BufferedInputStream( IS, 500 ) );
              StringBuilder lineSB = new StringBuilder( 256 );
              lineSB.setLength( 0 );

              BufferedReader buff = new BufferedReader( BIS );
              String sLine = null;
              theSQL = Const.CR;

              while ( ( sLine = buff.readLine() ) != null ) {
                if ( Utils.isEmpty( sLine ) ) {
                  theSQL = theSQL + Const.CR;
                } else {
                  theSQL = theSQL + Const.CR + sLine;
                }
              }
            } finally {
              IS.close();
            }
          } catch ( Exception e ) {
            throw new HopDatabaseException( BaseMessages.getString( PKG, "JobSQL.ErrorRunningSQLfromFile" ), e );
          }

        } else {
          theSQL = sql;
        }
        if ( !Utils.isEmpty( theSQL ) ) {
          // let it run
          if ( useVariableSubstitution ) {
            theSQL = environmentSubstitute( theSQL );
          }
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobSQL.Log.SQlStatement", theSQL ) );
          }
          if ( sendOneStatement ) {
            db.execStatement( theSQL );
          } else {
            db.execStatements( theSQL );
          }
        }
      } catch ( HopDatabaseException je ) {
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "JobSQL.ErrorRunJobEntry", je.getMessage() ) );
      } finally {
        db.disconnect();
        if ( SQLfile != null ) {
          try {
            SQLfile.close();
          } catch ( Exception e ) {
            // Ignore errors
          }
        }
      }
    } else {
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "JobSQL.NoDatabaseConnection" ) );
    }

    if ( result.getNrErrors() == 0 ) {
      result.setResult( true );
    } else {
      result.setResult( false );
    }

    return result;
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return true;
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
    JobEntryValidatorUtils.andValidator().validate( this, "SQL", remarks,
      AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
  }

}
