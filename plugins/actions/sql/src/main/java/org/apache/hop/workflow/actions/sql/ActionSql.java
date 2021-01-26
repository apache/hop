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

package org.apache.hop.workflow.actions.sql;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

/**
 * This defines an SQL action.
 *
 * @author Matt
 * @since 05-11-2003
 */

@Action(
  id = "SQL",
  name = "i18n::ActionSQL.Name",
  description = "i18n::ActionSQL.Description",
  image = "sql.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Scripting",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/sql.html"
)
public class ActionSql extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSql.class; // For Translator

  private String sql;
  private DatabaseMeta connection;
  private boolean useVariableSubstitution = false;
  private boolean sqlfromfile = false;
  private String sqlfilename;
  private boolean sendOneStatement = false;

  public ActionSql(String n ) {
    super( n, "" );
    sql = null;
    connection = null;
  }

  public ActionSql() {
    this( "" );
  }

  public Object clone() {
    ActionSql je = (ActionSql) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXml() );

    retval.append( "      " ).append( XmlHandler.addTagValue( "sql", sql ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "useVariableSubstitution", useVariableSubstitution ? "T" : "F" ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "sqlfromfile", sqlfromfile ? "T" : "F" ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "sqlfilename", sqlfilename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "sendOneStatement", sendOneStatement ? "T" : "F" ) );

    retval.append( "      " ).append(
      XmlHandler.addTagValue( "connection", connection == null ? null : connection.getName() ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      sql = XmlHandler.getTagValue( entrynode, "sql" );
      String dbname = XmlHandler.getTagValue( entrynode, "connection" );
      String sSubs = XmlHandler.getTagValue( entrynode, "useVariableSubstitution" );

      if ( sSubs != null && sSubs.equalsIgnoreCase( "T" ) ) {
        useVariableSubstitution = true;
      }
      connection = DatabaseMeta.loadDatabase( metadataProvider, dbname );

      String ssql = XmlHandler.getTagValue( entrynode, "sqlfromfile" );
      if ( ssql != null && ssql.equalsIgnoreCase( "T" ) ) {
        sqlfromfile = true;
      }

      sqlfilename = XmlHandler.getTagValue( entrynode, "sqlfilename" );

      String sOneStatement = XmlHandler.getTagValue( entrynode, "sendOneStatement" );
      if ( sOneStatement != null && sOneStatement.equalsIgnoreCase( "T" ) ) {
        sendOneStatement = true;
      }

    } catch ( HopException e ) {
      throw new HopXmlException( "Unable to load action of type 'sql' from XML node", e );
    }
  }

  public void setSql( String sql ) {
    this.sql = sql;
  }

  public String getSql() {
    return sql;
  }

  public String getSqlFilename() {
    return sqlfilename;
  }

  public void setSqlFilename( String sqlfilename ) {
    this.sqlfilename = sqlfilename;
  }

  public boolean getUseVariableSubstitution() {
    return useVariableSubstitution;
  }

  public void setUseVariableSubstitution( boolean subs ) {
    useVariableSubstitution = subs;
  }

  public void setSqlFromFile( boolean sqlfromfilein ) {
    sqlfromfile = sqlfromfilein;
  }

  public boolean getSqlFromFile() {
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
      Database db = new Database( this, this, connection );
      FileObject sqlFile = null;
      try {
        String theSql = null;
        db.connect();

        if ( sqlfromfile ) {
          if ( sqlfilename == null ) {
            throw new HopDatabaseException( BaseMessages.getString( PKG, "JobSQL.NoSQLFileSpecified" ) );
          }

          try {
            String realfilename = resolve( sqlfilename );
            sqlFile = HopVfs.getFileObject( realfilename );
            if ( !sqlFile.exists() ) {
              logError( BaseMessages.getString( PKG, "JobSQL.SQLFileNotExist", realfilename ) );
              throw new HopDatabaseException( BaseMessages.getString(
                PKG, "JobSQL.SQLFileNotExist", realfilename ) );
            }
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobSQL.SQLFileExists", realfilename ) );
            }

            InputStream IS = HopVfs.getInputStream( sqlFile );
            try {
              InputStreamReader BIS = new InputStreamReader( new BufferedInputStream( IS, 500 ) );
              StringBuilder lineSB = new StringBuilder( 256 );
              lineSB.setLength( 0 );

              BufferedReader buff = new BufferedReader( BIS );
              String sLine = null;
              theSql = Const.CR;

              while ( ( sLine = buff.readLine() ) != null ) {
                if ( Utils.isEmpty( sLine ) ) {
                  theSql = theSql + Const.CR;
                } else {
                  theSql = theSql + Const.CR + sLine;
                }
              }
            } finally {
              IS.close();
            }
          } catch ( Exception e ) {
            throw new HopDatabaseException( BaseMessages.getString( PKG, "JobSQL.ErrorRunningSQLfromFile" ), e );
          }

        } else {
          theSql = sql;
        }
        if ( !Utils.isEmpty( theSql ) ) {
          // let it run
          if ( useVariableSubstitution ) {
            theSql = resolve( theSql );
          }
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobSQL.Log.SQlStatement", theSql ) );
          }
          if ( sendOneStatement ) {
            db.execStatement( theSql );
          } else {
            db.execStatements( theSql );
          }
        }
      } catch ( HopDatabaseException je ) {
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "JobSQL.ErrorRunAction", je.getMessage() ) );
      } finally {
        db.disconnect();
        if ( sqlFile != null ) {
          try {
            sqlFile.close();
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

  @Override public boolean isEvaluation() {
    return true;
  }

  public boolean isUnconditional() {
    return true;
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
    ActionValidatorUtils.andValidator().validate( this, "SQL", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
  }

}
