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

package org.apache.hop.workflow.actions.mysqlbulkload;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.io.File;
import java.util.List;

/**
 * This defines a MySQL action.
 *
 * @author Samatar Hassan
 * @since Jan-2007
 */

@Action(
  id = "MYSQL_BULK_LOAD",
  name = "i18n::ActionMysqlBulkLoad.Name",
  description = "i18n::ActionMysqlBulkLoad.Description",
  image = "MysqlBulkLoad.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.BulkLoading",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/mysqlbulkload.html"
)
public class ActionMysqlBulkLoad extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionMysqlBulkLoad.class; // For Translator

  private String schemaname;
  private String tableName;
  private String filename;
  private String separator;
  private String enclosed;
  private String escaped;
  private String linestarted;
  private String lineterminated;
  private String ignorelines;
  private boolean replacedata;
  private String listattribut;
  private boolean localinfile;
  public int prorityvalue;
  private boolean addfiletoresult;

  private DatabaseMeta connection;

  public ActionMysqlBulkLoad( String n ) {
    super( n, "" );
    tableName = null;
    schemaname = null;
    filename = null;
    separator = null;
    enclosed = null;
    escaped = null;
    lineterminated = null;
    linestarted = null;
    replacedata = true;
    ignorelines = "0";
    listattribut = null;
    localinfile = true;
    connection = null;
    addfiletoresult = false;
  }

  public ActionMysqlBulkLoad() {
    this( "" );
  }

  public Object clone() {
    ActionMysqlBulkLoad je = (ActionMysqlBulkLoad) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "schemaname", schemaname ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "tablename", tableName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "filename", filename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "separator", separator ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "enclosed", enclosed ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "escaped", escaped ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "linestarted", linestarted ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "lineterminated", lineterminated ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "replacedata", replacedata ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "ignorelines", ignorelines ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "listattribut", listattribut ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "localinfile", localinfile ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "prorityvalue", prorityvalue ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "addfiletoresult", addfiletoresult ) );

    retval.append( "      " ).append(
      XmlHandler.addTagValue( "connection", connection == null ? null : connection.getName() ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      schemaname = XmlHandler.getTagValue( entrynode, "schemaname" );
      tableName = XmlHandler.getTagValue( entrynode, "tablename" );
      filename = XmlHandler.getTagValue( entrynode, "filename" );
      separator = XmlHandler.getTagValue( entrynode, "separator" );
      enclosed = XmlHandler.getTagValue( entrynode, "enclosed" );
      escaped = XmlHandler.getTagValue( entrynode, "escaped" );

      linestarted = XmlHandler.getTagValue( entrynode, "linestarted" );
      lineterminated = XmlHandler.getTagValue( entrynode, "lineterminated" );
      replacedata = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "replacedata" ) );
      ignorelines = XmlHandler.getTagValue( entrynode, "ignorelines" );
      listattribut = XmlHandler.getTagValue( entrynode, "listattribut" );
      localinfile = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "localinfile" ) );
      prorityvalue = Const.toInt( XmlHandler.getTagValue( entrynode, "prorityvalue" ), -1 );
      String dbname = XmlHandler.getTagValue( entrynode, "connection" );
      addfiletoresult = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "addfiletoresult" ) );
      connection = DatabaseMeta.loadDatabase( metadataProvider, dbname );
    } catch ( HopException e ) {
      throw new HopXmlException( "Unable to load action of type 'Mysql bulk load' from XML node", e );
    }
  }

  public void setTablename( String tableName ) {
    this.tableName = tableName;
  }

  public void setSchemaname( String schemaname ) {
    this.schemaname = schemaname;
  }

  public String getSchemaname() {
    return schemaname;
  }

  public String getTablename() {
    return tableName;
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
    return true;
  }

  public Result execute( Result previousResult, int nr ) {
    String ReplaceIgnore;
    String IgnoreNbrLignes = "";
    String ListOfColumn = "";
    String LocalExec = "";
    String PriorityText = "";
    String LineTerminatedby = "";
    String FieldTerminatedby = "";

    Result result = previousResult;
    result.setResult( false );

    String vfsFilename = resolve( filename );

    // Let's check the filename ...
    if ( !Utils.isEmpty( vfsFilename ) ) {
      try {
        // User has specified a file, We can continue ...
        //

        // This is running over VFS but we need a normal file.
        // As such, we're going to verify that it's a local file...
        // We're also going to convert VFS FileObject to File
        //
        FileObject fileObject = HopVfs.getFileObject( vfsFilename );
        if ( !( fileObject instanceof LocalFile ) ) {
          // MySQL LOAD DATA can only use local files, so that's what we limit ourselves to.
          //
          throw new HopException( "Only local files are supported at this time, file ["
            + vfsFilename + "] is not a local file." );
        }

        // Convert it to a regular platform specific file name
        //
        String realFilename = HopVfs.getFilename( fileObject );

        // Here we go... back to the regular scheduled program...
        //
        File file = new File( realFilename );
        if ( ( file.exists() && file.canRead() ) || isLocalInfile() == false ) {
          // User has specified an existing file, We can continue ...
          if ( log.isDetailed() ) {
            logDetailed( "File [" + realFilename + "] exists." );
          }

          if ( connection != null ) {
            // User has specified a connection, We can continue ...
            Database db = new Database( this, this, connection );
            try {
              db.connect();
              // Get schemaname
              String realSchemaname = resolve( schemaname );
              // Get tablename
              String realTablename = resolve( tableName );

              if ( db.checkTableExists( realTablename ) ) {
                // The table existe, We can continue ...
                if ( log.isDetailed() ) {
                  logDetailed( "Table [" + realTablename + "] exists." );
                }

                // Add schemaname (Most the time Schemaname.Tablename)
                if ( schemaname != null ) {
                  realTablename = realSchemaname + "." + realTablename;
                }

                // Set the REPLACE or IGNORE
                if ( isReplacedata() ) {
                  ReplaceIgnore = "REPLACE";
                } else {
                  ReplaceIgnore = "IGNORE";
                }

                // Set the IGNORE LINES
                if ( Const.toInt( getRealIgnorelines(), 0 ) > 0 ) {
                  IgnoreNbrLignes = "IGNORE " + getRealIgnorelines() + " LINES";
                }

                // Set list of Column
                if ( getRealListattribut() != null ) {
                  ListOfColumn = "(" + MysqlString( getRealListattribut() ) + ")";

                }

                // Local File execution
                if ( isLocalInfile() ) {
                  LocalExec = "LOCAL";
                }

                // Prority
                if ( prorityvalue == 1 ) {
                  // LOW
                  PriorityText = "LOW_PRIORITY";
                } else if ( prorityvalue == 2 ) {
                  // CONCURRENT
                  PriorityText = "CONCURRENT";
                }

                // Fields ....
                if ( getRealSeparator() != null || getRealEnclosed() != null || getRealEscaped() != null ) {
                  FieldTerminatedby = "FIELDS ";

                  if ( getRealSeparator() != null ) {
                    FieldTerminatedby =
                      FieldTerminatedby
                        + "TERMINATED BY '" + Const.replace( getRealSeparator(), "'", "''" ) + "'";
                  }
                  if ( getRealEnclosed() != null ) {
                    FieldTerminatedby =
                      FieldTerminatedby + " ENCLOSED BY '" + Const.replace( getRealEnclosed(), "'", "''" ) + "'";

                  }
                  if ( getRealEscaped() != null ) {

                    FieldTerminatedby =
                      FieldTerminatedby + " ESCAPED BY '" + Const.replace( getRealEscaped(), "'", "''" ) + "'";

                  }
                }

                // LINES ...
                if ( getRealLinestarted() != null || getRealLineterminated() != null ) {
                  LineTerminatedby = "LINES ";

                  // Line starting By
                  if ( getRealLinestarted() != null ) {
                    LineTerminatedby =
                      LineTerminatedby
                        + "STARTING BY '" + Const.replace( getRealLinestarted(), "'", "''" ) + "'";
                  }

                  // Line terminating By
                  if ( getRealLineterminated() != null ) {
                    LineTerminatedby =
                      LineTerminatedby
                        + " TERMINATED BY '" + Const.replace( getRealLineterminated(), "'", "''" ) + "'";
                  }
                }

                String SqlBulkLoad =
                  "LOAD DATA "
                    + PriorityText + " " + LocalExec + " INFILE '" + realFilename.replace( '\\', '/' ) + "' "
                    + ReplaceIgnore + " INTO TABLE " + realTablename + " " + FieldTerminatedby + " "
                    + LineTerminatedby + " " + IgnoreNbrLignes + " " + ListOfColumn + ";";

                try {
                  // Run the SQL
                  db.execStatement( SqlBulkLoad );

                  // Everything is OK...we can deconnect now
                  db.disconnect();

                  if ( isAddFileToResult() ) {
                    // Add zip filename to output files
                    ResultFile resultFile =
                      new ResultFile(
                        ResultFile.FILE_TYPE_GENERAL, HopVfs.getFileObject( realFilename ), parentWorkflow
                        .getWorkflowName(), toString() );
                    result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
                  }

                  result.setResult( true );
                } catch ( HopDatabaseException je ) {
                  db.disconnect();
                  result.setNrErrors( 1 );
                  logError( "An error occurred executing this action : " + je.getMessage() );
                } catch ( HopFileException e ) {
                  logError( "An error occurred executing this action : " + e.getMessage() );
                  result.setNrErrors( 1 );
                }
              } else {
                // Of course, the table should have been created already before the bulk load operation
                db.disconnect();
                result.setNrErrors( 1 );
                if ( log.isDetailed() ) {
                  logDetailed( "Table [" + realTablename + "] doesn't exist!" );
                }
              }
            } catch ( HopDatabaseException dbe ) {
              db.disconnect();
              result.setNrErrors( 1 );
              logError( "An error occurred executing this entry: " + dbe.getMessage() );
            }
          } else {
            // No database connection is defined
            result.setNrErrors( 1 );
            logError( BaseMessages.getString( PKG, "JobMysqlBulkLoad.Nodatabase.Label" ) );
          }
        } else {
          // the file doesn't exist
          result.setNrErrors( 1 );
          logError( "File [" + realFilename + "] doesn't exist!" );
        }
      } catch ( Exception e ) {
        // An unexpected error occurred
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "JobMysqlBulkLoad.UnexpectedError.Label" ), e );
      }
    } else {
      // No file was specified
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "JobMysqlBulkLoad.Nofilename.Label" ) );
    }
    return result;
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    return new DatabaseMeta[] { connection, };
  }

  public boolean isReplacedata() {
    return replacedata;
  }

  public void setReplacedata( boolean replacedata ) {
    this.replacedata = replacedata;
  }

  public void setLocalInfile( boolean localinfile ) {
    this.localinfile = localinfile;
  }

  public boolean isLocalInfile() {
    return localinfile;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public String getFilename() {
    return filename;
  }

  public void setSeparator( String separator ) {
    this.separator = separator;
  }

  public void setLineterminated( String lineterminated ) {
    this.lineterminated = lineterminated;
  }

  public void setLinestarted( String linestarted ) {
    this.linestarted = linestarted;
  }

  public String getEnclosed() {
    return enclosed;
  }

  public String getRealEnclosed() {
    return resolve( getEnclosed() );
  }

  public void setEnclosed( String enclosed ) {
    this.enclosed = enclosed;
  }

  public String getEscaped() {
    return escaped;
  }

  public String getRealEscaped() {
    return resolve( getEscaped() );
  }

  public void setEscaped( String escaped ) {
    this.escaped = escaped;
  }

  public String getSeparator() {
    return separator;
  }

  public String getLineterminated() {
    return lineterminated;
  }

  public String getLinestarted() {
    return linestarted;
  }

  public String getRealLinestarted() {
    return resolve( getLinestarted() );
  }

  public String getRealLineterminated() {
    return resolve( getLineterminated() );
  }

  public String getRealSeparator() {
    return resolve( getSeparator() );
  }

  public void setIgnorelines( String ignorelines ) {
    this.ignorelines = ignorelines;
  }

  public String getIgnorelines() {
    return ignorelines;
  }

  public String getRealIgnorelines() {
    return resolve( getIgnorelines() );
  }

  public void setListattribut( String listattribut ) {
    this.listattribut = listattribut;
  }

  public String getListattribut() {
    return listattribut;
  }

  public String getRealListattribut() {
    return resolve( getListattribut() );
  }

  public void setAddFileToResult( boolean addfiletoresultin ) {
    this.addfiletoresult = addfiletoresultin;
  }

  public boolean isAddFileToResult() {
    return addfiletoresult;
  }

  private String MysqlString( String listcolumns ) {
    /*
     * Handle forbiden char like '
     */
    String returnString = "";
    String[] split = listcolumns.split( "," );

    for ( int i = 0; i < split.length; i++ ) {
      if ( returnString.equals( "" ) ) {
        returnString = "`" + Const.trim( split[ i ] ) + "`";
      } else {
        returnString = returnString + ", `" + Const.trim( split[ i ] ) + "`";
      }
    }

    return returnString;
  }

  public List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( variables, workflowMeta );
    ResourceReference reference = null;
    if ( connection != null ) {
      reference = new ResourceReference( this );
      references.add( reference );
      reference.getEntries().add( new ResourceEntry( connection.getHostname(), ResourceType.SERVER ) );
      reference.getEntries().add( new ResourceEntry( connection.getDatabaseName(), ResourceType.DATABASENAME ) );
    }
    if ( filename != null ) {
      String realFilename = getRealFilename();
      if ( reference == null ) {
        reference = new ResourceReference( this );
        references.add( reference );
      }
      reference.getEntries().add( new ResourceEntry( realFilename, ResourceType.FILE ) );
    }
    return references;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, ActionValidatorUtils.notBlankValidator(), ActionValidatorUtils.fileExistsValidator() );
    ActionValidatorUtils.andValidator().validate( this, "filename", remarks, ctx );

    ActionValidatorUtils.andValidator().validate( this, "tablename", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
  }

}
