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

package org.apache.hop.workflow.actions.mysqlbulkfile;

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
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * This defines an MYSQL Bulk file action.
 *
 * @author Samatar
 * @since 05-03-2006
 */
	 
@Action(
  id = "MYSQL_BULK_FILE",
  name = "i18n::ActionMysqlBulkFile.Name",
  description = "i18n::ActionMysqlBulkFile.Description",
  image = "MysqlBulkFile.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.BulkLoading",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/mysqlbulkfile.html"
)
public class ActionMysqlBulkFile extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionMysqlBulkFile.class; // For Translator

  private String tableName;
  private String schemaName;
  private String filename;
  private String separator;
  private String enclosed;
  private String lineTerminated;
  private String limitLines;
  private String listColumn;
  private boolean highPriority;
  private boolean optionEnclosed;
  public int outDumpValue;
  public int ifFileExists;
  private boolean addFileToResult;

  private DatabaseMeta connection;

  public ActionMysqlBulkFile( String n ) {
    super( n, "" );
    tableName = null;
    schemaName = null;
    filename = null;
    separator = null;
    enclosed = null;
    limitLines = "0";
    listColumn = null;
    lineTerminated = null;
    highPriority = true;
    optionEnclosed = false;
    ifFileExists = 2;
    connection = null;
    addFileToResult = false;
  }

  public ActionMysqlBulkFile() {
    this( "" );
  }

  public Object clone() {
    ActionMysqlBulkFile je = (ActionMysqlBulkFile) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "schemaname", schemaName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "tablename", tableName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "filename", filename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "separator", separator ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "enclosed", enclosed ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "optionenclosed", optionEnclosed ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "lineterminated", lineTerminated ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "limitlines", limitLines ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "listcolumn", listColumn ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "highpriority", highPriority ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "outdumpvalue", outDumpValue ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "iffileexists", ifFileExists ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "addfiletoresult", addFileToResult ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "connection", connection == null ? null : connection.getName() ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      schemaName = XmlHandler.getTagValue( entrynode, "schemaname" );
      tableName = XmlHandler.getTagValue( entrynode, "tablename" );
      filename = XmlHandler.getTagValue( entrynode, "filename" );
      separator = XmlHandler.getTagValue( entrynode, "separator" );
      enclosed = XmlHandler.getTagValue( entrynode, "enclosed" );
      lineTerminated = XmlHandler.getTagValue( entrynode, "lineterminated" );
      limitLines = XmlHandler.getTagValue( entrynode, "limitlines" );
      listColumn = XmlHandler.getTagValue( entrynode, "listcolumn" );
      highPriority = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "highpriority" ) );
      optionEnclosed = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "optionenclosed" ) );
      outDumpValue = Const.toInt( XmlHandler.getTagValue( entrynode, "outdumpvalue" ), -1 );
      ifFileExists = Const.toInt( XmlHandler.getTagValue( entrynode, "iffileexists" ), -1 );
      String dbname = XmlHandler.getTagValue( entrynode, "connection" );
      connection = DatabaseMeta.loadDatabase( metadataProvider, dbname );
      addFileToResult = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "addfiletoresult" ) );
    } catch ( HopException e ) {
      throw new HopXmlException( "Unable to load action of type 'table exists' from XML node", e );
    }
  }

  public void setTableName( String tableName ) {
    this.tableName = tableName;
  }

  public void setSchemaName( String schemaName ) {
    this.schemaName = schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getSchemaName() {
    return schemaName;
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

    String LimitNbrLignes = "";
    String ListOfColumn = "*";
    String strHighPriority = "";
    String OutDumpText = "";
    String OptionEnclosed = "";
    String FieldSeparator = "";
    String LinesTerminated = "";

    Result result = previousResult;
    result.setResult( false );

    // Let's check the filename ...
    if ( filename != null ) {
      // User has specified a file, We can continue ...
      String realFilename = getRealFilename();
      File file = new File( realFilename );

      if ( file.exists() && ifFileExists == 2 ) {
        // the file exists and user want to Fail
        result.setResult( false );
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "JobMysqlBulkFile.FileExists1.Label" )
          + realFilename + BaseMessages.getString( PKG, "JobMysqlBulkFile.FileExists2.Label" ) );

      } else if ( file.exists() && ifFileExists == 1 ) {
        // the file exists and user want to do nothing
        result.setResult( true );
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobMysqlBulkFile.FileExists1.Label" )
            + realFilename + BaseMessages.getString( PKG, "JobMysqlBulkFile.FileExists2.Label" ) );
        }

      } else {

        if ( file.exists() && ifFileExists == 0 ) {
          // File exists and user want to renamme it with unique name

          // Format Date

          // Try to clean filename (without wildcard)
          String wildcard = realFilename.substring( realFilename.length() - 4, realFilename.length() );
          if ( wildcard.substring( 0, 1 ).equals( "." ) ) {
            // Find wildcard
            realFilename =
              realFilename.substring( 0, realFilename.length() - 4 )
                + "_" + StringUtil.getFormattedDateTimeNow( true ) + wildcard;
          } else {
            // did not find wildcard
            realFilename = realFilename + "_" + StringUtil.getFormattedDateTimeNow( true );
          }

          logDebug( BaseMessages.getString( PKG, "JobMysqlBulkFile.FileNameChange1.Label" )
            + realFilename + BaseMessages.getString( PKG, "JobMysqlBulkFile.FileNameChange1.Label" ) );

        }

        // User has specified an existing file, We can continue ...
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobMysqlBulkFile.FileExists1.Label" )
            + realFilename + BaseMessages.getString( PKG, "JobMysqlBulkFile.FileExists2.Label" ) );
        }

        if ( connection != null ) {
          // User has specified a connection, We can continue ...
          Database db = new Database( this, this, connection );
          try {
            db.connect();
            // Get schemaname
            String realSchemaname = resolve( schemaName );
            // Get tablename
            String realTablename = resolve( tableName );

            if ( db.checkTableExists( realTablename ) ) {
              // The table existe, We can continue ...
              if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JobMysqlBulkFile.TableExists1.Label" )
                  + realTablename + BaseMessages.getString( PKG, "JobMysqlBulkFile.TableExists2.Label" ) );
              }

              // Add schemaname (Most the time Schemaname.Tablename)
              if ( schemaName != null ) {
                realTablename = realSchemaname + "." + realTablename;
              }

              // Set the Limit lines
              if ( Const.toInt( getRealLimitlines(), 0 ) > 0 ) {
                LimitNbrLignes = "LIMIT " + getRealLimitlines();
              }

              // Set list of Column, if null get all columns (*)
              if ( getRealListColumn() != null ) {
                ListOfColumn = MysqlString( getRealListColumn() );
              }

              // Fields separator
              if ( getRealSeparator() != null && outDumpValue == 0 ) {
                FieldSeparator = "FIELDS TERMINATED BY '" + Const.replace( getRealSeparator(), "'", "''" ) + "'";

              }

              // Lines Terminated by
              if ( getRealLineterminated() != null && outDumpValue == 0 ) {
                LinesTerminated =
                  "LINES TERMINATED BY '" + Const.replace( getRealLineterminated(), "'", "''" ) + "'";

              }

              // High Priority ?
              if ( isHighPriority() ) {
                strHighPriority = "HIGH_PRIORITY";
              }

              if ( getRealEnclosed() != null && outDumpValue == 0 ) {
                if ( isOptionEnclosed() ) {
                  OptionEnclosed = "OPTIONALLY ";
                }
                OptionEnclosed =
                  OptionEnclosed + "ENCLOSED BY '" + Const.replace( getRealEnclosed(), "'", "''" ) + "'";

              }

              // OutFile or Dumpfile
              if ( outDumpValue == 0 ) {
                OutDumpText = "INTO OUTFILE";
              } else {
                OutDumpText = "INTO DUMPFILE";
              }

              String FILEBulkFile =
                "SELECT "
                  + strHighPriority + " " + ListOfColumn + " " + OutDumpText + " '" + realFilename + "' "
                  + FieldSeparator + " " + OptionEnclosed + " " + LinesTerminated + " FROM " + realTablename
                  + " " + LimitNbrLignes + " LOCK IN SHARE MODE";

              try {
                if ( log.isDetailed() ) {
                  logDetailed( FILEBulkFile );
                }
                // Run the SQL
                PreparedStatement ps = db.prepareSql( FILEBulkFile );
                ps.execute();

                // Everything is OK...we can disconnect now
                db.disconnect();

                if ( isAddFileToResult() ) {
                  // Add filename to output files
                  ResultFile resultFile =
                    new ResultFile(
                      ResultFile.FILE_TYPE_GENERAL, HopVfs.getFileObject( realFilename ), parentWorkflow
                      .getWorkflowName(), toString() );
                  result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
                }

                result.setResult( true );

              } catch ( SQLException je ) {
                db.disconnect();
                result.setNrErrors( 1 );
                logError( BaseMessages.getString( PKG, "JobMysqlBulkFile.Error.Label" ) + " " + je.getMessage() );
              } catch ( HopFileException e ) {
                logError( BaseMessages.getString( PKG, "JobMysqlBulkFile.Error.Label" ) + e.getMessage() );
                result.setNrErrors( 1 );
              }

            } else {
              // Of course, the table should have been created already before the bulk load operation
              db.disconnect();
              result.setNrErrors( 1 );
              if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JobMysqlBulkFile.TableNotExists1.Label" )
                  + realTablename + BaseMessages.getString( PKG, "JobMysqlBulkFile.TableNotExists2.Label" ) );
              }
            }

          } catch ( HopDatabaseException dbe ) {
            db.disconnect();
            result.setNrErrors( 1 );
            logError( BaseMessages.getString( PKG, "JobMysqlBulkFile.Error.Label" ) + " " + dbe.getMessage() );
          }

        } else {
          // No database connection is defined
          result.setNrErrors( 1 );
          logError( BaseMessages.getString( PKG, "JobMysqlBulkFile.Nodatabase.Label" ) );
        }

      }

    } else {
      // No file was specified
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "JobMysqlBulkFile.Nofilename.Label" ) );
    }

    return result;

  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    return new DatabaseMeta[] { connection, };
  }

  public void setHighPriority( boolean highpriority ) {
    this.highPriority = highpriority;
  }

  public void setOptionEnclosed( boolean optionenclosed ) {
    this.optionEnclosed = optionenclosed;
  }

  public boolean isHighPriority() {
    return highPriority;
  }

  public boolean isOptionEnclosed() {
    return optionEnclosed;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public String getFilename() {
    return filename;
  }

  public String getRealFilename() {
    String RealFile = resolve( getFilename() );
    return RealFile.replace( '\\', '/' );
  }

  public void setSeparator( String separator ) {
    this.separator = separator;
  }

  public void setEnclosed( String enclosed ) {
    this.enclosed = enclosed;
  }

  public void setLineTerminated( String lineTerminated ) {
    this.lineTerminated = lineTerminated;
  }

  public String getLineTerminated() {
    return lineTerminated;
  }

  public String getRealLineterminated() {
    return resolve( getLineTerminated() );
  }

  public String getSeparator() {
    return separator;
  }

  public String getEnclosed() {
    return enclosed;
  }

  public String getRealSeparator() {
    return resolve( getSeparator() );
  }

  public String getRealEnclosed() {
    return resolve( getEnclosed() );
  }

  public void setLimitLines( String limitLines ) {
    this.limitLines = limitLines;
  }

  public String getLimitLines() {
    return limitLines;
  }

  public String getRealLimitlines() {
    return resolve( getLimitLines() );
  }

  public void setListColumn( String listcolumn ) {
    this.listColumn = listcolumn;
  }

  public String getListColumn() {
    return listColumn;
  }

  public String getRealListColumn() {
    return resolve( getListColumn() );
  }

  public void setAddFileToResult( boolean addfiletoresultin ) {
    this.addFileToResult = addfiletoresultin;
  }

  public boolean isAddFileToResult() {
    return addFileToResult;
  }

  private String MysqlString( String listcolumns ) {
    /*
     * handle forbiden char like '
     */
    String ReturnString = "";
    String[] split = listcolumns.split( "," );

    for ( int i = 0; i < split.length; i++ ) {
      if ( ReturnString.equals( "" ) ) {
        ReturnString = "`" + Const.trim( split[ i ] ) + "`";
      } else {
        ReturnString = ReturnString + ", `" + Const.trim( split[ i ] ) + "`";
      }

    }

    return ReturnString;

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
    ActionValidatorUtils.andValidator().validate( this, "filename", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    ActionValidatorUtils.andValidator().validate( this, "tablename", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
  }

}
