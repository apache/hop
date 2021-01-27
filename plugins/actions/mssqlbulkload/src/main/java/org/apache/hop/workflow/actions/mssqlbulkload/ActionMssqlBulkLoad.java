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

package org.apache.hop.workflow.actions.mssqlbulkload;

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
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * This defines a MSSQL Bulk action.
 *
 * @author Samatar Hassan
 * @since Jan-2007
 */
@Action(
	  id = "MSSQL_BULK_LOAD",
	  name = "i18n::ActionMssqlBulkLoad.Name",
	  description = "i18n::ActionMssqlBulkLoad.Description",
	  image = "MssqlBulkLoad.svg",
	  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.BulkLoading",
	  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/mssqlbulkload.html"
)
public class ActionMssqlBulkLoad extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionMssqlBulkLoad.class; // For Translator

  private String schemaname;
  private String tableName;
  private String filename;
  private String datafiletype;
  private String fieldterminator;
  private String lineterminated;
  private String codepage;
  private String specificcodepage;
  private int startfile;
  private int endfile;
  private String orderby;
  private boolean addfiletoresult;
  private String formatfilename;
  private boolean firetriggers;
  private boolean checkconstraints;
  private boolean keepnulls;
  private boolean tablock;
  private String errorfilename;
  private boolean adddatetime;
  private String orderdirection;
  private int maxerrors;
  private int batchsize;
  private int rowsperbatch;
  private boolean keepidentity;
  private boolean truncate;

  private DatabaseMeta connection;

  public ActionMssqlBulkLoad( String n ) {
    super( n, "" );
    tableName = null;
    schemaname = null;
    filename = null;
    datafiletype = "char";
    fieldterminator = null;
    lineterminated = null;
    codepage = "OEM";
    specificcodepage = null;
    checkconstraints = false;
    keepnulls = false;
    tablock = false;
    startfile = 0;
    endfile = 0;
    orderby = null;

    errorfilename = null;
    adddatetime = false;
    orderdirection = "Asc";
    maxerrors = 0;
    batchsize = 0;
    rowsperbatch = 0;

    connection = null;
    addfiletoresult = false;
    formatfilename = null;
    firetriggers = false;
    keepidentity = false;
    truncate = false;
  }

  public ActionMssqlBulkLoad() {
    this( "" );
  }

  public Object clone() {
    ActionMssqlBulkLoad je = (ActionMssqlBulkLoad) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "schemaname", schemaname ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "tablename", tableName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "filename", filename ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "datafiletype", datafiletype ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "fieldterminator", fieldterminator ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "lineterminated", lineterminated ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "codepage", codepage ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "specificcodepage", specificcodepage ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "formatfilename", formatfilename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "firetriggers", firetriggers ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "checkconstraints", checkconstraints ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "keepnulls", keepnulls ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "keepidentity", keepidentity ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "tablock", tablock ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "startfile", startfile ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "endfile", endfile ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "orderby", orderby ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "orderdirection", orderdirection ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "maxerrors", maxerrors ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "batchsize", batchsize ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "rowsperbatch", rowsperbatch ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "errorfilename", errorfilename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "adddatetime", adddatetime ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "addfiletoresult", addfiletoresult ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "truncate", truncate ) );

    retval.append( "      " ).append(
      XmlHandler.addTagValue( "connection", connection == null ? null : connection.getName() ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      schemaname = XmlHandler.getTagValue( entrynode, "schemaname" );
      tableName = XmlHandler.getTagValue( entrynode, "tablename" );
      filename = XmlHandler.getTagValue( entrynode, "filename" );
      datafiletype = XmlHandler.getTagValue( entrynode, "datafiletype" );
      fieldterminator = XmlHandler.getTagValue( entrynode, "fieldterminator" );

      lineterminated = XmlHandler.getTagValue( entrynode, "lineterminated" );
      codepage = XmlHandler.getTagValue( entrynode, "codepage" );
      specificcodepage = XmlHandler.getTagValue( entrynode, "specificcodepage" );
      formatfilename = XmlHandler.getTagValue( entrynode, "formatfilename" );

      firetriggers = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "firetriggers" ) );
      checkconstraints = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "checkconstraints" ) );
      keepnulls = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "keepnulls" ) );
      keepidentity = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "keepidentity" ) );

      tablock = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "tablock" ) );
      startfile = Const.toInt( XmlHandler.getTagValue( entrynode, "startfile" ), 0 );
      endfile = Const.toInt( XmlHandler.getTagValue( entrynode, "endfile" ), 0 );

      orderby = XmlHandler.getTagValue( entrynode, "orderby" );
      orderdirection = XmlHandler.getTagValue( entrynode, "orderdirection" );

      errorfilename = XmlHandler.getTagValue( entrynode, "errorfilename" );

      maxerrors = Const.toInt( XmlHandler.getTagValue( entrynode, "maxerrors" ), 0 );
      batchsize = Const.toInt( XmlHandler.getTagValue( entrynode, "batchsize" ), 0 );
      rowsperbatch = Const.toInt( XmlHandler.getTagValue( entrynode, "rowsperbatch" ), 0 );
      adddatetime = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "adddatetime" ) );
      String dbname = XmlHandler.getTagValue( entrynode, "connection" );
      addfiletoresult = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "addfiletoresult" ) );
      truncate = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "truncate" ) );

      connection = DatabaseMeta.loadDatabase( metadataProvider, dbname );

    } catch ( HopException e ) {
      throw new HopXmlException( "Unable to load action of type 'MSsql bulk load' from XML node", e );
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

  public void setMaxErrors( int maxerrors ) {
    this.maxerrors = maxerrors;
  }

  public int getMaxErrors() {
    return maxerrors;
  }

  public int getBatchSize() {
    return batchsize;
  }

  public void setBatchSize( int batchsize ) {
    this.batchsize = batchsize;
  }

  public int getRowsPerBatch() {
    return rowsperbatch;
  }

  public void setRowsPerBatch( int rowsperbatch ) {
    this.rowsperbatch = rowsperbatch;
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
    String TakeFirstNbrLines = "";
    String LineTerminatedby = "";
    String FieldTerminatedby = "";
    boolean useFieldSeparator = false;
    String UseCodepage = "";
    String ErrorfileName = "";

    Result result = previousResult;
    result.setResult( false );

    String vfsFilename = resolve( filename );
    FileObject fileObject = null;
    // Let's check the filename ...
    if ( !Utils.isEmpty( vfsFilename ) ) {
      try {
        // User has specified a file, We can continue ...
        //
        // This is running over VFS but we need a normal file.
        // As such, we're going to verify that it's a local file...
        // We're also going to convert VFS FileObject to File
        //
        fileObject = HopVfs.getFileObject( vfsFilename );
        if ( !( fileObject instanceof LocalFile ) ) {
          // MSSQL BUKL INSERT can only use local files, so that's what we limit ourselves to.
          //
          throw new HopException( BaseMessages.getString(
            PKG, "JobMssqlBulkLoad.Error.OnlyLocalFileSupported", vfsFilename ) );
        }

        // Convert it to a regular platform specific file name
        //
        String realFilename = HopVfs.getFilename( fileObject );

        // Here we go... back to the regular scheduled program...
        //
        File file = new File( realFilename );
        if ( file.exists() && file.canRead() ) {
          // User has specified an existing file, We can continue ...
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobMssqlBulkLoad.FileExists.Label", realFilename ) );
          }

          if ( connection != null ) {
            // User has specified a connection, We can continue ...
            Database db = new Database( this, this, connection );

            if ( !"MSSQL".equals(db.getDatabaseMeta().getPluginId()) ) {

           // if ( !( db.getDatabaseMeta().getIDatabase() instanceof MSSQLServerDatabaseMeta ) ) {
            	
              logError( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Error.DbNotMSSQL", connection
                .getDatabaseName() ) );
              return result;
            }
            try {
              db.connect();
              // Get schemaname
              String realSchemaname = resolve( schemaname );
              // Get tablename
              String realTablename = resolve( tableName );

              // Add schemaname (Most the time Schemaname.Tablename)
              if ( schemaname != null ) {
                realTablename = realSchemaname + "." + realTablename;
              }

              if ( db.checkTableExists( realTablename ) ) {
                // The table existe, We can continue ...
                if ( log.isDetailed() ) {
                  logDetailed( BaseMessages.getString( PKG, "JobMssqlBulkLoad.TableExists.Label", realTablename ) );
                }

                // FIELDTERMINATOR
                String Fieldterminator = getRealFieldTerminator();
                if ( Utils.isEmpty( Fieldterminator )
                  && ( datafiletype.equals( "char" ) || datafiletype.equals( "widechar" ) ) ) {
                  logError( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Error.FieldTerminatorMissing" ) );
                  return result;
                } else {
                  if ( datafiletype.equals( "char" ) || datafiletype.equals( "widechar" ) ) {
                    useFieldSeparator = true;
                    FieldTerminatedby = "FIELDTERMINATOR='" + Fieldterminator + "'";
                  }
                }
                // Check Specific Code page
                if ( codepage.equals( "Specific" ) ) {
                  String realCodePage = resolve( codepage );
                  if ( specificcodepage.length() < 0 ) {
                    logError( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Error.SpecificCodePageMissing" ) );
                    return result;

                  } else {
                    UseCodepage = "CODEPAGE = '" + realCodePage + "'";
                  }
                } else {
                  UseCodepage = "CODEPAGE = '" + codepage + "'";
                }

                // Check Error file
                String realErrorFile = resolve( errorfilename );
                if ( realErrorFile != null ) {
                  File errorfile = new File( realErrorFile );
                  if ( errorfile.exists() && !adddatetime ) {
                    // The error file is created when the command is executed. An error occurs if the file already
                    // exists.
                    logError( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Error.ErrorFileExists" ) );
                    return result;
                  }
                  if ( adddatetime ) {
                    // Add date time to filename...
                    SimpleDateFormat daf = new SimpleDateFormat();
                    Date now = new Date();
                    daf.applyPattern( "yyyMMdd_HHmmss" );
                    String d = daf.format( now );

                    ErrorfileName = "ERRORFILE ='" + realErrorFile + "_" + d + "'";
                  } else {
                    ErrorfileName = "ERRORFILE ='" + realErrorFile + "'";
                  }
                }

                // ROWTERMINATOR
                String Rowterminator = getRealLineterminated();
                if ( !Utils.isEmpty( Rowterminator ) ) {
                  LineTerminatedby = "ROWTERMINATOR='" + Rowterminator + "'";
                }

                // Start file at
                if ( startfile > 0 ) {
                  TakeFirstNbrLines = "FIRSTROW=" + startfile;
                }

                // End file at
                if ( endfile > 0 ) {
                  TakeFirstNbrLines = "LASTROW=" + endfile;
                }

                // Truncate table?
                String SqlBulkLoad = "";
                if ( truncate ) {
                  SqlBulkLoad = "TRUNCATE TABLE " + realTablename + ";";
                }

                // Build BULK Command
                SqlBulkLoad =
                  SqlBulkLoad
                    + "BULK INSERT " + realTablename + " FROM " + "'" + realFilename.replace( '\\', '/' )
                    + "'";
                SqlBulkLoad = SqlBulkLoad + " WITH (";
                if ( useFieldSeparator ) {
                  SqlBulkLoad = SqlBulkLoad + FieldTerminatedby;
                } else {
                  SqlBulkLoad = SqlBulkLoad + "DATAFILETYPE ='" + datafiletype + "'";
                }

                if ( LineTerminatedby.length() > 0 ) {
                  SqlBulkLoad = SqlBulkLoad + "," + LineTerminatedby;
                }
                if ( TakeFirstNbrLines.length() > 0 ) {
                  SqlBulkLoad = SqlBulkLoad + "," + TakeFirstNbrLines;
                }
                if ( UseCodepage.length() > 0 ) {
                  SqlBulkLoad = SqlBulkLoad + "," + UseCodepage;
                }
                String realFormatFile = resolve( formatfilename );
                if ( realFormatFile != null ) {
                  SqlBulkLoad = SqlBulkLoad + ", FORMATFILE='" + realFormatFile + "'";
                }
                if ( firetriggers ) {
                  SqlBulkLoad = SqlBulkLoad + ",FIRE_TRIGGERS";
                }
                if ( keepnulls ) {
                  SqlBulkLoad = SqlBulkLoad + ",KEEPNULLS";
                }
                if ( keepidentity ) {
                  SqlBulkLoad = SqlBulkLoad + ",KEEPIDENTITY";
                }
                if ( checkconstraints ) {
                  SqlBulkLoad = SqlBulkLoad + ",CHECK_CONSTRAINTS";
                }
                if ( tablock ) {
                  SqlBulkLoad = SqlBulkLoad + ",TABLOCK";
                }
                if ( orderby != null ) {
                  SqlBulkLoad = SqlBulkLoad + ",ORDER ( " + orderby + " " + orderdirection + ")";
                }
                if ( ErrorfileName.length() > 0 ) {
                  SqlBulkLoad = SqlBulkLoad + ", " + ErrorfileName;
                }
                if ( maxerrors > 0 ) {
                  SqlBulkLoad = SqlBulkLoad + ", MAXERRORS=" + maxerrors;
                }
                if ( batchsize > 0 ) {
                  SqlBulkLoad = SqlBulkLoad + ", BATCHSIZE=" + batchsize;
                }
                if ( rowsperbatch > 0 ) {
                  SqlBulkLoad = SqlBulkLoad + ", ROWS_PER_BATCH=" + rowsperbatch;
                }
                // End of Bulk command
                SqlBulkLoad = SqlBulkLoad + ")";

                try {
                  // Run the SQL
                  db.execStatement( SqlBulkLoad );

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
                } catch ( HopDatabaseException je ) {
                  result.setNrErrors( 1 );
                  logError( "An error occurred executing this action : " + je.getMessage(), je );
                } catch ( HopFileException e ) {
                  logError( "An error occurred executing this action : " + e.getMessage(), e );
                  result.setNrErrors( 1 );
                } finally {
                  if ( db != null ) {
                    db.disconnect();
                    db = null;
                  }
                }
              } else {
                // Of course, the table should have been created already before the bulk load operation
                db.disconnect();
                result.setNrErrors( 1 );
                logError( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Error.TableNotExists", realTablename ) );
              }
            } catch ( HopDatabaseException dbe ) {
              db.disconnect();
              result.setNrErrors( 1 );
              logError( "An error occurred executing this entry: " + dbe.getMessage() );
            }
          } else {
            // No database connection is defined
            result.setNrErrors( 1 );
            logError( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Nodatabase.Label" ) );
          }
        } else {
          // the file doesn't exist
          result.setNrErrors( 1 );
          logError( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Error.FileNotExists", realFilename ) );
        }
      } catch ( Exception e ) {
        // An unexpected error occurred
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "JobMssqlBulkLoad.UnexpectedError.Label" ), e );
      } finally {
        try {
          if ( fileObject != null ) {
            fileObject.close();
          }
        } catch ( Exception e ) {
          // Ignore errors
        }
      }
    } else {
      // No file was specified
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "JobMssqlBulkLoad.Nofilename.Label" ) );
    }
    return result;
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    return new DatabaseMeta[] { connection, };
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public String getFilename() {
    return filename;
  }

  public void setFieldTerminator( String fieldterminator ) {
    this.fieldterminator = fieldterminator;
  }

  public void setLineterminated( String lineterminated ) {
    this.lineterminated = lineterminated;
  }

  public void setCodePage( String codepage ) {
    this.codepage = codepage;
  }

  public String getCodePage() {
    return codepage;
  }

  public void setSpecificCodePage( String specificcodepage ) {
    this.specificcodepage = specificcodepage;
  }

  public String getSpecificCodePage() {
    return specificcodepage;
  }

  public void setFormatFilename( String formatfilename ) {
    this.formatfilename = formatfilename;
  }

  public String getFormatFilename() {
    return formatfilename;
  }

  public String getFieldTerminator() {
    return fieldterminator;
  }

  public String getLineterminated() {
    return lineterminated;
  }

  public String getDataFileType() {
    return datafiletype;
  }

  public void setDataFileType( String datafiletype ) {
    this.datafiletype = datafiletype;
  }

  public String getRealLineterminated() {
    return resolve( getLineterminated() );
  }

  public String getRealFieldTerminator() {
    return resolve( getFieldTerminator() );
  }

  public void setStartFile( int startfile ) {
    this.startfile = startfile;
  }

  public int getStartFile() {
    return startfile;
  }

  public void setEndFile( int endfile ) {
    this.endfile = endfile;
  }

  public int getEndFile() {
    return endfile;
  }

  public void setOrderBy( String orderby ) {
    this.orderby = orderby;
  }

  public String getOrderBy() {
    return orderby;
  }

  public String getOrderDirection() {
    return orderdirection;
  }

  public void setOrderDirection( String orderdirection ) {
    this.orderdirection = orderdirection;
  }

  public void setErrorFilename( String errorfilename ) {
    this.errorfilename = errorfilename;
  }

  public String getErrorFilename() {
    return errorfilename;
  }

  public String getRealOrderBy() {
    return resolve( getOrderBy() );
  }

  public void setAddFileToResult( boolean addfiletoresultin ) {
    this.addfiletoresult = addfiletoresultin;
  }

  public boolean isAddFileToResult() {
    return addfiletoresult;
  }

  public void setTruncate( boolean truncate ) {
    this.truncate = truncate;
  }

  public boolean isTruncate() {
    return truncate;
  }

  public void setAddDatetime( boolean adddatetime ) {
    this.adddatetime = adddatetime;
  }

  public boolean isAddDatetime() {
    return adddatetime;
  }

  public void setFireTriggers( boolean firetriggers ) {
    this.firetriggers = firetriggers;
  }

  public boolean isFireTriggers() {
    return firetriggers;
  }

  public void setCheckConstraints( boolean checkconstraints ) {
    this.checkconstraints = checkconstraints;
  }

  public boolean isCheckConstraints() {
    return checkconstraints;
  }

  public void setKeepNulls( boolean keepnulls ) {
    this.keepnulls = keepnulls;
  }

  public boolean isKeepNulls() {
    return keepnulls;
  }

  public void setKeepIdentity( boolean keepidentity ) {
    this.keepidentity = keepidentity;
  }

  public boolean isKeepIdentity() {
    return keepidentity;
  }

  public void setTablock( boolean tablock ) {
    this.tablock = tablock;
  }

  public boolean isTablock() {
    return tablock;
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
    AndValidator.putValidators( ctx, ActionValidatorUtils.notBlankValidator(),
      ActionValidatorUtils.fileExistsValidator() );
    ActionValidatorUtils.andValidator().validate( this, "filename", remarks, ctx );

    ActionValidatorUtils.andValidator().validate( this, "tablename", remarks, AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
  }

}
