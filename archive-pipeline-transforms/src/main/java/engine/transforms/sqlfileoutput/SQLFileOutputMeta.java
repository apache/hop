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

package org.apache.hop.pipeline.transforms.sqlfileoutput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/*
 * Created on 26-may-2007
 *
 */

public class SQLFileOutputMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = SQLFileOutputMeta.class; // for i18n purposes, needed by Translator!!

  private DatabaseMeta databaseMeta;
  private String schemaName;
  private String tablename;
  private boolean truncateTable;

  private boolean AddToResult;

  private boolean createTable;

  /**
   * The base name of the output file
   */
  private String fileName;

  /**
   * The file extention in case of a generated filename
   */
  private String extension;

  /**
   * if this value is larger then 0, the text file is split up into parts of this number of lines
   */
  private int splitEvery;

  /**
   * Flag to indicate the we want to append to the end of an existing file (if it exists)
   */
  private boolean fileAppended;

  /**
   * Flag: add the transformnr in the filename
   */
  private boolean transformNrInFilename;

  /**
   * Flag: add the partition number in the filename
   */
  private boolean partNrInFilename;

  /**
   * Flag: add the date in the filename
   */
  private boolean dateInFilename;

  /**
   * Flag: add the time in the filename
   */
  private boolean timeInFilename;

  /**
   * The encoding to use for reading: null or empty string means system default encoding
   */
  private String encoding;

  /**
   * The date format
   */
  private String dateformat;

  /**
   * Start New line for each statement
   */
  private boolean StartNewLine;

  /**
   * Flag: create parent folder if needed
   */
  private boolean createparentfolder;

  private boolean DoNotOpenNewFileInit;

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode, metaStore );
  }

  public Object clone() {

    SQLFileOutputMeta retval = (SQLFileOutputMeta) super.clone();

    return retval;
  }

  /**
   * @return Returns the database.
   */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * @param database The database to set.
   */
  public void setDatabaseMeta( DatabaseMeta database ) {
    this.databaseMeta = database;
  }

  /**
   * @return Returns the extension.
   */
  public String getExtension() {
    return extension;
  }

  /**
   * @param extension The extension to set.
   */
  public void setExtension( String extension ) {
    this.extension = extension;
  }

  /**
   * @return Returns the fileAppended.
   */
  public boolean isFileAppended() {
    return fileAppended;
  }

  /**
   * @param fileAppended The fileAppended to set.
   */
  public void setFileAppended( boolean fileAppended ) {
    this.fileAppended = fileAppended;
  }

  /**
   * @return Returns the fileName.
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * @return Returns the splitEvery.
   */
  public int getSplitEvery() {
    return splitEvery;
  }

  /**
   * @param splitEvery The splitEvery to set.
   */
  public void setSplitEvery( int splitEvery ) {
    this.splitEvery = splitEvery;
  }

  /**
   * @return Returns the transformNrInFilename.
   */
  public boolean isTransformNrInFilename() {
    return transformNrInFilename;
  }

  /**
   * @param transformNrInFilename The transformNrInFilename to set.
   */
  public void setTransformNrInFilename( boolean transformNrInFilename ) {
    this.transformNrInFilename = transformNrInFilename;
  }

  /**
   * @return Returns the timeInFilename.
   */
  public boolean isTimeInFilename() {
    return timeInFilename;
  }

  /**
   * @return Returns the dateInFilename.
   */
  public boolean isDateInFilename() {
    return dateInFilename;
  }

  /**
   * @param dateInFilename The dateInFilename to set.
   */
  public void setDateInFilename( boolean dateInFilename ) {
    this.dateInFilename = dateInFilename;
  }

  /**
   * @param timeInFilename The timeInFilename to set.
   */
  public void setTimeInFilename( boolean timeInFilename ) {
    this.timeInFilename = timeInFilename;
  }

  /**
   * @param fileName The fileName to set.
   */
  public void setFileName( String fileName ) {
    this.fileName = fileName;
  }

  /**
   * @return The desired encoding of output file, null or empty if the default system encoding needs to be used.
   */
  public String getEncoding() {
    return encoding;
  }

  /**
   * @return The desired date format.
   */
  public String getDateFormat() {
    return dateformat;
  }

  /**
   * @param encoding The desired encoding of output file, null or empty if the default system encoding needs to be used.
   */
  public void setEncoding( String encoding ) {
    this.encoding = encoding;
  }

  /**
   * @param dateFormat The desired date format of output field date used.
   */
  public void setDateFormat( String dateFormat ) {
    this.dateformat = dateFormat;
  }

  /**
   * @return Returns the table name.
   */
  public String getTablename() {
    return tablename;
  }

  /**
   * @param tablename The table name to set.
   */
  public void setTablename( String tablename ) {
    this.tablename = tablename;
  }

  /**
   * @return Returns the truncate table flag.
   */
  public boolean truncateTable() {
    return truncateTable;
  }

  /**
   * @return Returns the Add to result filesname flag.
   */
  public boolean AddToResult() {
    return AddToResult;
  }

  /**
   * @return Returns the Start new line flag.
   */
  public boolean StartNewLine() {
    return StartNewLine;
  }

  public boolean isDoNotOpenNewFileInit() {
    return DoNotOpenNewFileInit;
  }

  public void setDoNotOpenNewFileInit( boolean DoNotOpenNewFileInit ) {
    this.DoNotOpenNewFileInit = DoNotOpenNewFileInit;
  }

  /**
   * @return Returns the create table flag.
   */
  public boolean createTable() {
    return createTable;
  }

  /**
   * @param truncateTable The truncate table flag to set.
   */
  public void setTruncateTable( boolean truncateTable ) {
    this.truncateTable = truncateTable;
  }

  /**
   * @param AddToResult The Add file to result to set.
   */
  public void setAddToResult( boolean AddToResult ) {
    this.AddToResult = AddToResult;
  }

  /**
   * @param StartNewLine The Start NEw Line to set.
   */
  public void setStartNewLine( boolean StartNewLine ) {
    this.StartNewLine = StartNewLine;
  }

  /**
   * @param createTable The create table flag to set.
   */
  public void setCreateTable( boolean createTable ) {
    this.createTable = createTable;
  }

  /**
   * @return Returns the create parent folder flag.
   */
  public boolean isCreateParentFolder() {
    return createparentfolder;
  }

  /**
   * @param createparentfolder The create parent folder flag to set.
   */
  public void setCreateParentFolder( boolean createparentfolder ) {
    this.createparentfolder = createparentfolder;
  }

  public String[] getFiles( String fileName ) {
    int copies = 1;
    int splits = 1;
    int parts = 1;

    if ( transformNrInFilename ) {
      copies = 3;
    }

    if ( partNrInFilename ) {
      parts = 3;
    }

    if ( splitEvery != 0 ) {
      splits = 3;
    }

    int nr = copies * parts * splits;
    if ( nr > 1 ) {
      nr++;
    }

    String[] retval = new String[ nr ];

    int i = 0;
    for ( int copy = 0; copy < copies; copy++ ) {
      for ( int part = 0; part < parts; part++ ) {
        for ( int split = 0; split < splits; split++ ) {
          retval[ i ] = buildFilename( fileName, copy, split );
          i++;
        }
      }
    }
    if ( i < nr ) {
      retval[ i ] = "...";
    }

    return retval;
  }

  public String buildFilename( String fileName, int transformnr, int splitnr ) {
    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String retval = fileName;

    Date now = new Date();

    if ( dateInFilename ) {
      daf.applyPattern( "yyyMMdd" );
      String d = daf.format( now );
      retval += "_" + d;
    }
    if ( timeInFilename ) {
      daf.applyPattern( "HHmmss" );
      String t = daf.format( now );
      retval += "_" + t;
    }
    if ( transformNrInFilename ) {
      retval += "_" + transformnr;
    }

    if ( splitEvery > 0 ) {
      retval += "_" + splitnr;
    }

    if ( extension != null && extension.length() != 0 ) {
      retval += "." + getDatabaseMeta().environmentSubstitute( extension );
    }

    return retval;
  }

  private void readData( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {

      String con = XMLHandler.getTagValue( transformNode, "connection" );
      databaseMeta = DatabaseMeta.loadDatabase( metaStore, con );
      schemaName = XMLHandler.getTagValue( transformNode, "schema" );
      tablename = XMLHandler.getTagValue( transformNode, "table" );
      truncateTable = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "truncate" ) );
      createTable = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "create" ) );
      encoding = XMLHandler.getTagValue( transformNode, "encoding" );
      dateformat = XMLHandler.getTagValue( transformNode, "dateformat" );
      AddToResult = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "AddToResult" ) );

      StartNewLine = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "StartNewLine" ) );

      fileName = XMLHandler.getTagValue( transformNode, "file", "name" );
      createparentfolder =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "create_parent_folder" ) );
      extension = XMLHandler.getTagValue( transformNode, "file", "extention" );
      fileAppended = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "append" ) );
      transformNrInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "split" ) );
      partNrInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "haspartno" ) );
      dateInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "add_date" ) );
      timeInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "add_time" ) );
      splitEvery = Const.toInt( XMLHandler.getTagValue( transformNode, "file", "splitevery" ), 0 );
      DoNotOpenNewFileInit =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "DoNotOpenNewFileInit" ) );

    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load transform info from XML", e );
    }
  }

  public void setDefault() {
    databaseMeta = null;
    tablename = "";
    createparentfolder = false;
    DoNotOpenNewFileInit = false;

  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    "
      + XMLHandler.addTagValue( "connection", databaseMeta == null ? "" : databaseMeta.getName() ) );
    retval.append( "    " + XMLHandler.addTagValue( "schema", schemaName ) );
    retval.append( "    " + XMLHandler.addTagValue( "table", tablename ) );
    retval.append( "    " + XMLHandler.addTagValue( "truncate", truncateTable ) );
    retval.append( "    " + XMLHandler.addTagValue( "create", createTable ) );
    retval.append( "    " + XMLHandler.addTagValue( "encoding", encoding ) );
    retval.append( "    " + XMLHandler.addTagValue( "dateformat", dateformat ) );
    retval.append( "    " + XMLHandler.addTagValue( "addtoresult", AddToResult ) );

    retval.append( "    " + XMLHandler.addTagValue( "startnewline", StartNewLine ) );

    retval.append( "    <file>" + Const.CR );
    retval.append( "      " + XMLHandler.addTagValue( "name", fileName ) );
    retval.append( "      " + XMLHandler.addTagValue( "extention", extension ) );
    retval.append( "      " + XMLHandler.addTagValue( "append", fileAppended ) );
    retval.append( "      " + XMLHandler.addTagValue( "split", transformNrInFilename ) );
    retval.append( "      " + XMLHandler.addTagValue( "haspartno", partNrInFilename ) );
    retval.append( "      " + XMLHandler.addTagValue( "add_date", dateInFilename ) );
    retval.append( "      " + XMLHandler.addTagValue( "add_time", timeInFilename ) );
    retval.append( "      " + XMLHandler.addTagValue( "splitevery", splitEvery ) );
    retval.append( "      " + XMLHandler.addTagValue( "create_parent_folder", createparentfolder ) );
    retval.append( "      " + XMLHandler.addTagValue( "DoNotOpenNewFileInit", DoNotOpenNewFileInit ) );

    retval.append( "      </file>" + Const.CR );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    if ( databaseMeta != null ) {
      CheckResult cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SQLFileOutputMeta.CheckResult.ConnectionExists" ), transformMeta );
      remarks.add( cr );

      Database db = new Database( loggingObject, databaseMeta );
      try {
        db.connect();

        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "SQLFileOutputMeta.CheckResult.ConnectionOk" ), transformMeta );
        remarks.add( cr );

        if ( !Utils.isEmpty( tablename ) ) {
          String schemaTable = databaseMeta.getQuotedSchemaTableCombination( schemaName, tablename );
          // Check if this table exists...
          if ( db.checkTableExists( schemaName, tablename ) ) {
            cr =
              new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
                PKG, "SQLFileOutputMeta.CheckResult.TableAccessible", schemaTable ), transformMeta );
            remarks.add( cr );

            IRowMeta r = db.getTableFieldsMeta( schemaName, tablename );
            if ( r != null ) {
              cr =
                new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
                  PKG, "SQLFileOutputMeta.CheckResult.TableOk", schemaTable ), transformMeta );
              remarks.add( cr );

              String error_message = "";
              boolean error_found = false;
              // OK, we have the table fields.
              // Now see what we can find as previous transform...
              if ( prev != null && prev.size() > 0 ) {
                cr =
                  new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
                    PKG, "SQLFileOutputMeta.CheckResult.FieldsReceived", "" + prev.size() ), transformMeta );
                remarks.add( cr );

                // Starting from prev...
                for ( int i = 0; i < prev.size(); i++ ) {
                  IValueMeta pv = prev.getValueMeta( i );
                  int idx = r.indexOfValue( pv.getName() );
                  if ( idx < 0 ) {
                    error_message += "\t\t" + pv.getName() + " (" + pv.getTypeDesc() + ")" + Const.CR;
                    error_found = true;
                  }
                }
                if ( error_found ) {
                  error_message =
                    BaseMessages.getString(
                      PKG, "SQLFileOutputMeta.CheckResult.FieldsNotFoundInOutput", error_message );

                  cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
                  remarks.add( cr );
                } else {
                  cr =
                    new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
                      PKG, "SQLFileOutputMeta.CheckResult.AllFieldsFoundInOutput" ), transformMeta );
                  remarks.add( cr );
                }

                // Starting from table fields in r...
                for ( int i = 0; i < r.size(); i++ ) {
                  IValueMeta rv = r.getValueMeta( i );
                  int idx = prev.indexOfValue( rv.getName() );
                  if ( idx < 0 ) {
                    error_message += "\t\t" + rv.getName() + " (" + rv.getTypeDesc() + ")" + Const.CR;
                    error_found = true;
                  }
                }
                if ( error_found ) {
                  error_message =
                    BaseMessages.getString( PKG, "SQLFileOutputMeta.CheckResult.FieldsNotFound", error_message );

                  cr = new CheckResult( CheckResult.TYPE_RESULT_WARNING, error_message, transformMeta );
                  remarks.add( cr );
                } else {
                  cr =
                    new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
                      PKG, "SQLFileOutputMeta.CheckResult.AllFieldsFound" ), transformMeta );
                  remarks.add( cr );
                }
              } else {
                cr =
                  new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
                    PKG, "SQLFileOutputMeta.CheckResult.NoFields" ), transformMeta );
                remarks.add( cr );
              }
            } else {
              cr =
                new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
                  PKG, "SQLFileOutputMeta.CheckResult.TableNotAccessible" ), transformMeta );
              remarks.add( cr );
            }
          } else {
            cr =
              new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
                PKG, "SQLFileOutputMeta.CheckResult.TableError", schemaTable ), transformMeta );
            remarks.add( cr );
          }
        } else {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
              PKG, "SQLFileOutputMeta.CheckResult.NoTableName" ), transformMeta );
          remarks.add( cr );
        }
      } catch ( HopException e ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "SQLFileOutputMeta.CheckResult.UndefinedError", e.getMessage() ), transformMeta );
        remarks.add( cr );
      } finally {
        db.disconnect();
      }
    } else {
      CheckResult cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SQLFileOutputMeta.CheckResult.NoConnection" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      CheckResult cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SQLFileOutputMeta.CheckResult.ExpectedInputOk" ), transformMeta );
      remarks.add( cr );
    } else {
      CheckResult cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SQLFileOutputMeta.CheckResult.ExpectedInputError" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new SQLFileOutput( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public ITransformData getTransformData() {
    return new SQLFileOutputData();
  }

  public void analyseImpact( List<DatabaseImpact> impact, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                             IRowMeta prev, String[] input, String[] output, IRowMeta info,
                             IMetaStore metaStore ) {
    if ( truncateTable ) {
      DatabaseImpact ii =
        new DatabaseImpact(
          DatabaseImpact.TYPE_IMPACT_TRUNCATE, pipelineMeta.getName(), transformMeta.getName(), databaseMeta
          .getDatabaseName(), tablename, "", "", "", "", "Truncate of table" );
      impact.add( ii );

    }
    // The values that are entering this transform are in "prev":
    if ( prev != null ) {
      for ( int i = 0; i < prev.size(); i++ ) {
        IValueMeta v = prev.getValueMeta( i );
        DatabaseImpact ii =
          new DatabaseImpact(
            DatabaseImpact.TYPE_IMPACT_WRITE, pipelineMeta.getName(), transformMeta.getName(), databaseMeta
            .getDatabaseName(), tablename, v.getName(), v.getName(), v != null ? v.getOrigin() : "?", "",
            "Type = " + v.toStringMeta() );
        impact.add( ii );
      }
    }
  }

  public SQLStatement getSQLStatements( PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev,
                                        IMetaStore metaStore ) {
    SQLStatement retval = new SQLStatement( transformMeta.getName(), databaseMeta, null ); // default: nothing to do!

    if ( databaseMeta != null ) {
      if ( prev != null && prev.size() > 0 ) {
        if ( !Utils.isEmpty( tablename ) ) {
          Database db = new Database( loggingObject, databaseMeta );
          db.shareVariablesWith( pipelineMeta );
          try {
            db.connect();

            String schemaTable = databaseMeta.getQuotedSchemaTableCombination( schemaName, tablename );
            String cr_table = db.getDDL( schemaTable, prev );

            // Empty string means: nothing to do: set it to null...
            if ( cr_table == null || cr_table.length() == 0 ) {
              cr_table = null;
            }

            retval.setSQL( cr_table );
          } catch ( HopDatabaseException dbe ) {
            retval.setError( BaseMessages.getString( PKG, "SQLFileOutputMeta.Error.ErrorConnecting", dbe
              .getMessage() ) );
          } finally {
            db.disconnect();
          }
        } else {
          retval.setError( BaseMessages.getString( PKG, "SQLFileOutputMeta.Exception.TableNotSpecified" ) );
        }
      } else {
        retval.setError( BaseMessages.getString( PKG, "SQLFileOutputMeta.Error.NoInput" ) );
      }
    } else {
      retval.setError( BaseMessages.getString( PKG, "SQLFileOutputMeta.Error.NoConnection" ) );
    }

    return retval;
  }

  public IRowMeta getRequiredFields( iVariables variables ) throws HopException {
    String realTableName = variables.environmentSubstitute( tablename );
    String realSchemaName = variables.environmentSubstitute( schemaName );

    if ( databaseMeta != null ) {
      Database db = new Database( loggingObject, databaseMeta );
      try {
        db.connect();

        if ( !Utils.isEmpty( realTableName ) ) {
          // Check if this table exists...
          if ( db.checkTableExists( realSchemaName, realTableName ) ) {
            return db.getTableFieldsMeta( realSchemaName, realTableName );
          } else {
            throw new HopException( BaseMessages.getString( PKG, "SQLFileOutputMeta.Exception.TableNotFound" ) );
          }
        } else {
          throw new HopException( BaseMessages.getString( PKG, "SQLFileOutputMeta.Exception.TableNotSpecified" ) );
        }
      } catch ( Exception e ) {
        throw new HopException(
          BaseMessages.getString( PKG, "SQLFileOutputMeta.Exception.ErrorGettingFields" ), e );
      } finally {
        db.disconnect();
      }
    } else {
      throw new HopException( BaseMessages.getString( PKG, "SQLFileOutputMeta.Exception.ConnectionNotDefined" ) );
    }
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if ( databaseMeta != null ) {
      return new DatabaseMeta[] { databaseMeta };
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  /**
   * @return the schemaName
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * @param schemaName the schemaName to set
   */
  public void setSchemaName( String schemaName ) {
    this.schemaName = schemaName;
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Since the exported pipeline that runs this will reside in a ZIP file, we can't reference files relatively. So
   * what this does is turn the name of files into absolute paths OR it simply includes the resource in the ZIP file.
   * For now, we'll simply turn it into an absolute path and pray that the file is on a shared drive or something like
   * that.
   *
   * @param variables                   the variable space to use
   * @param definitions
   * @param iResourceNaming
   * @param metaStore               the metaStore in which non-kettle metadata could reside.
   * @return the filename of the exported resource
   */
  public String exportResources( iVariables variables, Map<String, ResourceDefinition> definitions,
                                 IResourceNaming iResourceNaming, IMetaStore metaStore ) throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      //
      // From : ${Internal.Pipeline.Filename.Directory}/../foo/bar.data
      // To : /home/matt/test/files/foo/bar.data
      //
      FileObject fileObject = HopVFS.getFileObject( variables.environmentSubstitute( fileName ), variables );

      // If the file doesn't exist, forget about this effort too!
      //
      if ( fileObject.exists() ) {
        // Convert to an absolute path...
        //
        fileName = iResourceNaming.nameResource( fileObject, variables, true );

        return fileName;
      }
      return null;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

}
