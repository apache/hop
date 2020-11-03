/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.orabulkloader;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ProvidesDatabaseConnectionInformation;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Created on 20-feb-2007
 *
 * @author Sven Boden
 */
@InjectionSupported( localizationPrefix = "OraBulkLoader.Injection.", groups = { "FIELDS", "DATABASE_FIELDS" } )
public class OraBulkLoaderMeta extends BaseTransformMeta implements ITransform,
  ProvidesDatabaseConnectionInformation {
  private static final Class<?> PKG = OraBulkLoaderMeta.class; // for i18n purposes, needed by Translator!!

  private static int DEFAULT_COMMIT_SIZE = 100000; // The bigger the better for Oracle
  private static int DEFAULT_BIND_SIZE = 0;
  private static int DEFAULT_READ_SIZE = 0;
  private static int DEFAULT_MAX_ERRORS = 50;

  /**
   * database connection
   */
  private DatabaseMeta databaseMeta;
  private IHopMetadataProvider metadataProvider;

  /**
   * what's the schema for the target?
   */
  @Injection( name = "SCHEMA_NAME", group = "FIELDS" )
  private String schemaName;

  /**
   * what's the table for the target?
   */
  @Injection( name = "TABLE_NAME", group = "FIELDS" )
  private String tableName;

  /**
   * Path to the sqlldr utility
   */
  @Injection( name = "SQLLDR_PATH", group = "FIELDS" )
  private String sqlldr;

  /**
   * Path to the control file
   */
  @Injection( name = "CONTROL_FILE", group = "FIELDS" )
  private String controlFile;

  /**
   * Path to the data file
   */
  @Injection( name = "DATA_FILE", group = "FIELDS" )
  private String dataFile;

  /**
   * Path to the log file
   */
  @Injection( name = "LOG_FILE", group = "FIELDS" )
  private String logFile;

  /**
   * Path to the bad file
   */
  @Injection( name = "BAD_FILE", group = "FIELDS" )
  private String badFile;

  /**
   * Path to the discard file
   */
  @Injection( name = "DISCARD_FILE", group = "FIELDS" )
  private String discardFile;

  /**
   * Field value to dateMask after lookup
   */
  @Injection( name = "FIELD_TABLE", group = "DATABASE_FIELDS" )
  private String[] fieldTable;

  /**
   * Field name in the stream
   */
  @Injection( name = "FIELD_STREAM", group = "DATABASE_FIELDS" )
  private String[] fieldStream;

  /**
   * boolean indicating if field needs to be updated
   */
  @Injection( name = "FIELD_DATEMASK", group = "DATABASE_FIELDS" )
  private String[] dateMask;

  /**
   * Commit size (ROWS)
   */
  @Injection( name = "COMMIT_SIZE", group = "FIELDS" )
  private String commitSize;

  /**
   * bindsize
   */
  @Injection( name = "BIND_SIZE", group = "FIELDS" )
  private String bindSize;

  /**
   * readsize
   */
  @Injection( name = "READ_SIZE", group = "FIELDS" )
  private String readSize;

  /**
   * maximum errors
   */
  @Injection( name = "MAX_ERRORS", group = "FIELDS" )
  private String maxErrors;

  /**
   * Load method
   */
  @Injection( name = "LOAD_METHOD", group = "FIELDS" )
  private String loadMethod;

  /**
   * Load action
   */
  @Injection( name = "LOAD_ACTION", group = "FIELDS" )
  private String loadAction;

  /**
   * Encoding to use
   */
  @Injection( name = "ENCODING", group = "FIELDS" )
  private String encoding;

  /**
   * Character set name used for Oracle
   */
  @Injection( name = "ORACLE_CHARSET_NAME", group = "FIELDS" )
  private String characterSetName;

  /**
   * Direct Path?
   */
  @Injection( name = "DIRECT_PATH", group = "FIELDS" )
  private boolean directPath;

  /**
   * Erase files after use
   */
  @Injection( name = "ERASE_FILES", group = "FIELDS" )
  private boolean eraseFiles;

  /**
   * Database name override
   */
  @Injection( name = "DB_NAME_OVERRIDE", group = "FIELDS" )
  private String dbNameOverride;

  /**
   * Fails when sqlldr returns a warning
   **/
  @Injection( name = "FAIL_ON_WARNING", group = "FIELDS" )
  private boolean failOnWarning;

  /**
   * Fails when sqlldr returns anything else than a warning or OK
   **/
  @Injection( name = "FAIL_ON_ERROR", group = "FIELDS" )
  private boolean failOnError;

  /**
   * allow Oracle to load data in parallel
   **/
  @Injection( name = "PARALLEL", group = "FIELDS" )
  private boolean parallel;

  /**
   * If not empty, use this record terminator instead of default one
   **/
  @Injection( name = "RECORD_TERMINATOR", group = "FIELDS" )
  private String altRecordTerm;

  @Injection( name = "CONNECTION_NAME" )
  public void setConnection( String connectionName ) {
    try {
      databaseMeta = DatabaseMeta.loadDatabase( metadataProvider, connectionName );
    } catch ( HopXmlException e ) {
      throw new RuntimeException( "Unable to load connection '" + connectionName + "'", e );
    }
  }

  /*
   * Do not translate following values!!! They are will end up in the workflow export.
   */
  public static final String ACTION_APPEND = "APPEND";
  public static final String ACTION_INSERT = "INSERT";
  public static final String ACTION_REPLACE = "REPLACE";
  public static final String ACTION_TRUNCATE = "TRUNCATE";

  /*
   * Do not translate following values!!! They are will end up in the workflow export.
   */
  public static final String METHOD_AUTO_CONCURRENT = "AUTO_CONCURRENT";
  public static final String METHOD_AUTO_END = "AUTO_END";
  public static final String METHOD_MANUAL = "MANUAL";

  /*
   * Do not translate following values!!! They are will end up in the workflow export.
   */
  public static final String DATE_MASK_DATE = "DATE";
  public static final String DATE_MASK_DATETIME = "DATETIME";

  public OraBulkLoaderMeta() {
    super();
  }

  public int getCommitSizeAsInt( IVariables varSpace ) {
    try {
      return Integer.valueOf( varSpace.environmentSubstitute( getCommitSize() ) );
    } catch ( NumberFormatException ex ) {
      return DEFAULT_COMMIT_SIZE;
    }
  }

  /**
   * @return Returns the commitSize.
   */
  public String getCommitSize() {
    return commitSize;
  }

  /**
   * @param commitSize The commitSize to set.
   */
  public void setCommitSize( String commitSize ) {
    this.commitSize = commitSize;
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
   * @return Returns the tableName.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName The tableName to set.
   */
  public void setTableName( String tableName ) {
    this.tableName = tableName;
  }

  public String getSqlldr() {
    return sqlldr;
  }

  public void setSqlldr( String sqlldr ) {
    this.sqlldr = sqlldr;
  }

  /**
   * @return Returns the fieldTable.
   */
  public String[] getFieldTable() {
    return fieldTable;
  }

  /**
   * @param fieldTable The fieldTable to set.
   */
  public void setFieldTable( String[] fieldTable ) {
    this.fieldTable = fieldTable;
  }

  /**
   * @return Returns the fieldStream.
   */
  public String[] getFieldStream() {
    return fieldStream;
  }

  /**
   * @param fieldStream The fieldStream to set.
   */
  public void setFieldStream( String[] fieldStream ) {
    this.fieldStream = fieldStream;
  }

  public String[] getDateMask() {
    return dateMask;
  }

  public void setDateMask( String[] dateMask ) {
    this.dateMask = dateMask;
  }

  public boolean isFailOnWarning() {
    return failOnWarning;
  }

  public void setFailOnWarning( boolean failOnWarning ) {
    this.failOnWarning = failOnWarning;
  }

  public boolean isFailOnError() {
    return failOnError;
  }

  public void setFailOnError( boolean failOnError ) {
    this.failOnError = failOnError;
  }

  public String getCharacterSetName() {
    return characterSetName;
  }

  public void setCharacterSetName( String characterSetName ) {
    this.characterSetName = characterSetName;
  }

  public String getAltRecordTerm() {
    return altRecordTerm;
  }

  public void setAltRecordTerm( String altRecordTerm ) {
    this.altRecordTerm = altRecordTerm;
  }

  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    readData( transformNode, metadataProvider );
  }

  public void allocate( int nrvalues ) {
    fieldTable = new String[ nrvalues ];
    fieldStream = new String[ nrvalues ];
    dateMask = new String[ nrvalues ];
  }

  public Object clone() {
    OraBulkLoaderMeta retval = (OraBulkLoaderMeta) super.clone();
    int nrvalues = fieldTable.length;

    retval.allocate( nrvalues );
    System.arraycopy( fieldTable, 0, retval.fieldTable, 0, nrvalues );
    System.arraycopy( fieldStream, 0, retval.fieldStream, 0, nrvalues );
    System.arraycopy( dateMask, 0, retval.dateMask, 0, nrvalues );
    return retval;
  }

  private void readData( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    try {
      // String csize, bsize, rsize, serror;
      // int nrvalues;
      this.databases = databases;
      String con = XmlHandler.getTagValue( transformNode, "connection" );
      databaseMeta = DatabaseMeta.loadDatabase( metadataProvider, con );

      commitSize = XmlHandler.getTagValue( transformNode, "commit" );
      if ( Utils.isEmpty( commitSize ) ) {
        commitSize = Integer.toString( DEFAULT_COMMIT_SIZE );
      }

      bindSize = XmlHandler.getTagValue( transformNode, "bind_size" );
      if ( Utils.isEmpty( bindSize ) ) {
        bindSize = Integer.toString( DEFAULT_BIND_SIZE );
      }

      readSize = XmlHandler.getTagValue( transformNode, "read_size" );
      if ( Utils.isEmpty( readSize ) ) {
        readSize = Integer.toString( DEFAULT_READ_SIZE );
      }

      maxErrors = XmlHandler.getTagValue( transformNode, "errors" );
      if ( Utils.isEmpty( maxErrors ) ) {
        maxErrors = Integer.toString( DEFAULT_MAX_ERRORS );
      }

      schemaName = XmlHandler.getTagValue( transformNode, "schema" );
      tableName = XmlHandler.getTagValue( transformNode, "table" );

      loadMethod = XmlHandler.getTagValue( transformNode, "load_method" );
      loadAction = XmlHandler.getTagValue( transformNode, "load_action" );
      sqlldr = XmlHandler.getTagValue( transformNode, "sqlldr" );
      controlFile = XmlHandler.getTagValue( transformNode, "control_file" );
      dataFile = XmlHandler.getTagValue( transformNode, "data_file" );
      logFile = XmlHandler.getTagValue( transformNode, "log_file" );
      badFile = XmlHandler.getTagValue( transformNode, "bad_file" );
      discardFile = XmlHandler.getTagValue( transformNode, "discard_file" );
      directPath = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "direct_path" ) );
      eraseFiles = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "erase_files" ) );
      encoding = XmlHandler.getTagValue( transformNode, "encoding" );
      dbNameOverride = XmlHandler.getTagValue( transformNode, "dbname_override" );

      characterSetName = XmlHandler.getTagValue( transformNode, "character_set" );
      failOnWarning = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "fail_on_warning" ) );
      failOnError = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "fail_on_error" ) );
      parallel = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "parallel" ) );
      altRecordTerm = XmlHandler.getTagValue( transformNode, "alt_rec_term" );

      int nrvalues = XmlHandler.countNodes( transformNode, "mapping" );
      allocate( nrvalues );

      for ( int i = 0; i < nrvalues; i++ ) {
        Node vnode = XmlHandler.getSubNodeByNr( transformNode, "mapping", i );

        fieldTable[ i ] = XmlHandler.getTagValue( vnode, "stream_name" );
        fieldStream[ i ] = XmlHandler.getTagValue( vnode, "field_name" );
        if ( fieldStream[ i ] == null ) {
          fieldStream[ i ] = fieldTable[ i ]; // default: the same name!
        }
        String locDateMask = XmlHandler.getTagValue( vnode, "date_mask" );
        if ( locDateMask == null ) {
          dateMask[ i ] = "";
        } else {
          if ( OraBulkLoaderMeta.DATE_MASK_DATE.equals( locDateMask )
            || OraBulkLoaderMeta.DATE_MASK_DATETIME.equals( locDateMask ) ) {
            dateMask[ i ] = locDateMask;
          } else {
            dateMask[ i ] = "";
          }
        }
      }
    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString(
        PKG, "OraBulkLoaderMeta.Exception.UnableToReadTransformMetaFromXML" ), e );
    }
  }

  public void setDefault() {
    fieldTable = null;
    databaseMeta = null;
    commitSize = Integer.toString( DEFAULT_COMMIT_SIZE );
    bindSize = Integer.toString( DEFAULT_BIND_SIZE ); // Use platform default
    readSize = Integer.toString( DEFAULT_READ_SIZE ); // Use platform default
    maxErrors = Integer.toString( DEFAULT_MAX_ERRORS );
    schemaName = "";
    tableName = BaseMessages.getString( PKG, "OraBulkLoaderMeta.DefaultTableName" );
    loadMethod = METHOD_AUTO_END;
    loadAction = ACTION_APPEND;
    sqlldr = "sqlldr";
    controlFile = "control${Internal.Transform.CopyNr}.cfg";
    dataFile = "load${Internal.Transform.CopyNr}.dat";
    logFile = "";
    badFile = "";
    discardFile = "";
    encoding = "";
    dbNameOverride = "";

    directPath = false;
    eraseFiles = true;

    characterSetName = "";
    failOnWarning = false;
    failOnError = false;
    parallel = false;
    altRecordTerm = "";

    int nrvalues = 0;
    allocate( nrvalues );
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 300 );

    retval
      .append( "    " ).append(
      XmlHandler.addTagValue( "connection", databaseMeta == null ? "" : databaseMeta.getName() ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "commit", commitSize ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "bind_size", bindSize ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "read_size", readSize ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "errors", maxErrors ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "schema", schemaName ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "table", tableName ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "load_method", loadMethod ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "load_action", loadAction ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "sqlldr", sqlldr ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "control_file", controlFile ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "data_file", dataFile ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "log_file", logFile ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "bad_file", badFile ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "discard_file", discardFile ) );

    retval.append( "    " ).append( XmlHandler.addTagValue( "direct_path", directPath ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "erase_files", eraseFiles ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "encoding", encoding ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "dbname_override", dbNameOverride ) );

    retval.append( "    " ).append( XmlHandler.addTagValue( "character_set", characterSetName ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "fail_on_warning", failOnWarning ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "fail_on_error", failOnError ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "parallel", parallel ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "alt_rec_term", altRecordTerm ) );

    for ( int i = 0; i < fieldTable.length; i++ ) {
      retval.append( "      <mapping>" ).append( Const.CR );
      retval.append( "        " ).append( XmlHandler.addTagValue( "stream_name", fieldTable[ i ] ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "field_name", fieldStream[ i ] ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "date_mask", dateMask[ i ] ) );
      retval.append( "      </mapping>" ).append( Const.CR );
    }

    return retval.toString();
  }

  public void getFields( IRowMeta rowMeta, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {
    // Default: nothing changes to rowMeta
  }

  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    CheckResult cr;
    String errorMessage = "";

    if ( databaseMeta != null ) {
      Database db = new Database( loggingObject, databaseMeta );
      db.shareVariablesWith( pipelineMeta );
      try {
        db.connect();

        if ( !Utils.isEmpty( tableName ) ) {
          cr =
            new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
              PKG, "OraBulkLoaderMeta.CheckResult.TableNameOK" ), transformMeta );
          remarks.add( cr );

          boolean first = true;
          boolean errorFound = false;
          errorMessage = "";

          // Check fields in table
          String schemaTable =
            databaseMeta.getQuotedSchemaTableCombination(
              pipelineMeta.environmentSubstitute( schemaName ), pipelineMeta.environmentSubstitute( tableName ) );
          IRowMeta r = db.getTableFields( schemaTable );
          if ( r != null ) {
            cr =
              new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
                PKG, "OraBulkLoaderMeta.CheckResult.TableExists" ), transformMeta );
            remarks.add( cr );

            // How about the fields to insert/dateMask in the table?
            first = true;
            errorFound = false;
            errorMessage = "";

            for ( int i = 0; i < fieldTable.length; i++ ) {
              String field = fieldTable[ i ];

              IValueMeta v = r.searchValueMeta( field );
              if ( v == null ) {
                if ( first ) {
                  first = false;
                  errorMessage +=
                    BaseMessages.getString(
                      PKG, "OraBulkLoaderMeta.CheckResult.MissingFieldsToLoadInTargetTable" )
                      + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + field + Const.CR;
              }
            }
            if ( errorFound ) {
              cr = new CheckResult( ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta );
            } else {
              cr =
                new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
                  PKG, "OraBulkLoaderMeta.CheckResult.AllFieldsFoundInTargetTable" ), transformMeta );
            }
            remarks.add( cr );
          } else {
            errorMessage = BaseMessages.getString( PKG, "OraBulkLoaderMeta.CheckResult.CouldNotReadTableInfo" );
            cr = new CheckResult( ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta );
            remarks.add( cr );
          }
        }

        // Look up fields in the input stream <prev>
        if ( prev != null && prev.size() > 0 ) {
          cr =
            new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
              PKG, "OraBulkLoaderMeta.CheckResult.TransformReceivingDatas", prev.size() + "" ), transformMeta );
          remarks.add( cr );

          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          for ( int i = 0; i < fieldStream.length; i++ ) {
            IValueMeta v = prev.searchValueMeta( fieldStream[ i ] );
            if ( v == null ) {
              if ( first ) {
                first = false;
                errorMessage +=
                  BaseMessages.getString( PKG, "OraBulkLoaderMeta.CheckResult.MissingFieldsInInput" ) + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + fieldStream[ i ] + Const.CR;
            }
          }
          if ( errorFound ) {
            cr = new CheckResult( ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta );
          } else {
            cr =
              new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
                PKG, "OraBulkLoaderMeta.CheckResult.AllFieldsFoundInInput" ), transformMeta );
          }
          remarks.add( cr );
        } else {
          errorMessage =
            BaseMessages.getString( PKG, "OraBulkLoaderMeta.CheckResult.MissingFieldsInInput3" ) + Const.CR;
          cr = new CheckResult( ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta );
          remarks.add( cr );
        }
      } catch ( HopException e ) {
        errorMessage =
          BaseMessages.getString( PKG, "OraBulkLoaderMeta.CheckResult.DatabaseErrorOccurred" ) + e.getMessage();
        cr = new CheckResult( ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta );
        remarks.add( cr );
      } finally {
        db.disconnect();
      }
    } else {
      errorMessage = BaseMessages.getString( PKG, "OraBulkLoaderMeta.CheckResult.InvalidConnection" );
      cr = new CheckResult( ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "OraBulkLoaderMeta.CheckResult.TransformReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "OraBulkLoaderMeta.CheckResult.NoInputError" ), transformMeta );
      remarks.add( cr );
    }
  }

  public SQLStatement getSqlStatements( PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev,
                                        IHopMetadataProvider metadataProvider ) throws HopTransformException {
    SQLStatement retval = new SQLStatement( transformMeta.getName(), databaseMeta, null ); // default: nothing to do!

    if ( databaseMeta != null ) {
      if ( prev != null && prev.size() > 0 ) {
        // Copy the row
        IRowMeta tableFields = new RowMeta();

        // Now change the field names
        for ( int i = 0; i < fieldTable.length; i++ ) {
          IValueMeta v = prev.searchValueMeta( fieldStream[ i ] );
          if ( v != null ) {
            IValueMeta tableField = v.clone();
            tableField.setName( fieldTable[ i ] );
            tableFields.addValueMeta( tableField );
          } else {
            throw new HopTransformException( "Unable to find field [" + fieldStream[ i ] + "] in the input rows" );
          }
        }

        if ( !Utils.isEmpty( tableName ) ) {
          Database db = new Database( loggingObject, databaseMeta );
          db.shareVariablesWith( pipelineMeta );
          try {
            db.connect();

            String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                pipelineMeta.environmentSubstitute( schemaName ), pipelineMeta.environmentSubstitute( tableName ) );
            String sql = db.getDDL( schemaTable, tableFields, null, false, null, true );

            if ( sql.length() == 0 ) {
              retval.setSql( null );
            } else {
              retval.setSql( sql );
            }
          } catch ( HopException e ) {
            retval.setError( BaseMessages.getString( PKG, "OraBulkLoaderMeta.GetSQL.ErrorOccurred" )
              + e.getMessage() );
          }
        } else {
          retval.setError( BaseMessages.getString( PKG, "OraBulkLoaderMeta.GetSQL.NoTableDefinedOnConnection" ) );
        }
      } else {
        retval.setError( BaseMessages.getString( PKG, "OraBulkLoaderMeta.GetSQL.NotReceivingAnyFields" ) );
      }
    } else {
      retval.setError( BaseMessages.getString( PKG, "OraBulkLoaderMeta.GetSQL.NoConnectionDefined" ) );
    }

    return retval;
  }

  public void analyseImpact( List<DatabaseImpact> impact, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                             IRowMeta prev, String[] input, String[] output, IRowMeta info,
                             IHopMetadataProvider metadataProvider ) throws HopTransformException {
    if ( prev != null ) {
      /* DEBUG CHECK THIS */
      // Insert dateMask fields : read/write
      for ( int i = 0; i < fieldTable.length; i++ ) {
        IValueMeta v = prev.searchValueMeta( fieldStream[ i ] );

        DatabaseImpact ii =
          new DatabaseImpact(
            DatabaseImpact.TYPE_IMPACT_READ_WRITE, pipelineMeta.getName(), transformMeta.getName(), databaseMeta
            .getDatabaseName(), pipelineMeta.environmentSubstitute( tableName ), fieldTable[ i ],
            fieldStream[ i ], v != null ? v.getOrigin() : "?", "", "Type = " + v.toStringMeta() );
        impact.add( ii );
      }
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new OraBulkLoader( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public ITransformData getTransformData() {
    return new OraBulkLoaderData();
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if ( databaseMeta != null ) {
      return new DatabaseMeta[] { databaseMeta };
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  /**
   * @return Do we want direct path loading.
   */
  public boolean isDirectPath() {
    return directPath;
  }

  /**
   * @param directPath do we want direct path
   */
  public void setDirectPath( boolean directPath ) {
    this.directPath = directPath;
  }

  public IRowMeta getRequiredFields( IVariables variables ) throws HopException {
    String realTableName = variables.environmentSubstitute( tableName );
    String realSchemaName = variables.environmentSubstitute( schemaName );

    if ( databaseMeta != null ) {
      Database db = new Database( loggingObject, databaseMeta );
      try {
        db.connect();

        if ( !Utils.isEmpty( realTableName ) ) {
          String schemaTable = databaseMeta.getQuotedSchemaTableCombination( realSchemaName, realTableName );

          // Check if this table exists...
          if ( db.checkTableExists( schemaTable ) ) {
            return db.getTableFields( schemaTable );
          } else {
            throw new HopException( BaseMessages.getString( PKG, "OraBulkLoaderMeta.Exception.TableNotFound" ) );
          }
        } else {
          throw new HopException( BaseMessages.getString( PKG, "OraBulkLoaderMeta.Exception.TableNotSpecified" ) );
        }
      } catch ( Exception e ) {
        throw new HopException(
          BaseMessages.getString( PKG, "OraBulkLoaderMeta.Exception.ErrorGettingFields" ), e );
      } finally {
        db.disconnect();
      }
    } else {
      throw new HopException( BaseMessages.getString( PKG, "OraBulkLoaderMeta.Exception.ConnectionNotDefined" ) );
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

  public String getBadFile() {
    return badFile;
  }

  public void setBadFile( String badFile ) {
    this.badFile = badFile;
  }

  public String getControlFile() {
    return controlFile;
  }

  public void setControlFile( String controlFile ) {
    this.controlFile = controlFile;
  }

  public String getDataFile() {
    return dataFile;
  }

  public void setDataFile( String dataFile ) {
    this.dataFile = dataFile;
  }

  public String getDiscardFile() {
    return discardFile;
  }

  public void setDiscardFile( String discardFile ) {
    this.discardFile = discardFile;
  }

  public String getLogFile() {
    return logFile;
  }

  public void setLogFile( String logFile ) {
    this.logFile = logFile;
  }

  public void setLoadAction( String action ) {
    this.loadAction = action;
  }

  public String getLoadAction() {
    return this.loadAction;
  }

  public void setLoadMethod( String method ) {
    this.loadMethod = method;
  }

  public String getLoadMethod() {
    return this.loadMethod;
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding( String encoding ) {
    this.encoding = encoding;
  }

  public String getDelimiter() {
    return ",";
  }

  public String getEnclosure() {
    return "\"";
  }

  public boolean isEraseFiles() {
    return eraseFiles;
  }

  public void setEraseFiles( boolean eraseFiles ) {
    this.eraseFiles = eraseFiles;
  }

  public int getBindSizeAsInt( IVariables varSpace ) {
    try {
      return Integer.valueOf( varSpace.environmentSubstitute( getBindSize() ) );
    } catch ( NumberFormatException ex ) {
      return DEFAULT_BIND_SIZE;
    }
  }

  public String getBindSize() {
    return bindSize;
  }

  public void setBindSize( String bindSize ) {
    this.bindSize = bindSize;
  }

  public int getMaxErrorsAsInt( IVariables varSpace ) {
    try {
      return Integer.valueOf( varSpace.environmentSubstitute( getMaxErrors() ) );
    } catch ( NumberFormatException ex ) {
      return DEFAULT_MAX_ERRORS;
    }
  }

  public String getMaxErrors() {
    return maxErrors;
  }

  public void setMaxErrors( String maxErrors ) {
    this.maxErrors = maxErrors;
  }

  public int getReadSizeAsInt( IVariables varSpace ) {
    try {
      return Integer.valueOf( varSpace.environmentSubstitute( getReadSize() ) );
    } catch ( NumberFormatException ex ) {
      return DEFAULT_READ_SIZE;
    }
  }

  public String getReadSize() {
    return readSize;
  }

  public void setReadSize( String readSize ) {
    this.readSize = readSize;
  }

  public String getDbNameOverride() {
    return dbNameOverride;
  }

  public void setDbNameOverride( String dbNameOverride ) {
    this.dbNameOverride = dbNameOverride;
  }

  /**
   * @return the parallel
   */
  public boolean isParallel() {
    return parallel;
  }

  /**
   * @param parallel the parallel to set
   */
  public void setParallel( boolean parallel ) {
    this.parallel = parallel;
  }

  @Override
  public String getMissingDatabaseConnectionInformationMessage() {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * If we use injection we can have different arrays lengths.
   * We need synchronize them for consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
    if ( fieldTable == null || fieldTable.length == 0 ) {
      return;
    }
    int nrFields = fieldTable.length;
    if ( fieldStream.length < nrFields ) {
      String[] newFieldStream = new String[ nrFields ];
      System.arraycopy( fieldStream, 0, newFieldStream, 0, fieldStream.length );
      fieldStream = newFieldStream;
    }
    for ( int i = 0; i < fieldStream.length; i++ ) {
      if ( fieldStream[ i ] == null ) {
        fieldStream[ i ] = StringUtils.EMPTY;
      }
    }
    //PDI-16472
    if ( dateMask.length < nrFields ) {
      String[] newDateMask = new String[ nrFields ];
      System.arraycopy( dateMask, 0, newDateMask, 0, dateMask.length );
      dateMask = newDateMask;
    }
  }
}
