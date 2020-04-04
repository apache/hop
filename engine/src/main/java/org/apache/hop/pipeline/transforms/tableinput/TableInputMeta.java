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

package org.apache.hop.pipeline.transforms.tableinput;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.pipeline.transform.errorhandling.IStream.StreamType;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 2-jun-2003
 *
 */
@InjectionSupported( localizationPrefix = "TableInputMeta.Injection." )
public class TableInputMeta
  extends BaseTransformMeta
  implements ITransformMeta<TableInput, TableInputData> {

  private static Class<?> PKG = TableInputMeta.class; // for i18n purposes, needed by Translator!!

  private IMetaStore metaStore;

  private DatabaseMeta databaseMeta;

  @Injection( name = "SQL" )
  private String sql;

  @Injection( name = "LIMIT" )
  private String rowLimit;

  /**
   * Should I execute once per row?
   */
  @Injection( name = "EXECUTE_FOR_EACH_ROW" )
  private boolean executeEachInputRow;

  @Injection( name = "REPLACE_VARIABLES" )
  private boolean variableReplacementActive;

  @Injection( name = "LAZY_CONVERSION" )
  private boolean lazyConversionActive;

  public TableInputMeta() {
    super();
  }

  @Injection( name = "CONNECTIONNAME" )
  public void setConnection( String connectionName ) {
    try {
      databaseMeta = DatabaseMeta.loadDatabase( metaStore, connectionName );
    } catch ( HopXMLException e ) {
      throw new RuntimeException( "Error loading conneciton '" + connectionName + "'", e );
    }
  }

  /**
   * @return Returns true if the transform should be run per row
   */
  public boolean isExecuteEachInputRow() {
    return executeEachInputRow;
  }

  /**
   * @param oncePerRow true if the transform should be run per row
   */
  public void setExecuteEachInputRow( boolean oncePerRow ) {
    this.executeEachInputRow = oncePerRow;
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
   * @return Returns the rowLimit.
   */
  public String getRowLimit() {
    return rowLimit;
  }

  /**
   * @param rowLimit The rowLimit to set.
   */
  public void setRowLimit( String rowLimit ) {
    this.rowLimit = rowLimit;
  }

  /**
   * @return Returns the sql.
   */
  public String getSQL() {
    return sql;
  }

  /**
   * @param sql The sql to set.
   */
  public void setSQL( String sql ) {
    this.sql = sql;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode, metaStore );
  }

  public Object clone() {
    TableInputMeta retval = (TableInputMeta) super.clone();
    return retval;
  }

  private void readData( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    this.metaStore = metaStore;
    try {
      databaseMeta = DatabaseMeta.loadDatabase( metaStore, XMLHandler.getTagValue( transformNode, "connection" ) );
      sql = XMLHandler.getTagValue( transformNode, "sql" );
      rowLimit = XMLHandler.getTagValue( transformNode, "limit" );

      String lookupFromTransformName = XMLHandler.getTagValue( transformNode, "lookup" );
      IStream infoStream = getTransformIOMeta().getInfoStreams().get( 0 );
      infoStream.setSubject( lookupFromTransformName );

      executeEachInputRow = "Y".equals( XMLHandler.getTagValue( transformNode, "execute_each_row" ) );
      variableReplacementActive = "Y".equals( XMLHandler.getTagValue( transformNode, "variables_active" ) );
      lazyConversionActive = "Y".equals( XMLHandler.getTagValue( transformNode, "lazy_conversion_active" ) );
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load transform info from XML", e );
    }
  }

  public void setDefault() {
    databaseMeta = null;
    sql = "SELECT <values> FROM <table name> WHERE <conditions>";
    rowLimit = "0";
  }

  protected Database getDatabase() {
    // Added for test purposes
    return new Database( loggingObject, databaseMeta );
  }

  public void getFields( IRowMeta row, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IMetaStore metaStore ) throws HopTransformException {
    if ( databaseMeta == null ) {
      return; // TODO: throw an exception here
    }

    boolean param = false;

    Database db = getDatabase();
    super.databases = new Database[] { db }; // keep track of it for canceling purposes...

    // First try without connecting to the database... (can be S L O W)
    String sNewSQL = sql;
    if ( isVariableReplacementActive() ) {
      sNewSQL = db.environmentSubstitute( sql );
      if ( variables != null ) {
        sNewSQL = variables.environmentSubstitute( sNewSQL );
      }
    }

    IRowMeta add = null;
    try {
      add = db.getQueryFields( sNewSQL, param );
    } catch ( HopDatabaseException dbe ) {
      throw new HopTransformException( "Unable to get queryfields for SQL: " + Const.CR + sNewSQL, dbe );
    }

    if ( add != null ) {
      for ( int i = 0; i < add.size(); i++ ) {
        IValueMeta v = add.getValueMeta( i );
        v.setOrigin( origin );
      }
      row.addRowMeta( add );
    } else {
      try {
        db.connect();

        IRowMeta paramRowMeta = null;
        Object[] paramData = null;

        IStream infoStream = getTransformIOMeta().getInfoStreams().get( 0 );
        if ( !Utils.isEmpty( infoStream.getTransformName() ) ) {
          param = true;
          if ( info.length > 0 && info[ 0 ] != null ) {
            paramRowMeta = info[ 0 ];
            paramData = RowDataUtil.allocateRowData( paramRowMeta.size() );
          }
        }

        add = db.getQueryFields( sNewSQL, param, paramRowMeta, paramData );

        if ( add == null ) {
          return;
        }
        for ( int i = 0; i < add.size(); i++ ) {
          IValueMeta v = add.getValueMeta( i );
          v.setOrigin( origin );
        }
        row.addRowMeta( add );
      } catch ( HopException ke ) {
        throw new HopTransformException( "Unable to get queryfields for SQL: " + Const.CR + sNewSQL, ke );
      } finally {
        db.disconnect();
      }
    }
    if ( isLazyConversionActive() ) {
      for ( int i = 0; i < row.size(); i++ ) {
        IValueMeta v = row.getValueMeta( i );
        try {
          if ( v.getType() == IValueMeta.TYPE_STRING ) {
            IValueMeta storageMeta = ValueMetaFactory.cloneValueMeta( v );
            storageMeta.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
            v.setStorageMetadata( storageMeta );
            v.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
          }
        } catch ( HopPluginException e ) {
          throw new HopTransformException( "Unable to clone meta for lazy conversion: " + Const.CR + v, e );
        }
      }
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    "
      + XMLHandler.addTagValue( "connection", databaseMeta == null ? "" : databaseMeta.getName() ) );
    retval.append( "    " + XMLHandler.addTagValue( "sql", sql ) );
    retval.append( "    " + XMLHandler.addTagValue( "limit", rowLimit ) );
    IStream infoStream = getTransformIOMeta().getInfoStreams().get( 0 );
    retval.append( "    " + XMLHandler.addTagValue( "lookup", infoStream.getTransformName() ) );
    retval.append( "    " + XMLHandler.addTagValue( "execute_each_row", executeEachInputRow ) );
    retval.append( "    " + XMLHandler.addTagValue( "variables_active", variableReplacementActive ) );
    retval.append( "    " + XMLHandler.addTagValue( "lazy_conversion_active", lazyConversionActive ) );

    return retval.toString();
  }

  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( databaseMeta != null ) {
      cr = new CheckResult( ICheckResult.TYPE_RESULT_OK, "Connection exists", transformMeta );
      remarks.add( cr );

      Database db = new Database( loggingObject, databaseMeta );
      db.shareVariablesWith( pipelineMeta );
      super.databases = new Database[] { db }; // keep track of it for canceling purposes...

      try {
        db.connect();
        cr = new CheckResult( ICheckResult.TYPE_RESULT_OK, "Connection to database OK", transformMeta );
        remarks.add( cr );

        if ( sql != null && sql.length() != 0 ) {
          cr = new CheckResult( ICheckResult.TYPE_RESULT_OK, "SQL statement is entered", transformMeta );
          remarks.add( cr );
        } else {
          cr = new CheckResult( ICheckResult.TYPE_RESULT_ERROR, "SQL statement is missing.", transformMeta );
          remarks.add( cr );
        }
      } catch ( HopException e ) {
        cr =
          new CheckResult(
            ICheckResult.TYPE_RESULT_ERROR, "An error occurred: " + e.getMessage(), transformMeta );
        remarks.add( cr );
      } finally {
        db.disconnect();
      }
    } else {
      cr =
        new CheckResult(
          ICheckResult.TYPE_RESULT_ERROR, "Please select or create a connection to use", transformMeta );
      remarks.add( cr );
    }

    // See if we have an informative transform...
    IStream infoStream = getTransformIOMeta().getInfoStreams().get( 0 );
    if ( !Utils.isEmpty( infoStream.getTransformName() ) ) {
      boolean found = false;
      for ( int i = 0; i < input.length; i++ ) {
        if ( infoStream.getTransformName().equalsIgnoreCase( input[ i ] ) ) {
          found = true;
        }
      }
      if ( found ) {
        cr =
          new CheckResult( ICheckResult.TYPE_RESULT_OK, "Previous transform to read info from ["
            + infoStream.getTransformName() + "] is found.", transformMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( ICheckResult.TYPE_RESULT_ERROR, "Previous transform to read info from ["
            + infoStream.getTransformName() + "] is not found.", transformMeta );
        remarks.add( cr );
      }

      // Count the number of ? in the SQL string:
      int count = 0;
      for ( int i = 0; i < sql.length(); i++ ) {
        char c = sql.charAt( i );
        if ( c == '\'' ) { // skip to next quote!
          do {
            i++;
            c = sql.charAt( i );
          } while ( c != '\'' );
        }
        if ( c == '?' ) {
          count++;
        }
      }
      // Verify with the number of informative fields...
      if ( info != null ) {
        if ( count == info.size() ) {
          cr =
            new CheckResult( ICheckResult.TYPE_RESULT_OK, "This transform is expecting and receiving "
              + info.size() + " fields of input from the previous transform.", transformMeta );
          remarks.add( cr );
        } else {
          cr =
            new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR, "This transform is receiving "
              + info.size() + " but not the expected " + count
              + " fields of input from the previous transform.", transformMeta );
          remarks.add( cr );
        }
      } else {
        cr =
          new CheckResult(
            ICheckResult.TYPE_RESULT_ERROR, "Input transform name is not recognized!", transformMeta );
        remarks.add( cr );
      }
    } else {
      if ( input.length > 0 ) {
        cr =
          new CheckResult(
            ICheckResult.TYPE_RESULT_ERROR, "Transform is not expecting info from input transforms.", transformMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult(
            ICheckResult.TYPE_RESULT_OK, "No input expected, no input provided.", transformMeta );
        remarks.add( cr );
      }

    }
  }

  /**
   * @param transforms optionally search the info transform in a list of transforms
   */
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    for ( IStream stream : infoStreams ) {
      stream.setTransformMeta( TransformMeta.findTransform( transforms, (String) stream.getSubject() ) );
    }
  }

  public ITransform createTransform( TransformMeta transformMeta, TableInputData data, int cnr,
                                     PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new TableInput( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public TableInputData getTransformData() {
    return new TableInputData();
  }

  @Override
  public void analyseImpact( List<DatabaseImpact> impact, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                             IRowMeta prev, String[] input, String[] output, IRowMeta info,
                             IMetaStore metaStore ) throws HopTransformException {

    // Find the lookupfields...
    IRowMeta out = new RowMeta();
    // TODO: this builds, but does it work in all cases.
    getFields( out, transformMeta.getName(), new IRowMeta[] { info }, null, pipelineMeta, metaStore );

    if ( out != null ) {
      for ( int i = 0; i < out.size(); i++ ) {
        IValueMeta outvalue = out.getValueMeta( i );
        DatabaseImpact ii =
          new DatabaseImpact(
            DatabaseImpact.TYPE_IMPACT_READ, pipelineMeta.getName(), transformMeta.getName(), databaseMeta
            .getDatabaseName(), "", outvalue.getName(), outvalue.getName(), transformMeta.getName(), sql,
            "read from one or more database tables via SQL statement" );
        impact.add( ii );

      }
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
   * @return Returns the variableReplacementActive.
   */
  public boolean isVariableReplacementActive() {
    return variableReplacementActive;
  }

  /**
   * @param variableReplacementActive The variableReplacementActive to set.
   */
  public void setVariableReplacementActive( boolean variableReplacementActive ) {
    this.variableReplacementActive = variableReplacementActive;
  }

  /**
   * @return the lazyConversionActive
   */
  public boolean isLazyConversionActive() {
    return lazyConversionActive;
  }

  /**
   * @param lazyConversionActive the lazyConversionActive to set
   */
  public void setLazyConversionActive( boolean lazyConversionActive ) {
    this.lazyConversionActive = lazyConversionActive;
  }

  /**
   * Returns the Input/Output metadata for this transform. The generator transform only produces output, does not accept input!
   */
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new TransformIOMeta( true, true, false, false, false, false );

      IStream stream =
        new Stream(
          StreamType.INFO, null, BaseMessages.getString( PKG, "TableInputMeta.InfoStream.Description" ),
          StreamIcon.INFO, null );
      ioMeta.addStream( stream );
      setTransformIOMeta( ioMeta );
    }

    return ioMeta;
  }

  public void resetTransformIoMeta() {
    // Do nothing, don't reset as there is no need to do this.
  }

  /**
   * For compatibility, wraps around the standard transform IO metadata
   *
   * @param transformMeta The transform where you read lookup data from
   */
  public void setLookupFromTransform( TransformMeta transformMeta ) {
    getTransformIOMeta().getInfoStreams().get( 0 ).setTransformMeta( transformMeta );
  }

  /**
   * For compatibility, wraps around the standard transform IO metadata
   *
   * @return The transform where you read lookup data from
   */
  public TransformMeta getLookupFromTransform() {
    return getTransformIOMeta().getInfoStreams().get( 0 ).getTransformMeta();
  }
}
