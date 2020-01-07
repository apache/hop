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

package org.apache.hop.trans.steps.tableinput;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;

import org.apache.hop.shared.SharedObjectInterface;
import org.apache.hop.trans.DatabaseImpact;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepIOMeta;
import org.apache.hop.trans.step.StepIOMetaInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.trans.step.errorhandling.Stream;
import org.apache.hop.trans.step.errorhandling.StreamIcon;
import org.apache.hop.trans.step.errorhandling.StreamInterface;
import org.apache.hop.trans.step.errorhandling.StreamInterface.StreamType;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 2-jun-2003
 *
 */
@InjectionSupported( localizationPrefix = "TableInputMeta.Injection." )
public class TableInputMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = TableInputMeta.class; // for i18n purposes, needed by Translator2!!

  private IMetaStore metaStore;

  private DatabaseMeta databaseMeta;

  @Injection( name = "SQL" )
  private String sql;

  @Injection( name = "LIMIT" )
  private String rowLimit;

  /** Should I execute once per row? */
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
      throw new RuntimeException( "Error loading conneciton '"+connectionName+"'", e );
    }
  }

  /**
   * @return Returns true if the step should be run per row
   */
  public boolean isExecuteEachInputRow() {
    return executeEachInputRow;
  }

  /**
   * @param oncePerRow
   *          true if the step should be run per row
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
   * @param database
   *          The database to set.
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
   * @param rowLimit
   *          The rowLimit to set.
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
   * @param sql
   *          The sql to set.
   */
  public void setSQL( String sql ) {
    this.sql = sql;
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode, metaStore );
  }

  public Object clone() {
    TableInputMeta retval = (TableInputMeta) super.clone();
    return retval;
  }

  private void readData( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    this.metaStore = metaStore;
    try {
      databaseMeta = DatabaseMeta.loadDatabase( metaStore, XMLHandler.getTagValue( stepnode, "connection" ) );
      sql = XMLHandler.getTagValue( stepnode, "sql" );
      rowLimit = XMLHandler.getTagValue( stepnode, "limit" );

      String lookupFromStepname = XMLHandler.getTagValue( stepnode, "lookup" );
      StreamInterface infoStream = getStepIOMeta().getInfoStreams().get( 0 );
      infoStream.setSubject( lookupFromStepname );

      executeEachInputRow = "Y".equals( XMLHandler.getTagValue( stepnode, "execute_each_row" ) );
      variableReplacementActive = "Y".equals( XMLHandler.getTagValue( stepnode, "variables_active" ) );
      lazyConversionActive = "Y".equals( XMLHandler.getTagValue( stepnode, "lazy_conversion_active" ) );
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load step info from XML", e );
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

  public void getFields( RowMetaInterface row, String origin, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, IMetaStore metaStore ) throws HopStepException {
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
      if ( space != null ) {
        sNewSQL = space.environmentSubstitute( sNewSQL );
      }
    }

    RowMetaInterface add = null;
    try {
      add = db.getQueryFields( sNewSQL, param );
    } catch ( HopDatabaseException dbe ) {
      throw new HopStepException( "Unable to get queryfields for SQL: " + Const.CR + sNewSQL, dbe );
    }

    if ( add != null ) {
      for ( int i = 0; i < add.size(); i++ ) {
        ValueMetaInterface v = add.getValueMeta( i );
        v.setOrigin( origin );
      }
      row.addRowMeta( add );
    } else {
      try {
        db.connect();

        RowMetaInterface paramRowMeta = null;
        Object[] paramData = null;

        StreamInterface infoStream = getStepIOMeta().getInfoStreams().get( 0 );
        if ( !Utils.isEmpty( infoStream.getStepname() ) ) {
          param = true;
          if ( info.length > 0 && info[0] != null ) {
            paramRowMeta = info[0];
            paramData = RowDataUtil.allocateRowData( paramRowMeta.size() );
          }
        }

        add = db.getQueryFields( sNewSQL, param, paramRowMeta, paramData );

        if ( add == null ) {
          return;
        }
        for ( int i = 0; i < add.size(); i++ ) {
          ValueMetaInterface v = add.getValueMeta( i );
          v.setOrigin( origin );
        }
        row.addRowMeta( add );
      } catch ( HopException ke ) {
        throw new HopStepException( "Unable to get queryfields for SQL: " + Const.CR + sNewSQL, ke );
      } finally {
        db.disconnect();
      }
    }
    if ( isLazyConversionActive() ) {
      for ( int i = 0; i < row.size(); i++ ) {
        ValueMetaInterface v = row.getValueMeta( i );
        try {
          if ( v.getType() == ValueMetaInterface.TYPE_STRING ) {
            ValueMetaInterface storageMeta = ValueMetaFactory.cloneValueMeta( v );
            storageMeta.setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );
            v.setStorageMetadata( storageMeta );
            v.setStorageType( ValueMetaInterface.STORAGE_TYPE_BINARY_STRING );
          }
        } catch ( HopPluginException e ) {
          throw new HopStepException( "Unable to clone meta for lazy conversion: " + Const.CR + v, e );
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
    StreamInterface infoStream = getStepIOMeta().getInfoStreams().get( 0 );
    retval.append( "    " + XMLHandler.addTagValue( "lookup", infoStream.getStepname() ) );
    retval.append( "    " + XMLHandler.addTagValue( "execute_each_row", executeEachInputRow ) );
    retval.append( "    " + XMLHandler.addTagValue( "variables_active", variableReplacementActive ) );
    retval.append( "    " + XMLHandler.addTagValue( "lazy_conversion_active", lazyConversionActive ) );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    IMetaStore metaStore ) {
    CheckResult cr;

    if ( databaseMeta != null ) {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, "Connection exists", stepMeta );
      remarks.add( cr );

      Database db = new Database( loggingObject, databaseMeta );
      db.shareVariablesWith( transMeta );
      super.databases = new Database[] { db }; // keep track of it for canceling purposes...

      try {
        db.connect();
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, "Connection to database OK", stepMeta );
        remarks.add( cr );

        if ( sql != null && sql.length() != 0 ) {
          cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, "SQL statement is entered", stepMeta );
          remarks.add( cr );
        } else {
          cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, "SQL statement is missing.", stepMeta );
          remarks.add( cr );
        }
      } catch ( HopException e ) {
        cr =
          new CheckResult(
            CheckResultInterface.TYPE_RESULT_ERROR, "An error occurred: " + e.getMessage(), stepMeta );
        remarks.add( cr );
      } finally {
        db.disconnect();
      }
    } else {
      cr =
        new CheckResult(
          CheckResultInterface.TYPE_RESULT_ERROR, "Please select or create a connection to use", stepMeta );
      remarks.add( cr );
    }

    // See if we have an informative step...
    StreamInterface infoStream = getStepIOMeta().getInfoStreams().get( 0 );
    if ( !Utils.isEmpty( infoStream.getStepname() ) ) {
      boolean found = false;
      for ( int i = 0; i < input.length; i++ ) {
        if ( infoStream.getStepname().equalsIgnoreCase( input[i] ) ) {
          found = true;
        }
      }
      if ( found ) {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, "Previous step to read info from ["
            + infoStream.getStepname() + "] is found.", stepMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, "Previous step to read info from ["
            + infoStream.getStepname() + "] is not found.", stepMeta );
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
            new CheckResult( CheckResultInterface.TYPE_RESULT_OK, "This step is expecting and receiving "
              + info.size() + " fields of input from the previous step.", stepMeta );
          remarks.add( cr );
        } else {
          cr =
            new CheckResult(
              CheckResultInterface.TYPE_RESULT_ERROR, "This step is receiving "
                + info.size() + " but not the expected " + count
                + " fields of input from the previous step.", stepMeta );
          remarks.add( cr );
        }
      } else {
        cr =
          new CheckResult(
            CheckResultInterface.TYPE_RESULT_ERROR, "Input step name is not recognized!", stepMeta );
        remarks.add( cr );
      }
    } else {
      if ( input.length > 0 ) {
        cr =
          new CheckResult(
            CheckResultInterface.TYPE_RESULT_ERROR, "Step is not expecting info from input steps.", stepMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult(
            CheckResultInterface.TYPE_RESULT_OK, "No input expected, no input provided.", stepMeta );
        remarks.add( cr );
      }

    }
  }

  /**
   * @param steps
   *          optionally search the info step in a list of steps
   */
  public void searchInfoAndTargetSteps( List<StepMeta> steps ) {
    List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();
    for ( StreamInterface stream : infoStreams ) {
      stream.setStepMeta( StepMeta.findStep( steps, (String) stream.getSubject() ) );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans trans ) {
    return new TableInput( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new TableInputData();
  }

  @Override
  public void analyseImpact( List<DatabaseImpact> impact, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info,
    IMetaStore metaStore ) throws HopStepException {

    // if ( stepMeta.getName().equalsIgnoreCase( "cdc_cust" ) ) {
    //   System.out.println( "HERE!" );
    // }

    // Find the lookupfields...
    RowMetaInterface out = new RowMeta();
    // TODO: this builds, but does it work in all cases.
    getFields( out, stepMeta.getName(), new RowMetaInterface[] { info }, null, transMeta, metaStore );

    if ( out != null ) {
      for ( int i = 0; i < out.size(); i++ ) {
        ValueMetaInterface outvalue = out.getValueMeta( i );
        DatabaseImpact ii =
          new DatabaseImpact(
            DatabaseImpact.TYPE_IMPACT_READ, transMeta.getName(), stepMeta.getName(), databaseMeta
              .getDatabaseName(), "", outvalue.getName(), outvalue.getName(), stepMeta.getName(), sql,
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
   * @param variableReplacementActive
   *          The variableReplacementActive to set.
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
   * @param lazyConversionActive
   *          the lazyConversionActive to set
   */
  public void setLazyConversionActive( boolean lazyConversionActive ) {
    this.lazyConversionActive = lazyConversionActive;
  }

  /**
   * Returns the Input/Output metadata for this step. The generator step only produces output, does not accept input!
   */
  public StepIOMetaInterface getStepIOMeta() {
    StepIOMetaInterface ioMeta = super.getStepIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new StepIOMeta( true, true, false, false, false, false );

      StreamInterface stream =
        new Stream(
          StreamType.INFO, null, BaseMessages.getString( PKG, "TableInputMeta.InfoStream.Description" ),
          StreamIcon.INFO, null );
      ioMeta.addStream( stream );
      setStepIOMeta( ioMeta );
    }

    return ioMeta;
  }

  public void resetStepIoMeta() {
    // Do nothing, don't reset as there is no need to do this.
  }

  /**
   * For compatibility, wraps around the standard step IO metadata
   *
   * @param stepMeta
   *          The step where you read lookup data from
   */
  public void setLookupFromStep( StepMeta stepMeta ) {
    getStepIOMeta().getInfoStreams().get( 0 ).setStepMeta( stepMeta );
  }

  /**
   * For compatibility, wraps around the standard step IO metadata
   *
   * @return The step where you read lookup data from
   */
  public StepMeta getLookupFromStep() {
    return getStepIOMeta().getInfoStreams().get( 0 ).getStepMeta();
  }
}
