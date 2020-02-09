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

package org.apache.hop.trans.steps.addsequence;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Meta data for the Add Sequence step.
 * <p>
 * Created on 13-may-2003
 */
public class AddSequenceMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = AddSequenceMeta.class; // for i18n purposes, needed by Translator2!!

  private String valuename;

  private boolean useDatabase;
  private DatabaseMeta databaseMeta;
  private String schemaName;
  private String sequenceName;

  private boolean useCounter;
  private String counterName;
  private String startAt;
  private String incrementBy;
  private String maxValue;

  /**
   * @return Returns the connection.
   */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * @param connection The connection to set.
   */
  public void setDatabaseMeta( DatabaseMeta connection ) {
    this.databaseMeta = connection;
  }

  /**
   * @return Returns the incrementBy.
   */
  public String getIncrementBy() {
    return incrementBy;
  }

  /**
   * @param incrementBy The incrementBy to set.
   */
  public void setIncrementBy( String incrementBy ) {
    this.incrementBy = incrementBy;
  }

  /**
   * @return Returns the maxValue.
   */
  public String getMaxValue() {
    return maxValue;
  }

  /**
   * @param maxValue The maxValue to set.
   */
  public void setMaxValue( String maxValue ) {
    this.maxValue = maxValue;
  }

  /**
   * @return Returns the sequenceName.
   */
  public String getSequenceName() {
    return sequenceName;
  }

  /**
   * @param sequenceName The sequenceName to set.
   */
  public void setSequenceName( String sequenceName ) {
    this.sequenceName = sequenceName;
  }

  /**
   * @param maxValue The maxValue to set.
   */
  public void setMaxValue( long maxValue ) {
    this.maxValue = Long.toString( maxValue );
  }

  /**
   * @param startAt The starting point of the sequence to set.
   */
  public void setStartAt( long startAt ) {
    this.startAt = Long.toString( startAt );
  }

  /**
   * @param incrementBy The incrementBy to set.
   */
  public void setIncrementBy( long incrementBy ) {
    this.incrementBy = Long.toString( incrementBy );
  }

  /**
   * @return Returns the start of the sequence.
   */
  public String getStartAt() {
    return startAt;
  }

  /**
   * @param startAt The starting point of the sequence to set.
   */
  public void setStartAt( String startAt ) {
    this.startAt = startAt;
  }

  /**
   * @return Returns the useCounter.
   */
  public boolean isCounterUsed() {
    return useCounter;
  }

  /**
   * @param useCounter The useCounter to set.
   */
  public void setUseCounter( boolean useCounter ) {
    this.useCounter = useCounter;
  }

  /**
   * @return Returns the useDatabase.
   */
  public boolean isDatabaseUsed() {
    return useDatabase;
  }

  /**
   * @param useDatabase The useDatabase to set.
   */
  public void setUseDatabase( boolean useDatabase ) {
    this.useDatabase = useDatabase;
  }

  /**
   * @return Returns the valuename.
   */
  public String getValuename() {
    return valuename;
  }

  /**
   * @param valuename The valuename to set.
   */
  public void setValuename( String valuename ) {
    this.valuename = valuename;
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    try {
      valuename = XMLHandler.getTagValue( stepnode, "valuename" );

      useDatabase = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "use_database" ) );
      String conn = XMLHandler.getTagValue( stepnode, "connection" );
      databaseMeta = DatabaseMeta.loadDatabase( metaStore, conn );
      schemaName = XMLHandler.getTagValue( stepnode, "schema" );
      sequenceName = XMLHandler.getTagValue( stepnode, "seqname" );

      useCounter = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "use_counter" ) );
      counterName = XMLHandler.getTagValue( stepnode, "counter_name" );
      startAt = XMLHandler.getTagValue( stepnode, "start_at" );
      incrementBy = XMLHandler.getTagValue( stepnode, "increment_by" );
      maxValue = XMLHandler.getTagValue( stepnode, "max_value" );

      // TODO startAt = Const.toLong(XMLHandler.getTagValue(stepnode, "start_at"), 1);
      // incrementBy = Const.toLong(XMLHandler.getTagValue(stepnode, "increment_by"), 1);
      // maxValue = Const.toLong(XMLHandler.getTagValue(stepnode, "max_value"), 999999999L);
    } catch ( Exception e ) {
      throw new HopXMLException(
        BaseMessages.getString( PKG, "AddSequenceMeta.Exception.ErrorLoadingStepInfo" ), e );
    }
  }

  @Override
  public void setDefault() {
    valuename = "valuename";

    useDatabase = false;
    schemaName = "";
    sequenceName = "SEQ_";
    databaseMeta = null;

    useCounter = true;
    counterName = null;
    startAt = "1";
    incrementBy = "1";
    maxValue = "999999999";
  }

  @Override
  public void getFields( RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    ValueMetaInterface v = new ValueMetaInteger( valuename );
    // v.setLength(ValueMetaInterface.DEFAULT_INTEGER_LENGTH, 0); Removed for 2.5.x compatibility reasons.
    v.setOrigin( name );
    row.addValueMeta( v );
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "      " ).append( XMLHandler.addTagValue( "valuename", valuename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "use_database", useDatabase ) );
    retval
      .append( "      " ).append( XMLHandler.addTagValue( "connection", databaseMeta == null ? "" : databaseMeta.getName() ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "schema", schemaName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "seqname", sequenceName ) );

    retval.append( "      " ).append( XMLHandler.addTagValue( "use_counter", useCounter ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "counter_name", counterName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "start_at", startAt ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "increment_by", incrementBy ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "max_value", maxValue ) );

    return retval.toString();
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( useDatabase ) {
      Database db = new Database( loggingObject, databaseMeta );
      db.shareVariablesWith( transMeta );
      try {
        db.connect();
        if ( db.checkSequenceExists( transMeta.environmentSubstitute( schemaName ), transMeta
          .environmentSubstitute( sequenceName ) ) ) {
          cr =
            new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
              PKG, "AddSequenceMeta.CheckResult.SequenceExists.Title" ), stepMeta );
        } else {
          cr =
            new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
              PKG, "AddSequenceMeta.CheckResult.SequenceCouldNotBeFound.Title", sequenceName ), stepMeta );
        }
      } catch ( HopException e ) {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "AddSequenceMeta.CheckResult.UnableToConnectDB.Title" )
            + Const.CR + e.getMessage(), stepMeta );
      } finally {
        db.disconnect();
      }
      remarks.add( cr );
    }

    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "AddSequenceMeta.CheckResult.StepIsReceving.Title" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "AddSequenceMeta.CheckResult.NoInputReceived.Title" ), stepMeta );
      remarks.add( cr );
    }
  }

  @Override
  public SQLStatement getSQLStatements( TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
                                        IMetaStore metaStore ) {
    SQLStatement retval = new SQLStatement( stepMeta.getName(), databaseMeta, null ); // default: nothing to do!

    if ( useDatabase ) {
      // Otherwise, don't bother!
      if ( databaseMeta != null ) {
        Database db = new Database( loggingObject, databaseMeta );
        db.shareVariablesWith( transMeta );
        try {
          db.connect();
          if ( !db.checkSequenceExists( schemaName, sequenceName ) ) {
            String cr_table = db.getCreateSequenceStatement( sequenceName, startAt, incrementBy, maxValue, true );
            retval.setSQL( cr_table );
          } else {
            retval.setSQL( null ); // Empty string means: nothing to do: set it to null...
          }
        } catch ( HopException e ) {
          retval.setError( BaseMessages.getString( PKG, "AddSequenceMeta.ErrorMessage.UnableToConnectDB" )
            + Const.CR + e.getMessage() );
        } finally {
          db.disconnect();
        }
      } else {
        retval.setError( BaseMessages.getString( PKG, "AddSequenceMeta.ErrorMessage.NoConnectionDefined" ) );
      }
    }

    return retval;
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                TransMeta transMeta, Trans trans ) {
    return new AddSequence( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new AddSequenceData();
  }

  @Override
  public DatabaseMeta[] getUsedDatabaseConnections() {
    if ( databaseMeta != null ) {
      return new DatabaseMeta[] { databaseMeta };
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  /**
   * @return the counterName
   */
  public String getCounterName() {
    return counterName;
  }

  /**
   * @param counterName the counterName to set
   */
  public void setCounterName( String counterName ) {
    this.counterName = counterName;
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
}
