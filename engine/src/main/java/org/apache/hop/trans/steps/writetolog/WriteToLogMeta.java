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

package org.apache.hop.trans.steps.writetolog;

import java.util.Arrays;
import java.util.List;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;

import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/*
 * Created on 30-06-2008
 *
 */

public class WriteToLogMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = WriteToLogMeta.class; // for i18n purposes, needed by Translator2!!

  /** by which fields to display? */
  private String[] fieldName;

  public static String[] logLevelCodes = {
    "log_level_nothing", "log_level_error", "log_level_minimal", "log_level_basic", "log_level_detailed",
    "log_level_debug", "log_level_rowlevel" };

  private boolean displayHeader;

  private boolean limitRows;

  private int limitRowsNumber;

  private String logmessage;

  private String loglevel;

  public WriteToLogMeta() {
    super(); // allocate BaseStepMeta
  }

  // For testing purposes only
  public int getLogLevel() {
    return Arrays.asList( logLevelCodes ).indexOf( loglevel );
  }

  public void setLogLevel( int i ) {
    loglevel = logLevelCodes[i];
  }

  public LogLevel getLogLevelByDesc() {
    if ( loglevel == null ) {
      return LogLevel.BASIC;
    }
    LogLevel retval;
    if ( loglevel.equals( logLevelCodes[0] ) ) {
      retval = LogLevel.NOTHING;
    } else if ( loglevel.equals( logLevelCodes[1] ) ) {
      retval = LogLevel.ERROR;
    } else if ( loglevel.equals( logLevelCodes[2] ) ) {
      retval = LogLevel.MINIMAL;
    } else if ( loglevel.equals( logLevelCodes[3] ) ) {
      retval = LogLevel.BASIC;
    } else if ( loglevel.equals( logLevelCodes[4] ) ) {
      retval = LogLevel.DETAILED;
    } else if ( loglevel.equals( logLevelCodes[5] ) ) {
      retval = LogLevel.DEBUG;
    } else {
      retval = LogLevel.ROWLEVEL;
    }
    return retval;
  }

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  @Override
  public Object clone() {
    WriteToLogMeta retval = (WriteToLogMeta) super.clone();

    int nrfields = fieldName.length;

    retval.allocate( nrfields );
    System.arraycopy( fieldName, 0, retval.fieldName, 0, nrfields );

    return retval;
  }

  public void allocate( int nrfields ) {
    fieldName = new String[nrfields];
  }

  /**
   * @return Returns the fieldName.
   */
  public String[] getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName
   *          The fieldName to set.
   */
  public void setFieldName( String[] fieldName ) {
    this.fieldName = fieldName;
  }

  /**
   * @deprecated use {@link #isDisplayHeader()} instead
   * @return
   */
  @Deprecated
  public boolean isdisplayHeader() {
    return isDisplayHeader();
  }

  public boolean isDisplayHeader() {
    return displayHeader;
  }

  /**
   * @deprecated use {@link #setDisplayHeader(boolean)} instead
   * @param displayheader
   */
  @Deprecated
  public void setdisplayHeader( boolean displayheader ) {
    setDisplayHeader( displayheader );
  }

  public void setDisplayHeader( boolean displayheader ) {
    this.displayHeader = displayheader;
  }

  public boolean isLimitRows() {
    return limitRows;
  }

  public void setLimitRows( boolean limitRows ) {
    this.limitRows = limitRows;
  }

  public int getLimitRowsNumber() {
    return limitRowsNumber;
  }

  public void setLimitRowsNumber( int limitRowsNumber ) {
    this.limitRowsNumber = limitRowsNumber;
  }

  public String getLogMessage() {
    if ( logmessage == null ) {
      logmessage = "";
    }
    return logmessage;
  }

  public void setLogMessage( String s ) {
    logmessage = s;
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      loglevel = XMLHandler.getTagValue( stepnode, "loglevel" );
      displayHeader = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "displayHeader" ) );

      limitRows = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "limitRows" ) );
      String limitRowsNumberString = XMLHandler.getTagValue( stepnode, "limitRowsNumber" );
      limitRowsNumber = Const.toInt( limitRowsNumberString, 5 );

      logmessage = XMLHandler.getTagValue( stepnode, "logmessage" );

      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        fieldName[i] = XMLHandler.getTagValue( fnode, "name" );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load step info from XML", e );
    }
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();
    retval.append( "      " + XMLHandler.addTagValue( "loglevel", loglevel ) );
    retval.append( "      " + XMLHandler.addTagValue( "displayHeader", displayHeader ) );
    retval.append( "      " + XMLHandler.addTagValue( "limitRows", limitRows ) );
    retval.append( "      " + XMLHandler.addTagValue( "limitRowsNumber", limitRowsNumber ) );

    retval.append( "      " + XMLHandler.addTagValue( "logmessage", logmessage ) );

    retval.append( "    <fields>" + Const.CR );
    for ( int i = 0; i < fieldName.length; i++ ) {
      retval.append( "      <field>" + Const.CR );
      retval.append( "        " + XMLHandler.addTagValue( "name", fieldName[i] ) );
      retval.append( "        </field>" + Const.CR );
    }
    retval.append( "      </fields>" + Const.CR );

    return retval.toString();
  }

  @Override
  public void setDefault() {
    loglevel = logLevelCodes[3];
    displayHeader = true;
    logmessage = "";

    int nrfields = 0;

    allocate( nrfields );

    for ( int i = 0; i < nrfields; i++ ) {
      fieldName[i] = "field" + i;
    }
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "WriteToLogMeta.CheckResult.NotReceivingFields" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "WriteToLogMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );
      remarks.add( cr );

      String error_message = "";
      boolean error_found = false;

      // Starting from selected fields in ...
      for ( int i = 0; i < fieldName.length; i++ ) {
        int idx = prev.indexOfValue( fieldName[i] );
        if ( idx < 0 ) {
          error_message += "\t\t" + fieldName[i] + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        error_message = BaseMessages.getString( PKG, "WriteToLogMeta.CheckResult.FieldsFound", error_message );

        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
        remarks.add( cr );
      } else {
        if ( fieldName.length > 0 ) {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
              PKG, "WriteToLogMeta.CheckResult.AllFieldsFound" ), stepMeta );
          remarks.add( cr );
        } else {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
              PKG, "WriteToLogMeta.CheckResult.NoFieldsEntered" ), stepMeta );
          remarks.add( cr );
        }
      }

    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "WriteToLogMeta.CheckResult.StepRecevingData2" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "WriteToLogMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
    Trans trans ) {
    return new WriteToLog( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new WriteToLogData();
  }

}
