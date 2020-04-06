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

package org.apache.hop.pipeline.transforms.writetolog;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;

/*
 * Created on 30-06-2008
 *
 */

public class WriteToLogMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = WriteToLogMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * by which fields to display?
   */
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
    super(); // allocate BaseTransformMeta
  }

  // For testing purposes only
  public int getLogLevel() {
    return Arrays.asList( logLevelCodes ).indexOf( loglevel );
  }

  public void setLogLevel( int i ) {
    loglevel = logLevelCodes[ i ];
  }

  public LogLevel getLogLevelByDesc() {
    if ( loglevel == null ) {
      return LogLevel.BASIC;
    }
    LogLevel retval;
    if ( loglevel.equals( logLevelCodes[ 0 ] ) ) {
      retval = LogLevel.NOTHING;
    } else if ( loglevel.equals( logLevelCodes[ 1 ] ) ) {
      retval = LogLevel.ERROR;
    } else if ( loglevel.equals( logLevelCodes[ 2 ] ) ) {
      retval = LogLevel.MINIMAL;
    } else if ( loglevel.equals( logLevelCodes[ 3 ] ) ) {
      retval = LogLevel.BASIC;
    } else if ( loglevel.equals( logLevelCodes[ 4 ] ) ) {
      retval = LogLevel.DETAILED;
    } else if ( loglevel.equals( logLevelCodes[ 5 ] ) ) {
      retval = LogLevel.DEBUG;
    } else {
      retval = LogLevel.ROWLEVEL;
    }
    return retval;
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  @Override
  public Object clone() {
    WriteToLogMeta retval = (WriteToLogMeta) super.clone();

    int nrFields = fieldName.length;

    retval.allocate( nrFields );
    System.arraycopy( fieldName, 0, retval.fieldName, 0, nrFields );

    return retval;
  }

  public void allocate( int nrFields ) {
    fieldName = new String[ nrFields ];
  }

  /**
   * @return Returns the fieldName.
   */
  public String[] getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName The fieldName to set.
   */
  public void setFieldName( String[] fieldName ) {
    this.fieldName = fieldName;
  }

  /**
   * @return
   * @deprecated use {@link #isDisplayHeader()} instead
   */
  @Deprecated
  public boolean isdisplayHeader() {
    return isDisplayHeader();
  }

  public boolean isDisplayHeader() {
    return displayHeader;
  }

  /**
   * @param displayheader
   * @deprecated use {@link #setDisplayHeader(boolean)} instead
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

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      loglevel = XMLHandler.getTagValue( transformNode, "loglevel" );
      displayHeader = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "displayHeader" ) );

      limitRows = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "limitRows" ) );
      String limitRowsNumberString = XMLHandler.getTagValue( transformNode, "limitRowsNumber" );
      limitRowsNumber = Const.toInt( limitRowsNumberString, 5 );

      logmessage = XMLHandler.getTagValue( transformNode, "logmessage" );

      Node fields = XMLHandler.getSubNode( transformNode, "fields" );
      int nrFields = XMLHandler.countNodes( fields, "field" );

      allocate( nrFields );

      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        fieldName[ i ] = XMLHandler.getTagValue( fnode, "name" );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load transform info from XML", e );
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
      retval.append( "        " + XMLHandler.addTagValue( "name", fieldName[ i ] ) );
      retval.append( "        </field>" + Const.CR );
    }
    retval.append( "      </fields>" + Const.CR );

    return retval.toString();
  }

  @Override
  public void setDefault() {
    loglevel = logLevelCodes[ 3 ];
    displayHeader = true;
    logmessage = "";

    int nrFields = 0;

    allocate( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      fieldName[ i ] = "field" + i;
    }
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "WriteToLogMeta.CheckResult.NotReceivingFields" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "WriteToLogMeta.CheckResult.TransformRecevingData", prev.size() + "" ), transformMeta );
      remarks.add( cr );

      String error_message = "";
      boolean error_found = false;

      // Starting from selected fields in ...
      for ( int i = 0; i < fieldName.length; i++ ) {
        int idx = prev.indexOfValue( fieldName[ i ] );
        if ( idx < 0 ) {
          error_message += "\t\t" + fieldName[ i ] + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        error_message = BaseMessages.getString( PKG, "WriteToLogMeta.CheckResult.FieldsFound", error_message );

        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
        remarks.add( cr );
      } else {
        if ( fieldName.length > 0 ) {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
              PKG, "WriteToLogMeta.CheckResult.AllFieldsFound" ), transformMeta );
          remarks.add( cr );
        } else {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
              PKG, "WriteToLogMeta.CheckResult.NoFieldsEntered" ), transformMeta );
          remarks.add( cr );
        }
      }

    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "WriteToLogMeta.CheckResult.TransformRecevingData2" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "WriteToLogMeta.CheckResult.NoInputReceivedFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    }
  }

  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new WriteToLog( transformMeta, this, data, cnr, tr, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new WriteToLogData();
  }

}
