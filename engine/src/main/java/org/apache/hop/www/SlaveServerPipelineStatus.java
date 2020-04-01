/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.www;

import org.apache.hop.cluster.HttpUtil;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.StepStatus;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SlaveServerPipelineStatus {
  public static final String XML_TAG = "pipeline-status";

  private String id;

  private String pipelineName;

  private String statusDescription;

  private String errorDescription;

  private String loggingString;

  private int firstLoggingLineNr;

  private int lastLoggingLineNr;

  private Date logDate;

  private List<StepStatus> stepStatusList;

  private Result result;

  private boolean paused;

  public SlaveServerPipelineStatus() {
    stepStatusList = new ArrayList<StepStatus>();
  }

  /**
   * @param pipelineName
   * @param statusDescription
   */
  public SlaveServerPipelineStatus( String pipelineName, String id, String statusDescription ) {
    this();
    this.pipelineName = pipelineName;
    this.id = id;
    this.statusDescription = statusDescription;
  }

  public String getXML() throws HopException {
    // See PDI-15781
    boolean sendResultXmlWithStatus = EnvUtil.getSystemProperty( "HOP_COMPATIBILITY_SEND_RESULT_XML_WITH_FULL_STATUS", "N" ).equalsIgnoreCase( "Y" );
    return getXML( sendResultXmlWithStatus );
  }

  public String getXML( boolean sendResultXmlWithStatus ) throws HopException {
    StringBuilder xml = new StringBuilder();

    xml.append( XMLHandler.openTag( XML_TAG ) ).append( Const.CR );
    xml.append( "  " ).append( XMLHandler.addTagValue( "pipeline_name", pipelineName ) );
    xml.append( "  " ).append( XMLHandler.addTagValue( "id", id ) );
    xml.append( "  " ).append( XMLHandler.addTagValue( "status_desc", statusDescription ) );
    xml.append( "  " ).append( XMLHandler.addTagValue( "error_desc", errorDescription ) );
    xml.append( "  " ).append( XMLHandler.addTagValue( "log_date", XMLHandler.date2string( logDate ) ) );
    xml.append( "  " ).append( XMLHandler.addTagValue( "paused", paused ) );

    xml.append( "  " ).append( XMLHandler.openTag( "step_status_list" ) ).append( Const.CR );
    for ( int i = 0; i < stepStatusList.size(); i++ ) {
      StepStatus stepStatus = stepStatusList.get( i );
      xml.append( "    " ).append( stepStatus.getXML() ).append( Const.CR );
    }
    xml.append( "  " ).append( XMLHandler.closeTag( "step_status_list" ) ).append( Const.CR );

    xml.append( "  " ).append( XMLHandler.addTagValue( "first_log_line_nr", firstLoggingLineNr ) );
    xml.append( "  " ).append( XMLHandler.addTagValue( "last_log_line_nr", lastLoggingLineNr ) );

    if ( result != null ) {
      String resultXML = sendResultXmlWithStatus ? result.getXML() : result.getBasicXml();
      xml.append( resultXML );
    }

    xml.append( "  " ).append( XMLHandler.addTagValue( "logging_string", XMLHandler.buildCDATA( loggingString ) ) );

    xml.append( XMLHandler.closeTag( XML_TAG ) );

    return xml.toString();
  }

  public SlaveServerPipelineStatus( Node pipelineStatusNode ) throws HopException {
    this();
    id = XMLHandler.getTagValue( pipelineStatusNode, "id" );
    pipelineName = XMLHandler.getTagValue( pipelineStatusNode, "pipeline_name" );
    statusDescription = XMLHandler.getTagValue( pipelineStatusNode, "status_desc" );
    errorDescription = XMLHandler.getTagValue( pipelineStatusNode, "error_desc" );
    logDate = XMLHandler.stringToDate( XMLHandler.getTagValue( pipelineStatusNode, "log_date" ) );
    paused = "Y".equalsIgnoreCase( XMLHandler.getTagValue( pipelineStatusNode, "paused" ) );

    Node statusListNode = XMLHandler.getSubNode( pipelineStatusNode, "step_status_list" );
    int nr = XMLHandler.countNodes( statusListNode, StepStatus.XML_TAG );
    for ( int i = 0; i < nr; i++ ) {
      Node stepStatusNode = XMLHandler.getSubNodeByNr( statusListNode, StepStatus.XML_TAG, i );
      StepStatus stepStatus = new StepStatus( stepStatusNode );
      stepStatusList.add( stepStatus );
    }

    firstLoggingLineNr = Const.toInt( XMLHandler.getTagValue( pipelineStatusNode, "first_log_line_nr" ), 0 );
    lastLoggingLineNr = Const.toInt( XMLHandler.getTagValue( pipelineStatusNode, "last_log_line_nr" ), 0 );

    String loggingString64 = XMLHandler.getTagValue( pipelineStatusNode, "logging_string" );

    if ( !Utils.isEmpty( loggingString64 ) ) {
      // This is a CDATA block with a Base64 encoded GZIP compressed stream of data.
      //
      String dataString64 =
        loggingString64.substring( "<![CDATA[".length(), loggingString64.length() - "]]>".length() );
      try {
        loggingString = HttpUtil.decodeBase64ZippedString( dataString64 );
      } catch ( IOException e ) {
        loggingString =
          "Unable to decode logging from remote server : " + e.toString() + Const.CR + Const.getStackTracker( e );
      }
    } else {
      loggingString = "";
    }

    // get the result object, if there is any...
    //
    Node resultNode = XMLHandler.getSubNode( pipelineStatusNode, Result.XML_TAG );
    if ( resultNode != null ) {
      try {
        result = new Result( resultNode );
      } catch ( HopException e ) {
        loggingString +=
          "Unable to serialize result object as XML" + Const.CR + Const.getStackTracker( e ) + Const.CR;
      }
      result.setLogText( loggingString );
    }
  }

  public static SlaveServerPipelineStatus fromXML( String xml ) throws HopException {
    Document document = XMLHandler.loadXMLString( xml );
    SlaveServerPipelineStatus status = new SlaveServerPipelineStatus( XMLHandler.getSubNode( document, XML_TAG ) );
    return status;
  }

  /**
   * @return the statusDescription
   */
  public String getStatusDescription() {
    return statusDescription;
  }

  /**
   * @param statusDescription the statusDescription to set
   */
  public void setStatusDescription( String statusDescription ) {
    this.statusDescription = statusDescription;
  }

  /**
   * @return the pipelineName
   */
  public String getPipelineName() {
    return pipelineName;
  }

  /**
   * @param pipelineName the pipelineName to set
   */
  public void setPipelineName( String pipelineName ) {
    this.pipelineName = pipelineName;
  }

  /**
   * @return the errorDescription
   */
  public String getErrorDescription() {
    return errorDescription;
  }

  /**
   * @param errorDescription the errorDescription to set
   */
  public void setErrorDescription( String errorDescription ) {
    this.errorDescription = errorDescription;
  }

  /**
   * @return the stepStatusList
   */
  public List<StepStatus> getStepStatusList() {
    return stepStatusList;
  }

  /**
   * @param stepStatusList the stepStatusList to set
   */
  public void setStepStatusList( List<StepStatus> stepStatusList ) {
    this.stepStatusList = stepStatusList;
  }

  /**
   * @return the loggingString
   */
  public String getLoggingString() {
    return loggingString;
  }

  /**
   * @param loggingString the loggingString to set
   */
  public void setLoggingString( String loggingString ) {
    this.loggingString = loggingString;
  }

  public boolean isRunning() {
    return getStatusDescription().equalsIgnoreCase( Pipeline.STRING_RUNNING )
      || getStatusDescription().equalsIgnoreCase( Pipeline.STRING_INITIALIZING );
  }

  public boolean isStopped() {
    return getStatusDescription().equalsIgnoreCase( Pipeline.STRING_STOPPED )
      || getStatusDescription().equalsIgnoreCase( Pipeline.STRING_STOPPED_WITH_ERRORS );
  }

  public boolean isWaiting() {
    return getStatusDescription().equalsIgnoreCase( Pipeline.STRING_WAITING );
  }

  public boolean isFinished() {
    return getStatusDescription().equalsIgnoreCase( Pipeline.STRING_FINISHED )
      || getStatusDescription().equalsIgnoreCase( Pipeline.STRING_FINISHED_WITH_ERRORS );
  }

  public long getNrStepErrors() {
    long errors = 0L;
    for ( int i = 0; i < stepStatusList.size(); i++ ) {
      StepStatus stepStatus = stepStatusList.get( i );
      errors += stepStatus.getErrors();
    }
    return errors;
  }

  public Result getResult( PipelineMeta pipelineMeta ) {
    Result result = new Result();

    for ( StepStatus stepStatus : stepStatusList ) {

      result.setNrErrors( result.getNrErrors() + stepStatus.getErrors() + ( result.isStopped() ? 1 : 0 ) ); // If the
      // remote
      // pipeline is
      // stopped,
      // count as
      // an error

      if ( stepStatus.getStepname().equals( pipelineMeta.getPipelineLogTable().getStepnameRead() ) ) {
        result.increaseLinesRead( stepStatus.getLinesRead() );
      }
      if ( stepStatus.getStepname().equals( pipelineMeta.getPipelineLogTable().getStepnameInput() ) ) {
        result.increaseLinesInput( stepStatus.getLinesInput() );
      }
      if ( stepStatus.getStepname().equals( pipelineMeta.getPipelineLogTable().getStepnameWritten() ) ) {
        result.increaseLinesWritten( stepStatus.getLinesWritten() );
      }
      if ( stepStatus.getStepname().equals( pipelineMeta.getPipelineLogTable().getStepnameOutput() ) ) {
        result.increaseLinesOutput( stepStatus.getLinesOutput() );
      }
      if ( stepStatus.getStepname().equals( pipelineMeta.getPipelineLogTable().getStepnameUpdated() ) ) {
        result.increaseLinesUpdated( stepStatus.getLinesUpdated() );
      }
      if ( stepStatus.getStepname().equals( pipelineMeta.getPipelineLogTable().getStepnameRejected() ) ) {
        result.increaseLinesRejected( stepStatus.getLinesRejected() );
      }

      if ( stepStatus.isStopped() ) {
        result.setStopped( true );
        result.setResult( false );
      }
    }

    return result;
  }

  /**
   * @return the result
   */
  public Result getResult() {
    return result;
  }

  /**
   * @param result the result to set
   */
  public void setResult( Result result ) {
    this.result = result;
  }

  /**
   * @return the paused
   */
  public boolean isPaused() {
    return paused;
  }

  /**
   * @param paused the paused to set
   */
  public void setPaused( boolean paused ) {
    this.paused = paused;
  }

  /**
   * @return the lastLoggingLineNr
   */
  public int getLastLoggingLineNr() {
    return lastLoggingLineNr;
  }

  /**
   * @param lastLoggingLineNr the lastLoggingLineNr to set
   */
  public void setLastLoggingLineNr( int lastLoggingLineNr ) {
    this.lastLoggingLineNr = lastLoggingLineNr;
  }

  /**
   * @return the firstLoggingLineNr
   */
  public int getFirstLoggingLineNr() {
    return firstLoggingLineNr;
  }

  /**
   * @param firstLoggingLineNr the firstLoggingLineNr to set
   */
  public void setFirstLoggingLineNr( int firstLoggingLineNr ) {
    this.firstLoggingLineNr = firstLoggingLineNr;
  }

  /**
   * @return the logDate
   */
  public Date getLogDate() {
    return logDate;
  }

  /**
   * @param the logDate
   */
  public void setLogDate( Date logDate ) {
    this.logDate = logDate;
  }

  /**
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId( String id ) {
    this.id = id;
  }
}
