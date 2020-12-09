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

package org.apache.hop.www;

import org.apache.hop.server.HttpUtil;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformStatus;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HopServerPipelineStatus {
  public static final String XML_TAG = "pipeline-status";

  private String id;

  private String pipelineName;

  private String statusDescription;

  private String errorDescription;

  private String loggingString;

  private int firstLoggingLineNr;

  private int lastLoggingLineNr;

  private Date logDate;

  private List<TransformStatus> transformStatusList;

  private Result result;

  private boolean paused;

  private Date executionStartDate;
  private Date executionEndDate;

  public HopServerPipelineStatus() {
    transformStatusList = new ArrayList<>();
  }

  /**
   * @param pipelineName
   * @param statusDescription
   */
  public HopServerPipelineStatus( String pipelineName, String id, String statusDescription ) {
    this();
    this.pipelineName = pipelineName;
    this.id = id;
    this.statusDescription = statusDescription;
  }

  public String getXml() throws HopException {
    // See PDI-15781
    boolean sendResultXmlWithStatus = EnvUtil.getSystemProperty( "HOP_COMPATIBILITY_SEND_RESULT_XML_WITH_FULL_STATUS", "N" ).equalsIgnoreCase( "Y" );
    return getXML( sendResultXmlWithStatus );
  }

  public String getXML( boolean sendResultXmlWithStatus ) throws HopException {
    StringBuilder xml = new StringBuilder();

    xml.append( XmlHandler.openTag( XML_TAG ) ).append( Const.CR );
    xml.append( "  " ).append( XmlHandler.addTagValue( "pipeline_name", pipelineName ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "id", id ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "status_desc", statusDescription ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "error_desc", errorDescription ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "log_date", XmlHandler.date2string( logDate ) ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "execution_start_date", XmlHandler.date2string( executionStartDate ) ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "execution_end_date", XmlHandler.date2string( executionEndDate ) ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "paused", paused ) );

    xml.append( "  " ).append( XmlHandler.openTag( "transform_status_list" ) ).append( Const.CR );
    for ( int i = 0; i < transformStatusList.size(); i++ ) {
      TransformStatus transformStatus = transformStatusList.get( i );
      xml.append( "    " ).append( transformStatus.getXml() ).append( Const.CR );
    }
    xml.append( "  " ).append( XmlHandler.closeTag( "transform_status_list" ) ).append( Const.CR );

    xml.append( "  " ).append( XmlHandler.addTagValue( "first_log_line_nr", firstLoggingLineNr ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "last_log_line_nr", lastLoggingLineNr ) );

    if ( result != null ) {
      String resultXML = sendResultXmlWithStatus ? result.getXml() : result.getBasicXml();
      xml.append( resultXML );
    }

    xml.append( "  " ).append( XmlHandler.addTagValue( "logging_string", XmlHandler.buildCDATA( loggingString ) ) );

    xml.append( XmlHandler.closeTag( XML_TAG ) );

    return xml.toString();
  }

  public HopServerPipelineStatus( Node pipelineStatusNode ) throws HopException {
    this();
    id = XmlHandler.getTagValue( pipelineStatusNode, "id" );
    pipelineName = XmlHandler.getTagValue( pipelineStatusNode, "pipeline_name" );
    statusDescription = XmlHandler.getTagValue( pipelineStatusNode, "status_desc" );
    errorDescription = XmlHandler.getTagValue( pipelineStatusNode, "error_desc" );
    logDate = XmlHandler.stringToDate( XmlHandler.getTagValue( pipelineStatusNode, "log_date" ) );
    paused = "Y".equalsIgnoreCase( XmlHandler.getTagValue( pipelineStatusNode, "paused" ) );

    Node statusListNode = XmlHandler.getSubNode( pipelineStatusNode, "transform_status_list" );
    int nr = XmlHandler.countNodes( statusListNode, TransformStatus.XML_TAG );
    for ( int i = 0; i < nr; i++ ) {
      Node transformStatusNode = XmlHandler.getSubNodeByNr( statusListNode, TransformStatus.XML_TAG, i );
      TransformStatus transformStatus = new TransformStatus( transformStatusNode );
      transformStatusList.add( transformStatus );
    }

    firstLoggingLineNr = Const.toInt( XmlHandler.getTagValue( pipelineStatusNode, "first_log_line_nr" ), 0 );
    lastLoggingLineNr = Const.toInt( XmlHandler.getTagValue( pipelineStatusNode, "last_log_line_nr" ), 0 );

    String loggingString64 = XmlHandler.getTagValue( pipelineStatusNode, "logging_string" );

    if ( !Utils.isEmpty( loggingString64 ) ) {
      // This is a CDATA block with a Base64 encoded GZIP compressed stream of data.
      //
      String dataString64 =
        loggingString64.substring( "<![CDATA[".length(), loggingString64.length() - "]]>".length() );
      try {
        loggingString = HttpUtil.decodeBase64ZippedString( dataString64 );
      } catch ( IOException e ) {
        loggingString =
          "Unable to decode logging from remote server : " + e.toString() + Const.CR + Const.getSimpleStackTrace( e ) + Const.CR + Const.getStackTracker( e );
      }
    } else {
      loggingString = "";
    }

    // get the result object, if there is any...
    //
    Node resultNode = XmlHandler.getSubNode( pipelineStatusNode, Result.XML_TAG );
    if ( resultNode != null ) {
      try {
        result = new Result( resultNode );
      } catch ( HopException e ) {
        loggingString +=
          "Unable to serialize result object as XML" + Const.CR + Const.getSimpleStackTrace( e ) + Const.CR + Const.getStackTracker( e ) + Const.CR;
      }
      result.setLogText( loggingString );
    }
  }

  public static HopServerPipelineStatus fromXml(String xml ) throws HopException {
    Document document = XmlHandler.loadXmlString( xml );
    HopServerPipelineStatus status = new HopServerPipelineStatus( XmlHandler.getSubNode( document, XML_TAG ) );
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
   * @return the transformStatusList
   */
  public List<TransformStatus> getTransformStatusList() {
    return transformStatusList;
  }

  /**
   * @param transformStatusList the transformStatusList to set
   */
  public void setTransformStatusList( List<TransformStatus> transformStatusList ) {
    this.transformStatusList = transformStatusList;
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
    return getStatusDescription()!=null && (
         getStatusDescription().equalsIgnoreCase( Pipeline.STRING_RUNNING )
      || getStatusDescription().equalsIgnoreCase( Pipeline.STRING_INITIALIZING )
      || getStatusDescription().equalsIgnoreCase( Pipeline.STRING_PAUSED )
    )
      ;
  }

  public boolean isStopped() {
    return getStatusDescription()!=null && (
         getStatusDescription().equalsIgnoreCase( Pipeline.STRING_STOPPED )
      || getStatusDescription().equalsIgnoreCase( Pipeline.STRING_STOPPED_WITH_ERRORS )
    );
  }

  public boolean isWaiting() {
    return getStatusDescription()!=null && getStatusDescription().equalsIgnoreCase( Pipeline.STRING_WAITING );
  }

  public boolean isFinished() {
    return getStatusDescription()!=null && (
         getStatusDescription().equalsIgnoreCase( Pipeline.STRING_FINISHED )
      || getStatusDescription().equalsIgnoreCase( Pipeline.STRING_FINISHED_WITH_ERRORS )
    );
  }

  public long getNrTransformErrors() {
    long errors = 0L;
    for ( int i = 0; i < transformStatusList.size(); i++ ) {
      TransformStatus transformStatus = transformStatusList.get( i );
      errors += transformStatus.getErrors();
    }
    return errors;
  }

  public Result getResult( PipelineMeta pipelineMeta ) {
    Result result = new Result();

    for ( TransformStatus transformStatus : transformStatusList ) {

      result.setNrErrors( result.getNrErrors() + transformStatus.getErrors() + ( result.isStopped() ? 1 : 0 ) ); // If the
      // remote
      // pipeline is
      // stopped,
      // count as
      // an error

      // For every transform metric, take the maximum amount
      //
      result.setNrLinesRead( Math.max( result.getNrLinesRead(), transformStatus.getLinesRead() ) );
      result.setNrLinesWritten( Math.max( result.getNrLinesWritten(), transformStatus.getLinesWritten() ) );
      result.setNrLinesInput( Math.max( result.getNrLinesInput(), transformStatus.getLinesInput() ) );
      result.setNrLinesOutput( Math.max( result.getNrLinesOutput(), transformStatus.getLinesOutput() ) );
      result.setNrLinesUpdated( Math.max( result.getNrLinesUpdated(), transformStatus.getLinesUpdated() ) );
      result.setNrLinesRejected( Math.max( result.getNrLinesRejected(), transformStatus.getLinesRejected() ) );

      if ( transformStatus.isStopped() ) {
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
   * @param logDate
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

  /**
   * Gets executionStartDate
   *
   * @return value of executionStartDate
   */
  public Date getExecutionStartDate() {
    return executionStartDate;
  }

  /**
   * @param executionStartDate The executionStartDate to set
   */
  public void setExecutionStartDate( Date executionStartDate ) {
    this.executionStartDate = executionStartDate;
  }

  /**
   * Gets executionEndDate
   *
   * @return value of executionEndDate
   */
  public Date getExecutionEndDate() {
    return executionEndDate;
  }

  /**
   * @param executionEndDate The executionEndDate to set
   */
  public void setExecutionEndDate( Date executionEndDate ) {
    this.executionEndDate = executionEndDate;
  }
}
