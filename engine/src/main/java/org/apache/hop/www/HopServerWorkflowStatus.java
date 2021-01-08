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
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.Date;

public class HopServerWorkflowStatus {
  public static final String XML_TAG = "workflow-status";

  private String workflowName;
  private String id;
  private String statusDescription;
  private String errorDescription;
  private String loggingString;
  private int firstLoggingLineNr;
  private int lastLoggingLineNr;
  private Date logDate;

  private Result result;

  public HopServerWorkflowStatus() {
  }

  /**
   * @param pipelineName
   * @param statusDescription
   */
  public HopServerWorkflowStatus( String pipelineName, String id, String statusDescription ) {
    this();
    this.workflowName = pipelineName;
    this.id = id;
    this.statusDescription = statusDescription;
  }

  public String getXml() throws HopException {
    // See PDI-15781
    boolean sendResultXmlWithStatus = EnvUtil.getSystemProperty( "HOP_COMPATIBILITY_SEND_RESULT_XML_WITH_FULL_STATUS", "N" ).equalsIgnoreCase( "Y" );
    StringBuilder xml = new StringBuilder();

    xml.append( XmlHandler.openTag( XML_TAG ) ).append( Const.CR );
    xml.append( "  " ).append( XmlHandler.addTagValue( "workflowname", workflowName ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "id", id ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "status_desc", statusDescription ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "error_desc", errorDescription ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "log_date", XmlHandler.date2string( logDate ) ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "logging_string", XmlHandler.buildCDATA( loggingString ) ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "first_log_line_nr", firstLoggingLineNr ) );
    xml.append( "  " ).append( XmlHandler.addTagValue( "last_log_line_nr", lastLoggingLineNr ) );

    if ( result != null ) {
      String resultXML = sendResultXmlWithStatus ? result.getXml() : result.getBasicXml();
      xml.append( resultXML );
    }

    xml.append( XmlHandler.closeTag( XML_TAG ) );

    return xml.toString();
  }

  public HopServerWorkflowStatus( Node workflowStatusNode ) throws HopException {
    this();
    workflowName = XmlHandler.getTagValue( workflowStatusNode, "workflowname" );
    id = XmlHandler.getTagValue( workflowStatusNode, "id" );
    statusDescription = XmlHandler.getTagValue( workflowStatusNode, "status_desc" );
    errorDescription = XmlHandler.getTagValue( workflowStatusNode, "error_desc" );
    logDate = XmlHandler.stringToDate( XmlHandler.getTagValue( workflowStatusNode, "log_date" ) );
    firstLoggingLineNr = Const.toInt( XmlHandler.getTagValue( workflowStatusNode, "first_log_line_nr" ), 0 );
    lastLoggingLineNr = Const.toInt( XmlHandler.getTagValue( workflowStatusNode, "last_log_line_nr" ), 0 );

    String loggingString64 = XmlHandler.getTagValue( workflowStatusNode, "logging_string" );

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
    Node resultNode = XmlHandler.getSubNode( workflowStatusNode, Result.XML_TAG );
    if ( resultNode != null ) {
      try {
        result = new Result( resultNode );
      } catch ( HopException e ) {
        loggingString +=
          "Unable to serialize result object as XML" + Const.CR + Const.getSimpleStackTrace( e ) + Const.CR + Const.getStackTracker( e ) + Const.CR;
      }
    }
  }

  public static HopServerWorkflowStatus fromXml(String xml ) throws HopException {
    Document document = XmlHandler.loadXmlString( xml );
    HopServerWorkflowStatus status = new HopServerWorkflowStatus( XmlHandler.getSubNode( document, XML_TAG ) );
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
   * @return the workflow name
   */
  public String getWorkflowName() {
    return workflowName;
  }

  /**
   * @param workflowName the workflow name to set
   */
  public void setWorkflowName( String workflowName ) {
    this.workflowName = workflowName;
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
    if (getStatusDescription()==null) {
      return false;
    }
    return getStatusDescription().equalsIgnoreCase( Pipeline.STRING_RUNNING )
      || getStatusDescription().equalsIgnoreCase( Pipeline.STRING_INITIALIZING );
  }

  public boolean isWaiting() {
    if (getStatusDescription()==null) {
      return false;
    }
    return getStatusDescription().equalsIgnoreCase( Pipeline.STRING_WAITING );
  }

  public boolean isFinished() {
    if (getStatusDescription()==null) {
      return false;
    }

    return getStatusDescription().equalsIgnoreCase( Pipeline.STRING_FINISHED )
      || getStatusDescription().equalsIgnoreCase( Pipeline.STRING_FINISHED_WITH_ERRORS );
  }

  public boolean isStopped() {
    if (getStatusDescription()==null) {
      return false;
    }

    return getStatusDescription().equalsIgnoreCase( Pipeline.STRING_STOPPED )
      || getStatusDescription().equalsIgnoreCase( Pipeline.STRING_STOPPED_WITH_ERRORS );
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
