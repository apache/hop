/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.www;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.server.HttpUtil;
import org.apache.hop.workflow.action.ActionStatus;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class HopServerWorkflowStatus {
  public static final String XML_TAG = "workflow-status";
  private static final String CONST_ACTION_STATUS = "action_status_list";
  private static final String CONST_LOG_DATE = "log_date";

  @Getter @Setter private String workflowName;
  @Getter @Setter private String id;
  @Getter @Setter private String statusDescription;
  @Getter @Setter private String errorDescription;
  @Getter @Setter private String loggingString;
  @Getter @Setter private int firstLoggingLineNr;
  @Getter @Setter private int lastLoggingLineNr;
  @Getter @Setter private List<ActionStatus> actionStatusList;
  @Getter @Setter private Result result;

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  @Getter
  @Setter
  private Date logDate;

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  @Getter
  @Setter
  private Date executionStartDate;

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  @Getter
  @Setter
  private Date executionEndDate;

  public HopServerWorkflowStatus() {
    logDate = new Date();
    actionStatusList = new ArrayList<>();
  }

  /**
   * @param workflowName
   * @param statusDescription
   */
  public HopServerWorkflowStatus(String workflowName, String id, String statusDescription) {
    this();
    this.workflowName = workflowName;
    this.id = id;
    this.statusDescription = statusDescription;
  }

  @JsonIgnore
  public String getXml() throws HopException {
    boolean sendResultXmlWithStatus =
        EnvUtil.getSystemProperty("HOP_COMPATIBILITY_SEND_RESULT_XML_WITH_FULL_STATUS", "N")
            .equalsIgnoreCase("Y");

    StringBuilder xml = new StringBuilder();

    xml.append(XmlHandler.openTag(XML_TAG)).append(Const.CR);
    xml.append("  ").append(XmlHandler.addTagValue("workflowname", workflowName));
    xml.append("  ").append(XmlHandler.addTagValue("id", id));
    xml.append("  ").append(XmlHandler.addTagValue("status_desc", statusDescription));
    xml.append("  ").append(XmlHandler.addTagValue("error_desc", errorDescription));
    xml.append("  ")
        .append(XmlHandler.addTagValue(CONST_LOG_DATE, XmlHandler.date2string(logDate)));
    xml.append("  ")
        .append(
            XmlHandler.addTagValue(
                "execution_start_date", XmlHandler.date2string(executionStartDate)));
    xml.append("  ")
        .append(
            XmlHandler.addTagValue("execution_end_date", XmlHandler.date2string(executionEndDate)));
    xml.append("  ")
        .append(XmlHandler.addTagValue("logging_string", XmlHandler.buildCDATA(loggingString)));
    xml.append("  ").append(XmlHandler.addTagValue("first_log_line_nr", firstLoggingLineNr));
    xml.append("  ").append(XmlHandler.addTagValue("last_log_line_nr", lastLoggingLineNr));

    xml.append("  ").append(XmlHandler.openTag(CONST_ACTION_STATUS)).append(Const.CR);
    for (ActionStatus actionStatus : actionStatusList) {
      xml.append(actionStatus.getXml()).append(Const.CR);
    }
    xml.append("  ").append(XmlHandler.closeTag(CONST_ACTION_STATUS)).append(Const.CR);

    if (result != null) {
      String resultXML = sendResultXmlWithStatus ? result.getXml() : result.getBasicXml();
      xml.append(resultXML);
    }

    xml.append(XmlHandler.closeTag(XML_TAG));

    return xml.toString();
  }

  public HopServerWorkflowStatus(Node workflowStatusNode) throws HopException {
    this();
    workflowName = XmlHandler.getTagValue(workflowStatusNode, "workflowname");
    id = XmlHandler.getTagValue(workflowStatusNode, "id");
    statusDescription = XmlHandler.getTagValue(workflowStatusNode, "status_desc");
    errorDescription = XmlHandler.getTagValue(workflowStatusNode, "error_desc");
    logDate = XmlHandler.stringToDate(XmlHandler.getTagValue(workflowStatusNode, CONST_LOG_DATE));
    executionStartDate =
        XmlHandler.stringToDate(XmlHandler.getTagValue(workflowStatusNode, "execution_start_date"));
    executionEndDate =
        XmlHandler.stringToDate(XmlHandler.getTagValue(workflowStatusNode, "execution_end_date"));
    logDate = XmlHandler.stringToDate(XmlHandler.getTagValue(workflowStatusNode, CONST_LOG_DATE));
    firstLoggingLineNr =
        Const.toInt(XmlHandler.getTagValue(workflowStatusNode, "first_log_line_nr"), 0);
    lastLoggingLineNr =
        Const.toInt(XmlHandler.getTagValue(workflowStatusNode, "last_log_line_nr"), 0);

    Node statusListNode = XmlHandler.getSubNode(workflowStatusNode, CONST_ACTION_STATUS);
    int nr = XmlHandler.countNodes(statusListNode, ActionStatus.XML_TAG);
    for (int i = 0; i < nr; i++) {
      Node actionStatusNode = XmlHandler.getSubNodeByNr(statusListNode, ActionStatus.XML_TAG, i);
      ActionStatus actionStatus = new ActionStatus(actionStatusNode);
      actionStatusList.add(actionStatus);
    }

    String loggingString64 = XmlHandler.getTagValue(workflowStatusNode, "logging_string");

    if (!Utils.isEmpty(loggingString64)) {
      // This is a CDATA block with a Base64 encoded GZIP compressed stream of data.
      //
      String dataString64 =
          loggingString64.substring(
              "<![CDATA[".length(), loggingString64.length() - "]]>".length());
      try {
        loggingString = HttpUtil.decodeBase64ZippedString(dataString64);
      } catch (IOException e) {
        loggingString =
            "Unable to decode logging from remote server : "
                + e
                + Const.CR
                + Const.getSimpleStackTrace(e)
                + Const.CR
                + Const.getStackTracker(e);
      }
    } else {
      loggingString = "";
    }

    // get the result object, if there is any...
    //
    Node resultNode = XmlHandler.getSubNode(workflowStatusNode, Result.XML_TAG);
    if (resultNode != null) {
      try {
        result = new Result(resultNode);
      } catch (HopException e) {
        loggingString +=
            "Unable to serialize result object as XML"
                + Const.CR
                + Const.getSimpleStackTrace(e)
                + Const.CR
                + Const.getStackTracker(e)
                + Const.CR;
      }
    }
  }

  public static HopServerWorkflowStatus fromXml(String xml) throws HopException {
    Document document = XmlHandler.loadXmlString(xml);

    return new HopServerWorkflowStatus(XmlHandler.getSubNode(document, XML_TAG));
  }

  public boolean isRunning() {
    if (getStatusDescription() == null) {
      return false;
    }
    return getStatusDescription().equalsIgnoreCase(Pipeline.STRING_RUNNING)
        || getStatusDescription().equalsIgnoreCase(Pipeline.STRING_INITIALIZING);
  }

  public boolean isWaiting() {
    if (getStatusDescription() == null) {
      return false;
    }
    return getStatusDescription().equalsIgnoreCase(Pipeline.STRING_WAITING);
  }

  public boolean isFinished() {
    if (getStatusDescription() == null) {
      return false;
    }

    return getStatusDescription().equalsIgnoreCase(Pipeline.STRING_FINISHED)
        || getStatusDescription().equalsIgnoreCase(Pipeline.STRING_FINISHED_WITH_ERRORS);
  }

  public boolean isStopped() {
    if (getStatusDescription() == null) {
      return false;
    }

    return getStatusDescription().equalsIgnoreCase(Pipeline.STRING_STOPPED)
        || getStatusDescription().equalsIgnoreCase(Pipeline.STRING_STOPPED_WITH_ERRORS);
  }
}
