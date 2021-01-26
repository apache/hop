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

package org.apache.hop.workflow.actions.syslog;

import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.productivity.java.syslog4j.Syslog;
import org.productivity.java.syslog4j.SyslogIF;
import org.w3c.dom.Node;

import java.util.List;

/**
 * This defines a Syslog action.
 *
 * @author Samatar
 * @since 05-01-2010
 */
@Action(
    id = "SYSLOG",
    name = "i18n::ActionSyslog.Name",
    description = "i18n::ActionSyslog.Description",
    image = "Syslog.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/syslog.html")
public class ActionSyslog extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSyslog.class; // For Translator

  private String serverName;
  private String port;
  private String message;
  private String facility;
  private String priority;
  private String datePattern;
  private boolean addTimestamp;
  private boolean addHostname;

  public ActionSyslog(String n) {
    super(n, "");
    port = String.valueOf(SyslogDefs.DEFAULT_PORT);
    serverName = null;
    message = null;
    facility = SyslogDefs.FACILITYS[0];
    priority = SyslogDefs.PRIORITYS[0];
    datePattern = SyslogDefs.DEFAULT_DATE_FORMAT;
    addTimestamp = true;
    addHostname = true;
  }

  public ActionSyslog() {
    this("");
  }

  public Object clone() {
    ActionSyslog je = (ActionSyslog) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(128);

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("port", port));
    retval.append("      ").append(XmlHandler.addTagValue("servername", serverName));
    retval.append("      ").append(XmlHandler.addTagValue("facility", facility));
    retval.append("      ").append(XmlHandler.addTagValue("priority", priority));
    retval.append("      ").append(XmlHandler.addTagValue("message", message));
    retval.append("      ").append(XmlHandler.addTagValue("datePattern", datePattern));
    retval.append("      ").append(XmlHandler.addTagValue("addTimestamp", addTimestamp));
    retval.append("      ").append(XmlHandler.addTagValue("addHostname", addHostname));

    return retval.toString();
  }

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      port = XmlHandler.getTagValue(entrynode, "port");
      serverName = XmlHandler.getTagValue(entrynode, "servername");
      facility = XmlHandler.getTagValue(entrynode, "facility");
      priority = XmlHandler.getTagValue(entrynode, "priority");
      message = XmlHandler.getTagValue(entrynode, "message");
      datePattern = XmlHandler.getTagValue(entrynode, "datePattern");
      addTimestamp = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "addTimestamp"));
      addHostname = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "addHostname"));

    } catch (HopXmlException xe) {
      throw new HopXmlException("Unable to load action of type 'Syslog' from XML node", xe);
    }
  }

  /** @return Returns the serverName. */
  public String getServerName() {
    return serverName;
  }

  /** @param serverName The serverName to set. */
  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  /** @return Returns the Facility. */
  public String getFacility() {
    return facility;
  }

  /** @param facility The facility to set. */
  public void setFacility(String facility) {
    this.facility = facility;
  }

  /** @param priority The priority to set. */
  public void setPriority(String priority) {
    this.priority = priority;
  }

  /** @return Returns the priority. */
  public String getPriority() {
    return priority;
  }

  /** @param message The message to set. */
  public void setMessage(String message) {
    this.message = message;
  }

  /** @return Returns the comString. */
  public String getMessage() {
    return message;
  }

  public void addTimestamp(boolean value) {
    this.addTimestamp = value;
  }

  /** @return Returns the addHostname. */
  public boolean isAddHostName() {
    return addHostname;
  }

  public void addHostName(boolean value) {
    this.addHostname = value;
  }

  /** @return Returns the addTimestamp. */
  public boolean isAddTimestamp() {
    return addTimestamp;
  }

  /** @param pattern The datePattern to set. */
  public void setDatePattern(String pattern) {
    this.datePattern = pattern;
  }

  /** @return Returns the datePattern. */
  public String getDatePattern() {
    return datePattern;
  }

  /** @return Returns the port. */
  public String getPort() {
    return port;
  }

  /** @param port The port to set. */
  public void setPort(String port) {
    this.port = port;
  }

  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setNrErrors(1);
    result.setResult(false);

    String servername = resolve(getServerName());

    if (Utils.isEmpty(servername)) {
      logError(BaseMessages.getString(PKG, "ActionSyslog.MissingServerName"));
    }

    String messageString = resolve(getMessage());

    if (Utils.isEmpty(messageString)) {
      logError(BaseMessages.getString(PKG, "ActionSyslog.MissingMessage"));
    }

    int nrPort = Const.toInt(resolve(getPort()), SyslogDefs.DEFAULT_PORT);

    SyslogIF syslog = null;
    try {
      String pattern = null;

      if (isAddTimestamp()) {
        // add timestamp to message
        pattern = resolve(getDatePattern());
        if (Utils.isEmpty(pattern)) {
          logError(BaseMessages.getString(PKG, "ActionSyslog.DatePatternEmpty"));
          throw new HopException(BaseMessages.getString(PKG, "ActionSyslog.DatePatternEmpty"));
        }
      }

      // Open syslog connection
      // Set a Specific Host, then Log to It
      syslog = Syslog.getInstance("udp");
      syslog.getConfig().setHost(servername);
      syslog.getConfig().setPort(nrPort);
      syslog.getConfig().setFacility(getFacility());
      syslog.getConfig().setSendLocalName(false);
      syslog.getConfig().setSendLocalTimestamp(false);
      SyslogDefs.sendMessage(
          syslog,
          SyslogDefs.getPriority(getPriority()),
          messageString,
          isAddTimestamp(),
          pattern,
          isAddHostName());

      // message was sent
      result.setNrErrors(0);
      result.setResult(true);
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionSyslog.ErrorSendingMessage", e.toString()));
    } finally {
      if (syslog != null) {
        syslog.shutdown();
      }
    }

    return result;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {}
}
