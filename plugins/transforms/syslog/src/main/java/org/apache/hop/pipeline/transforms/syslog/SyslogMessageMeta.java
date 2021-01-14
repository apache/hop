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

package org.apache.hop.pipeline.transforms.syslog;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.actions.syslog.SyslogDefs;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "SyslogMessage",
    image = "syslogmessage.svg",
    name = "i18n::BaseTransform.TypeLongDesc.SyslogMessage",
    description = "i18n::BaseTransform.TypeTooltipDesc.SyslogMessage",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/syslog.html")
public class SyslogMessageMeta extends BaseTransformMeta
    implements ITransformMeta<SyslogMessage, SyslogMessageData> {
  private static final Class<?> PKG = SyslogMessageMeta.class; // For Translator

  /** dynamic message fieldname */
  private String messagefieldname;

  private String serverName;
  private String port;
  private String facility;
  private String priority;
  private String datePattern;
  private boolean addTimestamp;
  private boolean addHostName;

  public SyslogMessageMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public Object clone() {
    SyslogMessageMeta retval = (SyslogMessageMeta) super.clone();

    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      SyslogMessageData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new SyslogMessage(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  public void setDefault() {
    messagefieldname = null;
    port = String.valueOf(SyslogDefs.DEFAULT_PORT);
    serverName = null;
    facility = SyslogDefs.FACILITYS[0];
    priority = SyslogDefs.PRIORITYS[0];
    datePattern = SyslogDefs.DEFAULT_DATE_FORMAT;
    addTimestamp = true;
    addHostName = true;
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

  /** @param messagefieldname The messagefieldname to set. */
  public void setMessageFieldName(String messagefieldname) {
    this.messagefieldname = messagefieldname;
  }

  /** @return Returns the messagefieldname. */
  public String getMessageFieldName() {
    return messagefieldname;
  }

  /** @return Returns the port. */
  public String getPort() {
    return port;
  }

  /** @param port The port to set. */
  public void setPort(String port) {
    this.port = port;
  }

  /**
   * @param value
   * @deprecated use {@link #setAddTimestamp(boolean)} instead
   */
  @Deprecated
  public void addTimestamp(boolean value) {
    setAddTimestamp(value);
  }

  public void setAddTimestamp(boolean value) {
    this.addTimestamp = value;
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

  /**
   * @param value
   * @deprecated use {@link #setAddHostName(boolean)} instead
   */
  @Deprecated
  public void addHostName(boolean value) {
    setAddHostName(value);
  }

  public void setAddHostName(boolean value) {
    this.addHostName = value;
  }

  /** @return Returns the addHostName. */
  public boolean isAddHostName() {
    return addHostName;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    " + XmlHandler.addTagValue("messagefieldname", messagefieldname));
    retval.append("    " + XmlHandler.addTagValue("port", port));
    retval.append("    " + XmlHandler.addTagValue("servername", serverName));
    retval.append("    " + XmlHandler.addTagValue("facility", facility));
    retval.append("    " + XmlHandler.addTagValue("priority", priority));
    retval.append("    " + XmlHandler.addTagValue("addTimestamp", addTimestamp));
    retval.append("    " + XmlHandler.addTagValue("datePattern", datePattern));
    retval.append("    " + XmlHandler.addTagValue("addHostName", addHostName));

    return retval.toString();
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      messagefieldname = XmlHandler.getTagValue(transformNode, "messagefieldname");
      port = XmlHandler.getTagValue(transformNode, "port");
      serverName = XmlHandler.getTagValue(transformNode, "servername");
      facility = XmlHandler.getTagValue(transformNode, "facility");
      priority = XmlHandler.getTagValue(transformNode, "priority");
      datePattern = XmlHandler.getTagValue(transformNode, "datePattern");
      addTimestamp = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "addTimestamp"));
      addHostName = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "addHostName"));

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "SyslogMessageMeta.Exception.UnableToReadTransformMeta"), e);
    }
  }

  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;
    String errorMessage = "";

    // source filename
    if (Utils.isEmpty(messagefieldname)) {
      errorMessage =
          BaseMessages.getString(PKG, "SyslogMessageMeta.CheckResult.MessageFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "SyslogMessageMeta.CheckResult.MessageFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SyslogMessageMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SyslogMessageMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public SyslogMessageData getTransformData() {
    return new SyslogMessageData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
