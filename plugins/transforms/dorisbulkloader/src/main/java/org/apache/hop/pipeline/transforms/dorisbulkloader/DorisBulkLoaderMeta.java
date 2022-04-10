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

package org.apache.hop.pipeline.transforms.dorisbulkloader;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "DorisBulkLoader",
    image = "dorisbulkloader.svg",
    name = "i18n::BaseTransform.TypeLongDesc.DorisBulkLoader",
    description = "i18n::BaseTransform.TypeTooltipDesc.DorisBulkLoader",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Bulk",
    keywords = "i18n::DorisBulkLoaderMeta.keyword",
    documentationUrl = "/pipeline/transforms/dorisBulkLoader.html")
public class DorisBulkLoaderMeta extends BaseTransformMeta<DorisBulkLoader, DorisBulkLoaderData> {
  private static final Class<?> PKG = DorisBulkLoaderMeta.class; // For Translator

  /** doris fe host */
  private String feHost;
  /** doris http port */
  private String feHttpPort;
  /** doris database name */
  private String databaseName;
  /** doris table name */
  private String tableName;
  /** doris login user */
  private String loginUser;
  /** doris login password */
  private String loginPassword;
  /** doris stream load data format */
  private String format;
  /** doris stream load line delimiter */
  private String lineDelimiter;
  /** doris stream load column delimiter */
  private String columnDelimiter;
  /** stream load http request headers */
  private String[] headerNames;
  private String[] headerValues;
  /** A buffer's capacity, in bytes. */
  private int bufferSize;
  /** BufferSize * BufferCount is the max capacity to buffer data before doing real stream load */
  private int bufferCount;

  /** doris stream load data fieldname */
  private String dataField;

  public DorisBulkLoaderMeta() {
    super(); // allocate BaseTransformMeta
  }

  public String getFeHost() {
    return feHost;
  }

  public void setFeHost(String feHost) {
    this.feHost = feHost;
  }

  public String getFeHttpPort() {
    return feHttpPort;
  }

  public void setFeHttpPort(String feHttpPort) {
    this.feHttpPort = feHttpPort;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getLoginUser() {
    return loginUser;
  }

  public void setLoginUser(String loginUser) {
    this.loginUser = loginUser;
  }

  public String getLoginPassword() {
    return loginPassword;
  }

  public void setLoginPassword(String loginPassword) {
    this.loginPassword = loginPassword;
  }

  public String getDataField() {
    return dataField;
  }

  public void setDataField(String dataField) {
    this.dataField = dataField;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public String getLineDelimiter() {
    return lineDelimiter;
  }

  public void setLineDelimiter(String lineDelimiter) {
    this.lineDelimiter = lineDelimiter;
  }

  public String getColumnDelimiter() {
    return columnDelimiter;
  }

  public void setColumnDelimiter(String columnDelimiter) {
    this.columnDelimiter = columnDelimiter;
  }

  public String[] getHeaderNames() {
    return headerNames;
  }

  public void setHeaderNames(String[] headerNames) {
    this.headerNames = headerNames;
  }

  public String[] getHeaderValues() {
    return headerValues;
  }

  public void setHeaderValues(String[] headerValues) {
    this.headerValues = headerValues;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public int getBufferCount() {
    return bufferCount;
  }

  public void setBufferCount(int bufferCount) {
    this.bufferCount = bufferCount;
  }

  /**
   * load xml into memory variables
   * @param transformNode the Node to get the info from
   * @param metadataProvider the metadata to optionally load external reference metadata from
   * @throws HopXmlException
   */
  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      feHost = XmlHandler.getTagValue(transformNode, "feHost");
      feHttpPort = XmlHandler.getTagValue(transformNode, "feHttpPort");
      databaseName = XmlHandler.getTagValue(transformNode, "databaseName");
      tableName = XmlHandler.getTagValue(transformNode, "tableName");
      dataField = XmlHandler.getTagValue(transformNode, "dataField");
      loginUser = XmlHandler.getTagValue(transformNode, "httpLogin");
      loginPassword =
              Encr.decryptPasswordOptionallyEncrypted(
                      XmlHandler.getTagValue(transformNode, "httpPassword"));
      format = XmlHandler.getTagValue(transformNode, "format");
      lineDelimiter = XmlHandler.getTagValue(transformNode, "lineDelimiter");
      lineDelimiter = XmlHandler.getTagValue(transformNode, "columnDelimiter");

      Node headersNode = XmlHandler.getSubNode(transformNode, "headers");
      int headerCount = XmlHandler.countNodes(headersNode, "header");
      allocate(headerCount);
      for (int i = 0; i < headerCount; i++) {
        Node anode = XmlHandler.getSubNodeByNr(headersNode, "header", i);
        headerNames[i] = XmlHandler.getTagValue(anode, "name");
        headerValues[i] = XmlHandler.getTagValue(anode, "value");
      }

      bufferSize = Integer.parseInt(XmlHandler.getTagValue(transformNode, "bufferSize"));
      bufferCount = Integer.parseInt(XmlHandler.getTagValue(transformNode, "bufferCount"));
    } catch (Exception e) {
      throw new HopXmlException(
              BaseMessages.getString(PKG, "DorisBulkLoaderMeta.Exception.UnableToReadTransformMeta"), e);
    }
  }

  /**
   * get xml from memory variables
   * @return
   */
  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();
    retval.append("    ").append(XmlHandler.addTagValue("feHost", feHost));
    retval.append("    ").append(XmlHandler.addTagValue("feHttpPort", feHttpPort));
    retval.append("    ").append(XmlHandler.addTagValue("databaseName", databaseName));
    retval.append("    ").append(XmlHandler.addTagValue("tableName", tableName));
    retval.append("    ").append(XmlHandler.addTagValue("dataField", dataField));
    retval.append("    ").append(XmlHandler.addTagValue("httpLogin", loginUser));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "httpPassword", Encr.encryptPasswordIfNotUsingVariables(loginPassword)));
    retval.append("    ").append(XmlHandler.addTagValue("format", format));
    retval.append("    ").append(XmlHandler.addTagValue("lineDelimiter", lineDelimiter));
    retval.append("    ").append(XmlHandler.addTagValue("columnDelimiter", columnDelimiter));

    retval.append("    <headers>").append(Const.CR);
    for (int i = 0, len = (headerNames != null ? headerNames.length : 0); i < len; i++) {
      retval.append("      <header>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", headerNames[i]));
      retval.append("        ").append(XmlHandler.addTagValue("value", headerValues[i]));
      retval.append("        </header>").append(Const.CR);
    }
    retval.append("      </headers>").append(Const.CR);

    retval.append("    ").append(XmlHandler.addTagValue("bufferSize", bufferSize));
    retval.append("    ").append(XmlHandler.addTagValue("bufferCount", bufferCount));

    return retval.toString();
  }

  @Override
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

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DorisBulkLoaderMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DorisBulkLoaderMeta.CheckResult.NoInpuReceived"),
              transformMeta);
    }
    remarks.add(cr);
  }

  /** whether the transform support error handling */
  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * allocate memory for doris heetp request header variable
   * @param headerCount
   */
  public void allocate(int headerCount) {
    headerNames = new String[headerCount];
    headerValues = new String[headerCount];
  }
}
