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
 *
 */

package org.apache.hop.pipeline.transforms.dorisbulkloader;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "DorisBulkLoader",
    image = "dorisbulkloader.svg",
    name = "i18n::BaseTransform.TypeLongDesc.DorisBulkLoader",
    description = "i18n::BaseTransform.TypeTooltipDesc.DorisBulkLoader",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Bulk",
    keywords = "i18n::DorisBulkLoaderMeta.keyword",
    documentationUrl = "/pipeline/transforms/dorisbulkloader.html")
public class DorisBulkLoaderMeta extends BaseTransformMeta<DorisBulkLoader, DorisBulkLoaderData> {
  private static final Class<?> PKG = DorisBulkLoaderMeta.class; // For Translator

  /** doris fe host */
  @HopMetadataProperty private String feHost;

  /** doris http port */
  @HopMetadataProperty private String feHttpPort;

  /** doris database name */
  @HopMetadataProperty private String databaseName;

  /** doris table name */
  @HopMetadataProperty private String tableName;

  /** doris login user */
  @HopMetadataProperty private String loginUser;

  /** doris login password */
  @HopMetadataProperty(password = true)
  private String loginPassword;

  /** doris stream load data format */
  @HopMetadataProperty private String format;

  /** doris stream load line delimiter */
  @HopMetadataProperty private String lineDelimiter;

  /** doris stream load column delimiter */
  @HopMetadataProperty private String columnDelimiter;

  /** stream load http request headers */
  @HopMetadataProperty(groupKey = "headers", key = "header")
  List<DorisHeader> headers;

  /** A buffer's capacity, in bytes. */
  @HopMetadataProperty private int bufferSize;

  /** BufferSize * BufferCount is the max capacity to buffer data before doing real stream load */
  @HopMetadataProperty private int bufferCount;

  /** doris stream load data fieldname */
  private String dataField;

  public DorisBulkLoaderMeta() {
    super(); // allocate BaseTransformMeta
    headers = new ArrayList<>();
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
              BaseMessages.getString(
                  PKG, "DorisBulkLoaderMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
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

  public List<DorisHeader> getHeaders() {
    return headers;
  }

  public void setHeaders(List<DorisHeader> headers) {
    this.headers = headers;
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

  public String getDataField() {
    return dataField;
  }

  public void setDataField(String dataField) {
    this.dataField = dataField;
  }
}
