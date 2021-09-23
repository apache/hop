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

package org.apache.hop.pipeline.transforms.abort;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;
import java.util.List;

/** Meta data for the abort transform. */
@Transform(
    id = "Abort",
    name = "i18n::Abort.Name",
    description = "i18n::Abort.Description",
    image = "abort.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    documentationUrl = "/pipeline/transforms/abort.html")
public class AbortMeta extends BaseTransformMeta<Abort, AbortData> {

  private static final Class<?> PKG = AbortMeta.class; // For Translator

  public enum AbortOption {
    ABORT,
    ABORT_WITH_ERROR,
    SAFE_STOP
  }

  /** Threshold to abort. */
  @HopMetadataProperty(
      key = "row_threshold",
      injectionKeyDescription = "AbortDialog.Options.RowThreshold.Label")
  private String rowThreshold;

  /** Message to put in log when aborting. */
  @HopMetadataProperty(injectionKeyDescription = "AbortDialog.Logging.AbortMessage.Tooltip")
  private String message;

  /** Always log rows. */
  @HopMetadataProperty(
      key = "always_log_rows",
      injectionKeyDescription = "AbortDialog.Logging.AlwaysLogRows.Label")
  private boolean alwaysLogRows;

  @HopMetadataProperty(
      key = "abort_option",
      injectionKeyDescription = "AbortMeta.Injection.AbortOption")
  private AbortOption abortOption;

  public AbortMeta() {
    abortOption = AbortOption.ABORT;
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Default: no values are added to the row in the transform
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transforminfo,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    // See if we have input streams leading to this transform!
    if (input.length == 0) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "AbortMeta.CheckResult.NoInputReceivedError"),
              transforminfo);
      remarks.add(cr);
    }
  }

  @Override
  public void setDefault() {
    rowThreshold = "0";
    message = "";
    alwaysLogRows = true;
    abortOption = AbortOption.ABORT_WITH_ERROR;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    super.loadXml(transformNode, metadataProvider);

    // Backward compatible code
    //
    if (abortOption == null) {
      String awe = XmlHandler.getTagValue(transformNode, "abort_with_error");
      if (awe == null) {
        awe = "Y"; // existing pipelines will have to maintain backward compatibility with yes
      }
      abortOption = "Y".equalsIgnoreCase(awe) ? AbortOption.ABORT_WITH_ERROR : AbortOption.ABORT;
    }
  }

  public boolean isAbortWithError() {
    return abortOption == AbortOption.ABORT_WITH_ERROR;
  }

  public boolean isAbort() {
    return abortOption == AbortOption.ABORT;
  }

  public boolean isSafeStop() {
    return abortOption == AbortOption.SAFE_STOP;
  }

  /**
   * Gets rowThreshold
   *
   * @return value of rowThreshold
   */
  public String getRowThreshold() {
    return rowThreshold;
  }

  /** @param rowThreshold The rowThreshold to set */
  public void setRowThreshold(String rowThreshold) {
    this.rowThreshold = rowThreshold;
  }

  /**
   * Gets message
   *
   * @return value of message
   */
  public String getMessage() {
    return message;
  }

  /** @param message The message to set */
  public void setMessage(String message) {
    this.message = message;
  }

  /**
   * Gets alwaysLogRows
   *
   * @return value of alwaysLogRows
   */
  public boolean isAlwaysLogRows() {
    return alwaysLogRows;
  }

  /** @param alwaysLogRows The alwaysLogRows to set */
  public void setAlwaysLogRows(boolean alwaysLogRows) {
    this.alwaysLogRows = alwaysLogRows;
  }

  /**
   * Gets abortOption
   *
   * @return value of abortOption
   */
  public AbortOption getAbortOption() {
    return abortOption;
  }

  /** @param abortOption The abortOption to set */
  public void setAbortOption(AbortOption abortOption) {
    this.abortOption = abortOption;
  }
}
