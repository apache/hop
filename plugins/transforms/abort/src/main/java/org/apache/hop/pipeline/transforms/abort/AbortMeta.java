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
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

import static org.apache.hop.core.util.StringUtil.isEmpty;

/** Meta data for the abort transform. */
@Transform(
    id = "Abort",
    name = "i18n::Abort.Name",
    description = "i18n::Abort.Description",
    image = "abort.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/abort.html")
public class AbortMeta extends BaseTransformMeta implements ITransformMeta<Abort, AbortData> {

  private static final Class<?> PKG = AbortMeta.class; // For Translator

  public enum AbortOption {
    ABORT,
    ABORT_WITH_ERROR,
    SAFE_STOP
  }

  /** Threshold to abort. */
  private String rowThreshold;

  /** Message to put in log when aborting. */
  private String message;

  /** Always log rows. */
  private boolean alwaysLogRows;

  private AbortOption abortOption;

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
  public Abort createTransform(
      TransformMeta transformMeta,
      AbortData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new Abort(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public AbortData getTransformData() {
    return new AbortData();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  @Override
  public void setDefault() {
    rowThreshold = "0";
    message = "";
    alwaysLogRows = true;
    abortOption = AbortOption.ABORT_WITH_ERROR;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(200);

    retval.append("      ").append(XmlHandler.addTagValue("row_threshold", rowThreshold));
    retval.append("      ").append(XmlHandler.addTagValue("message", message));
    retval.append("      ").append(XmlHandler.addTagValue("always_log_rows", alwaysLogRows));
    retval.append("      ").append(XmlHandler.addTagValue("abort_option", abortOption.toString()));

    return retval.toString();
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      rowThreshold = XmlHandler.getTagValue(transformNode, "row_threshold");
      message = XmlHandler.getTagValue(transformNode, "message");
      alwaysLogRows =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "always_log_rows"));
      String abortOptionString = XmlHandler.getTagValue(transformNode, "abort_option");
      if (!isEmpty(abortOptionString)) {
        abortOption = AbortOption.valueOf(abortOptionString);
      } else {
        // Backwards compatibility
        String awe = XmlHandler.getTagValue(transformNode, "abort_with_error");
        if (awe == null) {
          awe = "Y"; // existing pipelines will have to maintain backward compatibility with yes
        }
        abortOption = "Y".equalsIgnoreCase(awe) ? AbortOption.ABORT_WITH_ERROR : AbortOption.ABORT;
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "AbortMeta.Exception.UnableToLoadTransformMetaFromXML"), e);
    }
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getRowThreshold() {
    return rowThreshold;
  }

  public void setRowThreshold(String rowThreshold) {
    this.rowThreshold = rowThreshold;
  }

  public boolean isAlwaysLogRows() {
    return alwaysLogRows;
  }

  public void setAlwaysLogRows(boolean alwaysLogRows) {
    this.alwaysLogRows = alwaysLogRows;
  }

  public AbortOption getAbortOption() {
    return abortOption;
  }

  public void setAbortOption(AbortOption abortOption) {
    this.abortOption = abortOption;
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
}
