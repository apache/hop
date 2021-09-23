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
 *
 */

package org.apache.hop.reflection.pipeline.transform;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "PipelineLogging",
    name = "i18n::PipelineLogging.Transform.Name",
    description = "i18n::PipelineLogging.Transform.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    image = "pipeline-log.svg",
    documentationUrl = "/logging/logging-reflection.html",
    keywords = "audit,log,metrics")
public class PipelineLoggingMeta extends BaseTransformMeta<PipelineLogging, PipelineLoggingData> {

  private boolean loggingTransforms;

  public PipelineLoggingMeta() {
    super();
  }

  @Override
  public void setDefault() {
    loggingTransforms = true;
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

    inputRowMeta.clear();

    // Logging date
    inputRowMeta.addValueMeta(new ValueMetaDate("loggingDate"));

    // Logging date
    inputRowMeta.addValueMeta(new ValueMetaString("loggingPhase"));

    // Name of the pipeline
    inputRowMeta.addValueMeta(new ValueMetaString("pipelineName", 255, -1));

    // Filename of the pipeline
    inputRowMeta.addValueMeta(new ValueMetaString("pipelineFilename", 255, -1));

    // Start date of the pipeline
    inputRowMeta.addValueMeta(new ValueMetaDate("pipelineStart"));

    // End date of the pipeline
    inputRowMeta.addValueMeta(new ValueMetaDate("pipelineEnd"));

    // Pipeline log channel ID
    inputRowMeta.addValueMeta(new ValueMetaString("pipelineLogChannelId", 32, -1));

    // Parent log channel ID
    inputRowMeta.addValueMeta(new ValueMetaString("parentLogChannelId", 32, -1));

    // Logging text of the pipeline
    inputRowMeta.addValueMeta(new ValueMetaString("pipelineLogging", 1000000, -1));

    // Number of errors
    inputRowMeta.addValueMeta(new ValueMetaInteger("pipelineErrorCount", 3, 0));

    // Pipeline status description
    inputRowMeta.addValueMeta(new ValueMetaString("pipelineStatusDescription", 32, -1));

    if (loggingTransforms) {
      // Name of the transform
      inputRowMeta.addValueMeta(new ValueMetaString("transformName"));

      // Copy number of the transform
      inputRowMeta.addValueMeta(new ValueMetaInteger("transformCopyNr"));

      // Copy number of the transform
      inputRowMeta.addValueMeta(new ValueMetaString("transformStatusDescription", 100, -1));

      // Transform log channel ID
      inputRowMeta.addValueMeta(new ValueMetaString("transformLogChannelId", 32, -1));

      // Transform logging text
      inputRowMeta.addValueMeta(new ValueMetaString("transformLoggingText", 1000000, -1));

      // Number of lines read
      inputRowMeta.addValueMeta(new ValueMetaInteger("transformLinesRead", 12, 0));

      // Number of lines written
      inputRowMeta.addValueMeta(new ValueMetaInteger("transformLinesWritten", 12, 0));

      // Number of lines input
      inputRowMeta.addValueMeta(new ValueMetaInteger("transformLinesInput", 12, 0));

      // Number of lines output
      inputRowMeta.addValueMeta(new ValueMetaInteger("transformLinesOutput", 12, 0));

      // Number of lines updated
      inputRowMeta.addValueMeta(new ValueMetaInteger("transformLinesUpdated", 12, 0));

      // Number of lines rejected
      inputRowMeta.addValueMeta(new ValueMetaInteger("transformLinesRejected", 12, 0));

      // Number of errors
      inputRowMeta.addValueMeta(new ValueMetaInteger("transformErrors", 3, 0));

      // Execution start
      inputRowMeta.addValueMeta(new ValueMetaDate("transformStart"));

      // Execution end
      inputRowMeta.addValueMeta(new ValueMetaDate("transformEnd"));

      // Execution duration in ms
      inputRowMeta.addValueMeta(new ValueMetaInteger("transformDuration", 12, 0));
    }
  }

  @Override
  public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer();
    xml.append(XmlHandler.addTagValue("log_transforms", loggingTransforms));
    return xml.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {

    loggingTransforms =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "log_transforms"));
  }

  /**
   * Gets loggingTransforms
   *
   * @return value of loggingTransforms
   */
  public boolean isLoggingTransforms() {
    return loggingTransforms;
  }

  /** @param loggingTransforms The loggingTransforms to set */
  public void setLoggingTransforms(boolean loggingTransforms) {
    this.loggingTransforms = loggingTransforms;
  }
}
