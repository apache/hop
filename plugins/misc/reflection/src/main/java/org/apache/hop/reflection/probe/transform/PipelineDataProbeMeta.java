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

package org.apache.hop.reflection.probe.transform;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "PipelineDataProbe",
    name = "i18n::PipelineDataProbe.Transform.Name",
    description = "i18n::PipelineDataProbe.Transform.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "/logging/logging-reflection.html",
    image = "probe.svg",
    keywords = "i18n::PipelineDataProbeMeta.keyword")
public class PipelineDataProbeMeta extends BaseTransformMeta<PipelineDataProbe, PipelineDataProbeData> {

  @HopMetadataProperty(key = "log_transforms")
  private boolean loggingTransforms;

  public PipelineDataProbeMeta() {
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

    // Source pipeline name
    inputRowMeta.addValueMeta(new ValueMetaString("sourcePipelineName", 255, -1));

    // Source log channel ID
    inputRowMeta.addValueMeta(new ValueMetaString("sourceTransformLogChannelId", 36, -1));

    // Source transform
    inputRowMeta.addValueMeta(new ValueMetaString("sourceTransformName", 255, -1));

    // source transform copy
    inputRowMeta.addValueMeta(new ValueMetaInteger("sourceTransformCopy", 7, 0));

    // Row number
    inputRowMeta.addValueMeta(new ValueMetaInteger("rowNr", 15, 0));

    // field name
    inputRowMeta.addValueMeta(new ValueMetaString("fieldName", 255, -1));

    // field type
    inputRowMeta.addValueMeta(new ValueMetaString("fieldType", 255, -1));

    // field format
    inputRowMeta.addValueMeta(new ValueMetaString("fieldFormat", 255, -1));

    // field length
    inputRowMeta.addValueMeta(new ValueMetaInteger("fieldLength", 7, -1));

    // field precision
    inputRowMeta.addValueMeta(new ValueMetaInteger("fieldPrecision", 7, -1));

    // value
    inputRowMeta.addValueMeta(new ValueMetaString("value", 1000000, -1));
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
