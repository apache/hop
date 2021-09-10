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

package org.apache.hop.beam.transforms.window;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.TimestampFn;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.Dummy;
import org.apache.hop.pipeline.transforms.dummy.DummyData;
import java.util.List;
import java.util.Map;

@Transform(
    id = "BeamTimestamp",
    name = "Beam Timestamp",
    description = "Add timestamps to a bounded data source",
    image = "beam-timestamp.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    documentationUrl =
        "https://hop.apache.org/manual/latest/pipeline/transforms/beamtimestamp.html")
public class BeamTimestampMeta extends BaseTransformMeta<Dummy, DummyData>
    implements IBeamPipelineTransformHandler {

  @HopMetadataProperty(key = "field_name")
  private String fieldName;

  @HopMetadataProperty(key = "read_timestamp")
  private boolean readingTimestamp;

  public BeamTimestampMeta() {
    fieldName = "";
  }

  @Override
  public String getDialogClassName() {
    return BeamTimestampDialog.class.getName();
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

    if (readingTimestamp) {
      ValueMetaDate valueMeta = new ValueMetaDate(fieldName);
      valueMeta.setOrigin(name);
      inputRowMeta.addValueMeta(valueMeta);
    }
  }

  @Override
  public boolean isInput() {
    return false;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      IBeamPipelineEngineRunConfiguration runConfiguration,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      List<String> transformPluginClasses,
      List<String> xpPluginClasses,
      TransformMeta transformMeta,
      Map<String, PCollection<HopRow>> transformCollectionMap,
      org.apache.beam.sdk.Pipeline pipeline,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      PCollection<HopRow> input)
      throws HopException {
    if (!readingTimestamp && StringUtils.isNotEmpty(fieldName)) {
      if (rowMeta.searchValueMeta(fieldName) == null) {
        throw new HopException(
            "Please specify a valid field name '" + transformMeta.getName() + "'");
      }
    }

    PCollection<HopRow> transformPCollection =
        input.apply(
            ParDo.of(
                new TimestampFn(
                    transformMeta.getName(),
                    JsonRowMeta.toJson(rowMeta),
                    variables.resolve(fieldName),
                    readingTimestamp,
                    transformPluginClasses,
                    xpPluginClasses)));

    // Save this in the map
    //
    transformCollectionMap.put(transformMeta.getName(), transformPCollection);
    log.logBasic(
        "Handled transform (TIMESTAMP) : "
            + transformMeta.getName()
            + ", gets data from "
            + previousTransforms.size()
            + " previous transform(s)");
  }

  /**
   * Gets fieldName
   *
   * @return value of fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

  /** @param fieldName The fieldName to set */
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  /**
   * Gets readingTimestamp
   *
   * @return value of readingTimestamp
   */
  public boolean isReadingTimestamp() {
    return readingTimestamp;
  }

  /** @param readingTimestamp The readingTimestamp to set */
  public void setReadingTimestamp(boolean readingTimestamp) {
    this.readingTimestamp = readingTimestamp;
  }
}
