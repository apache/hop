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

package org.apache.hop.avro.transforms.avroinput;

import org.apache.hop.avro.type.ValueMetaAvroRecord;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "AvroFileInput",
    name = "Avro File Input",
    description = "Reads file serialized in the Apache Avro file format",
    image = "avro_input.svg",
    categoryDescription = "Input",
    documentationUrl =
        "https://hop.apache.org/manual/latest/pipeline/transforms/avro-file-input.html",
    keywords = {"Avro", "Read Avro"})
@InjectionSupported(localizationPrefix = "AvroInputMeta.Injection.")
public class AvroFileInputMeta extends BaseTransformMeta
    implements ITransformMeta<AvroFileInput, AvroFileInputData> {

  @HopMetadataProperty(key = "output_field")
  private String outputFieldName;

  @HopMetadataProperty(key = "data_filename_field")
  private String dataFilenameField;

  @HopMetadataProperty(key = "rows_limit")
  private String rowsLimit;

  public AvroFileInputMeta() {
    outputFieldName = "avro";
  }

  public AvroFileInputMeta(AvroFileInputMeta m) {
    this.outputFieldName = m.outputFieldName;
    this.dataFilenameField = m.dataFilenameField;
    this.rowsLimit = m.rowsLimit;
  }

  public AvroFileInputMeta clone() {
    return new AvroFileInputMeta(this);
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
    // Output is one extra field with an Avro data type
    // We don't have a schema just yet.
    //
    IValueMeta outputValue = new ValueMetaAvroRecord(outputFieldName);
    inputRowMeta.addValueMeta(outputValue);
  }

  @Override
  public AvroFileInput createTransform(
      TransformMeta transformMeta,
      AvroFileInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new AvroFileInput(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public AvroFileInputData getTransformData() {
    return new AvroFileInputData();
  }

  /**
   * Gets outputFieldName
   *
   * @return value of outputFieldName
   */
  public String getOutputFieldName() {
    return outputFieldName;
  }

  /** @param outputFieldName The outputFieldName to set */
  public void setOutputFieldName(String outputFieldName) {
    this.outputFieldName = outputFieldName;
  }

  /**
   * Gets dataFilenameField
   *
   * @return value of dataFilenameField
   */
  public String getDataFilenameField() {
    return dataFilenameField;
  }

  /** @param dataFilenameField The dataFilenameField to set */
  public void setDataFilenameField(String dataFilenameField) {
    this.dataFilenameField = dataFilenameField;
  }

  /**
   * Gets rowsLimit
   *
   * @return value of rowsLimit
   */
  public String getRowsLimit() {
    return rowsLimit;
  }

  /** @param rowsLimit The rowsLimit to set */
  public void setRowsLimit(String rowsLimit) {
    this.rowsLimit = rowsLimit;
  }
}
