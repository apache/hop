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

package org.apache.hop.parquet.transforms.input;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "ParquetFileInput",
    image = "parquet_input.svg",
    name = "i18n::ParquetInput.Name",
    description = "i18n::ParquetInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "/pipeline/transforms/parquet-file-input.html",
    keywords = {"parquet", "read", "file", "column"})
public class ParquetInputMeta extends BaseTransformMeta
    implements ITransformMeta<ParquetInput, ParquetInputData> {

  @HopMetadataProperty(key = "filename_field")
  private String filenameField;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<ParquetField> fields;

  public ParquetInputMeta() {
    fields = new ArrayList<>();
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
    // Add the fields to the input
    //
    for (ParquetField field : fields) {
      try {
        IValueMeta valueMeta = field.createValueMeta();
        valueMeta.setOrigin(name);
        inputRowMeta.addValueMeta(valueMeta);
      } catch (HopException e) {
        throw new HopTransformException(
            "Unable to create value metadata of type '" + field.getTargetType() + "'", e);
      }
    }
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      ParquetInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ParquetInput(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public ParquetInputData getTransformData() {
    return new ParquetInputData();
  }

  /**
   * Gets filenameField
   *
   * @return value of filenameField
   */
  public String getFilenameField() {
    return filenameField;
  }

  /** @param filenameField The filenameField to set */
  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<ParquetField> getFields() {
    return fields;
  }

  /** @param fields The fields to set */
  public void setFields(List<ParquetField> fields) {
    this.fields = fields;
  }
}
