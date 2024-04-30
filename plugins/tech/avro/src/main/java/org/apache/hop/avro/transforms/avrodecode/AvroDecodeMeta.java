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

package org.apache.hop.avro.transforms.avrodecode;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "AvroDecode",
    name = "Avro Decode",
    description = "Decodes Avro data types into Hop fields",
    image = "avro_decode.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "/pipeline/transforms/avro-decode.html",
    keywords = "i18n::AvroDecodeMeta.keyword")
public class AvroDecodeMeta extends BaseTransformMeta<AvroDecode, AvroDecodeData> {
  private static final Class<?> PKG = AvroDecodeMeta.class; // For Translator

  @HopMetadataProperty(key = "source_field")
  private String sourceFieldName;

  @HopMetadataProperty(key = "remove_source_field")
  private boolean removingSourceField;

  @HopMetadataProperty(key = "ignore_missing")
  private boolean ignoringMissingPaths;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<TargetField> targetFields;

  public AvroDecodeMeta() {
    sourceFieldName = "avro";
    removingSourceField = true;
    ignoringMissingPaths = true;
    targetFields = new ArrayList<>();
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String transformName,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (TargetField targetField : targetFields) {
      try {
        IValueMeta valueMeta = targetField.createTargetValueMeta(variables);
        rowMeta.addValueMeta(valueMeta);
      } catch (HopException e) {
        throw new HopTransformException(
            "Error creating target field with name " + targetField.getTargetFieldName(), e);
      }
    }
  }

  /**
   * Gets sourceFieldName
   *
   * @return value of sourceFieldName
   */
  public String getSourceFieldName() {
    return sourceFieldName;
  }

  /**
   * @param sourceFieldName The sourceFieldName to set
   */
  public void setSourceFieldName(String sourceFieldName) {
    this.sourceFieldName = sourceFieldName;
  }

  public boolean isRemovingSourceField() {
    return removingSourceField;
  }

  public void setRemovingSourceField(boolean removingSourceField) {
    this.removingSourceField = removingSourceField;
  }

  /**
   * Gets ignoringMissingPaths
   *
   * @return value of ignoringMissingPaths
   */
  public boolean isIgnoringMissingPaths() {
    return ignoringMissingPaths;
  }

  /**
   * @param ignoringMissingPaths The ignoringMissingPaths to set
   */
  public void setIgnoringMissingPaths(boolean ignoringMissingPaths) {
    this.ignoringMissingPaths = ignoringMissingPaths;
  }

  /**
   * Gets targetFields
   *
   * @return value of targetFields
   */
  public List<TargetField> getTargetFields() {
    return targetFields;
  }

  /**
   * @param targetFields The targetFields to set
   */
  public void setTargetFields(List<TargetField> targetFields) {
    this.targetFields = targetFields;
  }
}
