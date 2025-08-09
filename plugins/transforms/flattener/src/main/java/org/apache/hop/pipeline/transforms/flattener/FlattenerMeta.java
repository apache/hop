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

package org.apache.hop.pipeline.transforms.flattener;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/** The flattener transform meta-data */
@Transform(
    id = "Flattener,Flatterner",
    image = "flattener.svg",
    name = "i18n::RowFlattener.Name",
    description = "i18n::RowFlattener.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::FlattenerMeta.keyword",
    documentationUrl = "/pipeline/transforms/rowflattener.html")
public class FlattenerMeta extends BaseTransformMeta<Flattener, FlattenerData> {
  private static final Class<?> PKG = FlattenerMeta.class;

  /** The field to flatten */
  @HopMetadataProperty(key = "field_name")
  private String fieldName;

  /** Fields to flatten, same data type as input */
  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<FField> targetFields;

  public FlattenerMeta() {
    super();
    this.targetFields = new ArrayList<>();
  }

  public FlattenerMeta(FlattenerMeta m) {
    this();
    this.fieldName = m.fieldName;
    m.targetFields.forEach(f -> this.targetFields.add(new FField(f)));
  }

  @Override
  public FlattenerMeta clone() {
    return new FlattenerMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    // Remove the key value (there will be different entries for each output row)
    //
    if (!Utils.isEmpty(fieldName)) {
      int idx = row.indexOfValue(fieldName);
      if (idx < 0) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "FlattenerMeta.Exception.UnableToLocateFieldInInputFields", fieldName));
      }

      IValueMeta v = row.getValueMeta(idx);
      row.removeValueMeta(idx);

      for (FField targetField : targetFields) {
        IValueMeta value = v.clone();
        value.setName(targetField.getName());
        value.setOrigin(name);

        row.addValueMeta(value);
      }
    } else {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "FlattenerMeta.Exception.FlattenFieldRequired"));
    }
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

    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FlattenerMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "FlattenerMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  /**
   * Gets fieldName
   *
   * @return value of fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * Sets fieldName
   *
   * @param fieldName value of fieldName
   */
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  /**
   * Gets targetFields
   *
   * @return value of targetFields
   */
  public List<FField> getTargetFields() {
    return targetFields;
  }

  /**
   * Sets targetFields
   *
   * @param targetFields value of targetFields
   */
  public void setTargetFields(List<FField> targetFields) {
    this.targetFields = targetFields;
  }

  public static final class FField {
    @HopMetadataProperty(key = "name")
    private String name;

    public FField() {}

    public FField(FField f) {
      this.name = f.name;
    }

    public FField(String name) {
      this.name = name;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }
  }
}
