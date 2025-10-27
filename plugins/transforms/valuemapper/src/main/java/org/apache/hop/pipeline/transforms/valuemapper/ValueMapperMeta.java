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

package org.apache.hop.pipeline.transforms.valuemapper;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Maps String values of a certain field to new values */
@Transform(
    id = "ValueMapper",
    image = "valuemapper.svg",
    name = "i18n::ValueMapper.Name",
    description = "i18n::ValueMapper.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::ValueMapperMeta.keyword",
    documentationUrl = "/pipeline/transforms/valuemapper.html")
public class ValueMapperMeta extends BaseTransformMeta<ValueMapper, ValueMapperData> {
  private static final Class<?> PKG = ValueMapperMeta.class;

  @HopMetadataProperty(
      key = "field_to_use",
      injectionKey = "FIELDNAME",
      injectionKeyDescription = "ValueMapper.Injection.FIELDNAME")
  private String fieldToUse;

  @HopMetadataProperty(
      key = "target_field",
      injectionKey = "TARGET_FIELDNAME",
      injectionKeyDescription = "ValueMapper.Injection.TARGET_FIELDNAME")
  private String targetField;

  @HopMetadataProperty(
      key = "non_match_default",
      injectionKey = "NON_MATCH_DEFAULT",
      injectionKeyDescription = "ValueMapper.Injection.NON_MATCH_DEFAULT")
  private String nonMatchDefault;

  @HopMetadataProperty(
      key = "target_type",
      injectionKey = "TARGET_TYPE",
      injectionKeyDescription = "ValueMapper.Injection.TARGET_TYPE")
  private String targetType;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "VALUES",
      injectionGroupDescription = "ValueMapper.Injection.VALUES")
  private List<Values> values;

  public ValueMapperMeta() {
    super(); // allocate BaseTransformMeta
    this.values = new ArrayList<>();
  }

  public ValueMapperMeta(ValueMapperMeta meta) {
    this();
    for (Values v : meta.values) {
      values.add(new Values(v));
    }

    this.fieldToUse = meta.fieldToUse;
    this.targetField = meta.targetField;
    this.nonMatchDefault = meta.nonMatchDefault;
    if (meta.targetType != null && meta.targetType.isEmpty()) {
      this.targetType = meta.targetType;
    } else {
      this.targetType = "String";
    }
  }

  /**
   * @return Returns the fieldValue.
   */
  public List<Values> getValues() {
    return values;
  }

  /**
   * @param values The fieldValue to set.
   */
  public void setValues(List<Values> values) {
    this.values = values;
  }

  @Override
  public Object clone() {
    return new ValueMapperMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    IValueMeta extra = null;

    // Determine target value meta type (default to String for backward compatibility)
    String targetTypeName = Utils.isEmpty(getTargetType()) ? "String" : getTargetType();
    int targetTypeId = ValueMetaFactory.getIdForValueMeta(targetTypeName);
    // fallback
    targetTypeId = targetTypeId == IValueMeta.TYPE_NONE ? IValueMeta.TYPE_STRING : targetTypeId;

    if (!Utils.isEmpty(getTargetField())) {
      // ADD a new field with the chosen type
      try {
        extra = ValueMetaFactory.createValueMeta(getTargetField(), targetTypeId);
      } catch (HopPluginException e) {
        // fallback if factory fails for some reason
        extra = new ValueMetaString(getTargetField());
      }

      if (extra.getType() == IValueMeta.TYPE_STRING) {
        // Lengths etc?
        // Take the max length of all the strings...
        //
        int maxlen = -1;
        for (Values v : this.values) {
          if (v.getTarget() != null && v.getTarget().length() > maxlen) {
            maxlen = v.getTarget().length();
          }
        }

        // include default value in max length calculation
        //
        if (nonMatchDefault != null && nonMatchDefault.length() > maxlen) {
          maxlen = nonMatchDefault.length();
        }
        extra.setLength(maxlen);
        extra.setOrigin(name);
      }
      r.addValueMeta(extra);
    } else {
      if (!Utils.isEmpty(getFieldToUse())) {
        extra = r.searchValueMeta(getFieldToUse());
      }
    }

    if (extra != null) {
      // The output of a changed field or new field is always a normal storage type...
      //
      extra.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
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
    if (prev == null || prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(
                  PKG, "ValueMapperMeta.CheckResult.NotReceivingFieldsFromPreviousTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "ValueMapperMeta.CheckResult.ReceivingFieldsFromPreviousTransforms",
                  "" + prev.size()),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ValueMapperMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "ValueMapperMeta.CheckResult.NotReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  /**
   * @return Returns the fieldToUse.
   */
  public String getFieldToUse() {
    return fieldToUse;
  }

  /**
   * @param fieldToUse The fieldToUse to set.
   */
  public void setFieldToUse(String fieldToUse) {
    this.fieldToUse = fieldToUse;
  }

  /**
   * @return Returns the targetField.
   */
  public String getTargetField() {
    return targetField;
  }

  /**
   * @param targetField The targetField to set.
   */
  public void setTargetField(String targetField) {
    this.targetField = targetField;
  }

  /**
   * @return the non match default. This is the string that will be used to fill in the data when no
   *     match is found.
   */
  public String getNonMatchDefault() {
    return nonMatchDefault;
  }

  /**
   * @param nonMatchDefault the non match default. This is the string that will be used to fill in
   *     the data when no match is found.
   */
  public void setNonMatchDefault(String nonMatchDefault) {
    this.nonMatchDefault = nonMatchDefault;
  }

  /**
   * @return Returns the targetType.
   */
  public String getTargetType() {
    return targetType;
  }

  /**
   * @param targetType The targetType to set.
   */
  public void setTargetType(String targetType) {
    this.targetType = targetType;
  }
}
