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

package org.apache.hop.pipeline.transforms.ifnull;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "IfNull",
    image = "ifnull.svg",
    name = "i18n::IfNull.Name",
    description = "i18n::IfNull.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::IfNullMeta.keyword",
    documentationUrl = "/pipeline/transforms/ifnull.html")
public class IfNullMeta extends BaseTransformMeta<IfNull, IfNullData> {

  private static final Class<?> PKG = IfNullMeta.class;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "IfNull.Injection.FIELDS")
  private List<Field> fields;

  @HopMetadataProperty(
      groupKey = "valuetypes",
      key = "valuetype",
      injectionGroupKey = "VALUE_TYPES",
      injectionGroupDescription = "IfNull.Injection.VALUE_TYPES")
  private List<ValueType> valueTypes;

  @HopMetadataProperty(
      key = "selectFields",
      injectionKey = "SELECT_FIELDS",
      injectionKeyDescription = "IfNull.Injection.SELECT_FIELDS")
  private boolean selectFields;

  @HopMetadataProperty(
      key = "selectValuesType",
      injectionKey = "SELECT_VALUES_TYPE",
      injectionKeyDescription = "IfNull.Injection.SELECT_VALUES_TYPE")
  private boolean selectValuesType;

  @HopMetadataProperty(
      key = "replaceAllByValue",
      injectionKey = "REPLACE_ALL_BY_VALUE",
      injectionKeyDescription = "IfNull.Injection.REPLACE_ALL_BY_VALUE")
  private String replaceAllByValue;

  @HopMetadataProperty(
      key = "replaceAllMask",
      injectionKey = "REPLACE_ALL_MASK",
      injectionKeyDescription = "IfNull.Injection.REPLACE_ALL_MASK")
  private String replaceAllMask;

  /** The flag to set auto commit on or off on the connection */
  @HopMetadataProperty(
      key = "setEmptyStringAll",
      injectionKey = "SET_EMPTY_STRING_ALL",
      injectionKeyDescription = "IfNull.Injection.SET_EMPTY_STRING_ALL")
  private boolean setEmptyStringAll;

  public IfNullMeta() {
    super(); // allocate BaseTransformMeta
    this.valueTypes = new ArrayList<>();
    this.fields = new ArrayList<>();
  }

  public IfNullMeta(final IfNullMeta meta) {
    this();
    this.selectFields = meta.selectFields;
    this.selectValuesType = meta.selectValuesType;
    this.replaceAllByValue = meta.replaceAllByValue;
    this.replaceAllMask = meta.replaceAllMask;
    this.setEmptyStringAll = meta.setEmptyStringAll;

    for (Field field : meta.fields) {
      this.fields.add(new Field(field));
    }
    for (ValueType vt : meta.valueTypes) {
      this.valueTypes.add(new ValueType(vt));
    }
  }

  /**
   * @return Returns the setEmptyStringAll.
   */
  public boolean isSetEmptyStringAll() {
    return setEmptyStringAll;
  }

  /**
   * @param setEmptyStringAll The setEmptyStringAll to set.
   */
  public void setSetEmptyStringAll(boolean setEmptyStringAll) {
    this.setEmptyStringAll = setEmptyStringAll;
  }

  @Override
  public Object clone() {
    return new IfNullMeta(this);
  }

  public boolean isSelectFields() {
    return selectFields;
  }

  public void setSelectFields(boolean selectFields) {
    this.selectFields = selectFields;
  }

  public void setSelectValuesType(boolean selectValuesType) {
    this.selectValuesType = selectValuesType;
  }

  public boolean isSelectValuesType() {
    return selectValuesType;
  }

  public void setReplaceAllByValue(String replaceValue) {
    this.replaceAllByValue = replaceValue;
  }

  public String getReplaceAllByValue() {
    return replaceAllByValue;
  }

  public void setReplaceAllMask(String replaceAllMask) {
    this.replaceAllMask = replaceAllMask;
  }

  public String getReplaceAllMask() {
    return replaceAllMask;
  }

  public List<Field> getFields() {
    return fields;
  }

  public void setFields(List<Field> fields) {
    this.fields = fields;
  }

  public List<ValueType> getValueTypes() {
    return valueTypes;
  }

  public void setValueTypes(List<ValueType> valueTypes) {
    this.valueTypes = valueTypes;
  }

  @Override
  public void setDefault() {
    this.replaceAllByValue = null;
    this.replaceAllMask = null;
    this.selectFields = false;
    this.selectValuesType = false;
    this.setEmptyStringAll = false;
    this.valueTypes = new ArrayList<>();
    this.fields = new ArrayList<>();
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
              BaseMessages.getString(PKG, "IfNullMeta.CheckResult.NotReceivingFields"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "IfNullMeta.CheckResult.TransformRecevingData", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (Field field : this.fields) {
        int idx = prev.indexOfValue(field.getName());
        if (idx < 0) {
          errorMessage += "\t\t" + field.getName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "IfNullMeta.CheckResult.FieldsFound", errorMessage);

        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        if (!fields.isEmpty()) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "IfNullMeta.CheckResult.AllFieldsFound"),
                  transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_WARNING,
                  BaseMessages.getString(PKG, "IfNullMeta.CheckResult.NoFieldsEntered"),
                  transformMeta);
          remarks.add(cr);
        }
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "IfNullMeta.CheckResult.TransformRecevingData2"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "IfNullMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
