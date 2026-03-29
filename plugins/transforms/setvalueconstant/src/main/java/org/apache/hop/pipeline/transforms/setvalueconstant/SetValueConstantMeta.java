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

package org.apache.hop.pipeline.transforms.setvalueconstant;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "SetValueConstant",
    image = "setvalueconstant.svg",
    name = "i18n::SetValueConstant.Name",
    description = "i18n::SetValueConstant.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::SetValueConstantMeta.keyword",
    documentationUrl = "/pipeline/transforms/setvalueconstant.html")
@Getter
@Setter
public class SetValueConstantMeta
    extends BaseTransformMeta<SetValueConstant, SetValueConstantData> {
  private static final Class<?> PKG = SetValueConstantMeta.class;

  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "SetValueConstant.Injection.FIELDS")
  private List<Field> fields;

  @HopMetadataProperty(
      key = "usevar",
      injectionKey = "USE_VARIABLE",
      injectionKeyDescription = "SetValueConstant.Injection.USE_VARIABLE")
  private boolean usingVariables;

  public SetValueConstantMeta() {
    super();
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
              BaseMessages.getString(PKG, "SetValueConstantMeta.CheckResult.NotReceivingFields"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SetValueConstantMeta.CheckResult.TransformRecevingData", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (Field field : fields) {
        int idx = prev.indexOfValue(field.getFieldName());
        if (idx < 0) {
          errorMessage += "\t\t" + field.getFieldName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(
                PKG, "SetValueConstantMeta.CheckResult.FieldsFound", errorMessage);

        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        if (Utils.isEmpty(fields)) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_WARNING,
                  BaseMessages.getString(PKG, "SetValueConstantMeta.CheckResult.NoFieldsEntered"),
                  transformMeta);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "SetValueConstantMeta.CheckResult.AllFieldsFound"),
                  transformMeta);
        }
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SetValueConstantMeta.CheckResult.TransformRecevingData2"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "SetValueConstantMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
    }
    remarks.add(cr);
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Getter
  @Setter
  public static class Field {
    @HopMetadataProperty(
        key = "name",
        injectionKey = "FIELD_NAME",
        injectionKeyDescription = "SetValueConstant.Injection.FIELD_NAME")
    private String fieldName;

    @HopMetadataProperty(
        key = "value",
        injectionKey = "REPLACE_VALUE",
        injectionKeyDescription = "SetValueConstant.Injection.REPLACE_VALUE")
    private String replaceValue;

    @HopMetadataProperty(
        key = "mask",
        injectionKey = "REPLACE_MASK",
        injectionKeyDescription = "SetValueConstant.Injection.REPLACE_MASK")
    private String replaceMask;

    @HopMetadataProperty(
        key = "set_empty_string",
        injectionKey = "EMPTY_STRING",
        injectionKeyDescription = "SetValueConstant.Injection.EMPTY_STRING")
    private boolean emptyString;

    public Field() {}

    public Field(Field f) {
      this();
      this.fieldName = f.fieldName;
      this.replaceValue = f.replaceValue;
      this.replaceMask = f.replaceMask;
      this.emptyString = f.emptyString;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Field field = (Field) o;
      return emptyString == field.emptyString
          && Objects.equals(fieldName, field.fieldName)
          && Objects.equals(replaceValue, field.replaceValue)
          && Objects.equals(replaceMask, field.replaceMask);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fieldName, replaceValue, replaceMask, emptyString);
    }
  }
}
