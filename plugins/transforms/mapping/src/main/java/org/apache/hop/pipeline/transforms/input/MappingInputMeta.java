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

package org.apache.hop.pipeline.transforms.input;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "MappingInput",
    name = "i18n::BaseTransform.TypeLongDesc.MappingInput",
    description = "i18n::BaseTransform.TypeTooltipDesc.MappingInput",
    image = "MPI.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Mapping",
    keywords = "i18n::MappingInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/mapping-input.html")
public class MappingInputMeta extends BaseTransformMeta<MappingInput, MappingInputData> {

  private static final Class<?> PKG = MappingInputMeta.class;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<InputField> fields;

  // This information is injected, not serialized
  private IRowMeta inputRowMeta;

  public MappingInputMeta() {
    super();
    this.fields = new ArrayList<>();
  }

  public MappingInputMeta(MappingInputMeta m) {
    this();
    for (InputField field : m.fields) {
      fields.add(new InputField(field));
    }
  }

  @Override
  public MappingInputMeta clone() {
    return new MappingInputMeta(this);
  }

  @Override
  public void setDefault() {
    // Do nothing
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Row should normally be empty when we get here.
    // That is because there is no previous transform to this mapping input transform from the
    // viewpoint of this single
    // sub-pipeline.
    // From the viewpoint of the pipeline that executes the mapping, it's important to know what
    // comes out at the
    // exit points.
    // For that reason we need to re-order etc, based on the input specification...
    //
    if (inputRowMeta != null && !inputRowMeta.isEmpty()) {
      // this gets set only in the parent pipeline...
      // It includes all the renames that needed to be done
      //
      row.mergeRowMeta(inputRowMeta);

      // Validate the existence of all the specified fields...
      //
      if (!row.isEmpty()) {
        for (InputField field : fields) {
          if (row.indexOfValue(field.getName()) < 0) {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG, "MappingInputMeta.Exception.UnknownField", field.getName()));
          }
        }
      }
    } else {
      if (row.isEmpty()) {
        for (InputField field : fields) {
          if (StringUtils.isEmpty(field.getName())) {
            continue;
          }
          int valueType = ValueMetaFactory.getIdForValueMeta(field.getType());
          int valueLength = Const.toInt(field.getLength(), -1);
          int valuePrecision = Const.toInt(field.getPrecision(), -1);
          if (valueType == IValueMeta.TYPE_NONE) {
            valueType = IValueMeta.TYPE_STRING;
          }

          IValueMeta v;
          try {
            v = ValueMetaFactory.createValueMeta(field.getName(), valueType);
            v.setLength(valueLength);
            v.setPrecision(valuePrecision);
            v.setOrigin(origin);
            row.addValueMeta(v);
          } catch (HopPluginException e) {
            throw new HopTransformException(e);
          }
        }
      }

      // else: row is OK, keep it as it is.
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
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MappingInputMeta.CheckResult.NotReceivingFieldsError"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG,
                  "MappingInputMeta.CheckResult.TransformReceivingDatasFromPreviousOne",
                  prev.size() + ""),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "MappingInputMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MappingInputMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public void setInputRowMeta(IRowMeta inputRowMeta) {
    this.inputRowMeta = inputRowMeta;
  }

  /**
   * @return the inputRowMeta
   */
  public IRowMeta getInputRowMeta() {
    return inputRowMeta;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<InputField> getFields() {
    return fields;
  }

  /**
   * Sets fields
   *
   * @param fields value of fields
   */
  public void setFields(List<InputField> fields) {
    this.fields = fields;
  }
}
