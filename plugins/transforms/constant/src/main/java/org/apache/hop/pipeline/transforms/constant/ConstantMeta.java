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

package org.apache.hop.pipeline.transforms.constant;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "Constant",
    image = "constant.svg",
    name = "i18n::AddConstants.Name",
    description = "i18n::AddConstants.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::ConstantMeta.keyword",
    documentationUrl = "/pipeline/transforms/addconstant.html")
public class ConstantMeta extends BaseTransformMeta<Constant, ConstantData> {

  private static final Class<?> PKG = ConstantMeta.class; // For Translator

  /** The output fields */
  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupDescription = "ConstantMeta.Injection.Fields",
      injectionKeyDescription = "ConstantMeta.Injection.Field")
  List<ConstantField> fields;

  public List<ConstantField> getFields() {
    return fields;
  }

  public void setFields(List<ConstantField> fields) {
    this.fields = fields;
  }

  public ConstantMeta() {
    super(); // allocate BaseTransformMeta
    this.fields = new ArrayList<>();
  }

  @Override
  public Object clone() {
    return (ConstantMeta) super.clone();
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (int i = 0; i < fields.size(); i++) {
      ConstantField item = fields.get(i);
      if (!StringUtils.isEmpty(item.getFieldName())) {
        int type = ValueMetaFactory.getIdForValueMeta(item.getFieldType());
        if (type == IValueMeta.TYPE_NONE) {
          type = IValueMeta.TYPE_STRING;
        }
        try {
          IValueMeta v = ValueMetaFactory.createValueMeta(item.getFieldName(), type);
          v.setLength(item.getFieldLength());
          v.setPrecision(item.getFieldPrecision());
          v.setOrigin(name);
          v.setConversionMask(item.getFieldFormat());
          rowMeta.addValueMeta(v);
        } catch (Exception e) {
          throw new HopTransformException(e);
        }
      }
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
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ConstantMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ConstantMeta.CheckResult.NoFields"),
              transformMeta);
      remarks.add(cr);
    }

    // Check the constants...
    ConstantData data = new ConstantData();
    ConstantMeta meta = (ConstantMeta) transformMeta.getTransform();
    Constant.buildRow(meta, data, remarks);
  }
}
