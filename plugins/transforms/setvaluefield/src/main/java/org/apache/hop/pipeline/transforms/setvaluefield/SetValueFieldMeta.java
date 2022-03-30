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

package org.apache.hop.pipeline.transforms.setvaluefield;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;
import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "SetValueField",
    image = "setvaluefield.svg",
    name = "i18n::SetValueField.Name",
    description = "i18n::SetValueField.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::SetValueFieldMeta.keyword",
    documentationUrl = "/pipeline/transforms/setvaluefield.html")
public class SetValueFieldMeta extends BaseTransformMeta<SetValueField, SetValueFieldData> {
  private static final Class<?> PKG = SetValueFieldMeta.class; // For Translator
  
  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupDescription = "SetValueField.Injection.SetFields")
  private List<SetField> fields = new ArrayList<>();
    
  public SetValueFieldMeta() {
    super();
  }

  public SetValueFieldMeta(SetValueFieldMeta clone) {
    super();
    for (SetField field : clone.getFields()) {
      fields.add(new SetField(field));
    }
  }
  
  public List<SetField> getFields() {
    return fields;
  }

  public void setFields(List<SetField> fields) {
    this.fields = fields;
  }

  @Override
  public Object clone() {
    return new SetValueFieldMeta(this);
  }

  @Override
  public void setDefault() {
    fields = new ArrayList<>();
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
    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "SetValueFieldMeta.CheckResult.NoReceivingFieldsError"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "SetValueFieldMeta.CheckResult.TransformReceivingFieldsOK",
                  prev.size() + ""),
              transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SetValueFieldMeta.CheckResult.TransformRecevingInfoFromOtherTransforms"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SetValueFieldMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
    }
    remarks.add(cr);

    if (fields==null || fields.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SetValueFieldMeta.CheckResult.FieldsSelectionEmpty"),
              transformMeta);
      remarks.add(cr);
    } else {
      int i = 1;
      for (SetField field:fields) {
        if (Utils.isEmpty(field.getReplaceByField())) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG,
                      "SetValueFieldMeta.CheckResult.ReplaceByValueMissing",
                      field.getFieldName(),
                      "" + i),
                  transformMeta);
          remarks.add(cr);
        }
        i++;
      }
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
