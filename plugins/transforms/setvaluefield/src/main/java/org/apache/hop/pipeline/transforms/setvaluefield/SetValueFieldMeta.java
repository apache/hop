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
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

@InjectionSupported(
    localizationPrefix = "SetValueField.Injection.",
    groups = {"FIELDS"})
@Transform(
    id = "SetValueField",
    image = "setvaluefield.svg",
    name = "i18n::BaseTransform.TypeLongDesc.SetValueField",
    description = "i18n::BaseTransform.TypeTooltipDesc.SetValueField",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/setvaluefield.html")
public class SetValueFieldMeta extends BaseTransformMeta
    implements ITransformMeta<SetValueField, SetValueFieldData> {
  private static final Class<?> PKG = SetValueFieldMeta.class; // For Translator

  @Injection(name = "FIELD_NAME", group = "FIELDS")
  private String[] fieldName;

  @Injection(name = "REPLACE_BY_FIELD_VALUE", group = "FIELDS")
  private String[] replaceByFieldValue;

  public SetValueFieldMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the fieldName. */
  public String[] getFieldName() {
    return fieldName;
  }

  /** @param fieldName The fieldName to set. */
  public void setFieldName(String[] fieldName) {
    this.fieldName = fieldName;
  }

  /** @return Returns the replaceByFieldValue. */
  public String[] getReplaceByFieldValue() {
    return replaceByFieldValue;
  }

  /** @param replaceByFieldValue The replaceByFieldValue to set. */
  public void setReplaceByFieldValue(String[] replaceByFieldValue) {
    this.replaceByFieldValue = replaceByFieldValue;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public void allocate(int count) {
    fieldName = new String[count];
    replaceByFieldValue = new String[count];
  }

  public Object clone() {
    SetValueFieldMeta retval = (SetValueFieldMeta) super.clone();

    int count = fieldName.length;

    retval.allocate(count);
    System.arraycopy(fieldName, 0, retval.fieldName, 0, count);
    System.arraycopy(replaceByFieldValue, 0, retval.replaceByFieldValue, 0, count);

    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      SetValueFieldData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new SetValueField(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int count = XmlHandler.countNodes(fields, "field");

      allocate(count);

      for (int i = 0; i < count; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        fieldName[i] = XmlHandler.getTagValue(fnode, "name");
        replaceByFieldValue[i] = XmlHandler.getTagValue(fnode, "replaceby");
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "SetValueFieldMeta.Exception.UnableToReadTransformMetaFromXML"),
          e);
    }
  }

  public void setDefault() {
    int count = 0;

    allocate(count);

    for (int i = 0; i < count; i++) {
      fieldName[i] = "field" + i;
      replaceByFieldValue[i] = "";
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    <fields>" + Const.CR);

    for (int i = 0; i < fieldName.length; i++) {
      retval.append("      <field>" + Const.CR);
      retval.append("        " + XmlHandler.addTagValue("name", fieldName[i]));
      retval.append("        " + XmlHandler.addTagValue("replaceby", replaceByFieldValue[i]));
      retval.append("        </field>" + Const.CR);
    }
    retval.append("      </fields>" + Const.CR);

    return retval.toString();
  }

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
              CheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "SetValueFieldMeta.CheckResult.NoReceivingFieldsError"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
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
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SetValueFieldMeta.CheckResult.TransformRecevingInfoFromOtherTransforms"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SetValueFieldMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
    }
    remarks.add(cr);

    if (fieldName == null && fieldName.length == 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SetValueFieldMeta.CheckResult.FieldsSelectionEmpty"),
              transformMeta);
      remarks.add(cr);
    } else {
      for (int i = 0; i < fieldName.length; i++) {
        if (Utils.isEmpty(replaceByFieldValue[i])) {
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG,
                      "SetValueFieldMeta.CheckResult.ReplaceByValueMissing",
                      fieldName[i],
                      "" + i),
                  transformMeta);
          remarks.add(cr);
        }
      }
    }
  }

  public SetValueFieldData getTransformData() {
    return new SetValueFieldData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
