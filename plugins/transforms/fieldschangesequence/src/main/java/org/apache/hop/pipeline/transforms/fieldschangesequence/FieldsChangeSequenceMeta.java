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

package org.apache.hop.pipeline.transforms.fieldschangesequence;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 30-06-2008
 *
 */

@Transform(
    id = "FieldsChangeSequence",
    image = "fieldschangesequence.svg",
    name = "i18n::BaseTransform.TypeLongDesc.FieldsChangeSequence",
    description = "i18n::BaseTransform.TypeTooltipDesc.FieldsChangeSequence",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/fieldschangesequence.html")
public class FieldsChangeSequenceMeta extends BaseTransformMeta
    implements ITransformMeta<FieldsChangeSequence, FieldsChangeSequenceData> {
  private static final Class<?> PKG = FieldsChangeSequenceMeta.class; // For Translator

  /** by which fields to display? */
  private String[] fieldName;

  private String resultfieldName;

  private String start;

  private String increment;

  public FieldsChangeSequenceMeta() {
    super(); // allocate BaseTransformMeta
  }

  public String getStart() {
    return start;
  }

  /** @return Returns the resultfieldName. */
  public String getResultFieldName() {
    return resultfieldName;
  }

  /** @param resultfieldName The resultfieldName to set. */
  public void setResultFieldName(String resultfieldName) {
    this.resultfieldName = resultfieldName;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  @Override
  public Object clone() {
    FieldsChangeSequenceMeta retval = (FieldsChangeSequenceMeta) super.clone();

    int nrFields = fieldName.length;

    retval.allocate(nrFields);

    System.arraycopy(fieldName, 0, retval.fieldName, 0, nrFields);
    return retval;
  }

  public void allocate(int nrFields) {
    fieldName = new String[nrFields];
  }

  /** @return Returns the fieldName. */
  public String[] getFieldName() {
    return fieldName;
  }

  /** @param fieldName The fieldName to set. */
  public void setFieldName(String[] fieldName) {
    this.fieldName = fieldName;
  }

  public void setStart(String start) {
    this.start = start;
  }

  public void setIncrement(String increment) {
    this.increment = increment;
  }

  public String getIncrement() {
    return increment;
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      start = XmlHandler.getTagValue(transformNode, "start");
      increment = XmlHandler.getTagValue(transformNode, "increment");
      resultfieldName = XmlHandler.getTagValue(transformNode, "resultfieldName");

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
        fieldName[i] = XmlHandler.getTagValue(fnode, "name");
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();
    retval.append("      " + XmlHandler.addTagValue("start", start));
    retval.append("      " + XmlHandler.addTagValue("increment", increment));
    retval.append("      " + XmlHandler.addTagValue("resultfieldName", resultfieldName));

    retval.append("    <fields>" + Const.CR);
    for (int i = 0; i < fieldName.length; i++) {
      retval.append("      <field>" + Const.CR);
      retval.append("        " + XmlHandler.addTagValue("name", fieldName[i]));
      retval.append("        </field>" + Const.CR);
    }
    retval.append("      </fields>" + Const.CR);

    return retval.toString();
  }

  @Override
  public void setDefault() {
    resultfieldName = null;
    start = "1";
    increment = "1";
    int nrFields = 0;

    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      fieldName[i] = "field" + i;
    }
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (!Utils.isEmpty(resultfieldName)) {
      IValueMeta v = new ValueMetaInteger(resultfieldName);
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      r.addValueMeta(v);
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
    String errorMessage = "";

    if (Utils.isEmpty(resultfieldName)) {
      errorMessage =
          BaseMessages.getString(PKG, "FieldsChangeSequenceMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "FieldsChangeSequenceMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
    }
    remarks.add(cr);

    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(
                  PKG, "FieldsChangeSequenceMeta.CheckResult.NotReceivingFields"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "FieldsChangeSequenceMeta.CheckResult.TransformRecevingData",
                  prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      boolean errorFound = false;
      errorMessage = "";

      // Starting from selected fields in ...
      for (int i = 0; i < fieldName.length; i++) {
        int idx = prev.indexOfValue(fieldName[i]);
        if (idx < 0) {
          errorMessage += "\t\t" + fieldName[i] + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(
                PKG, "FieldsChangeSequenceMeta.CheckResult.FieldsFound", errorMessage);

        cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        if (fieldName.length > 0) {
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG, "FieldsChangeSequenceMeta.CheckResult.AllFieldsFound"),
                  transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_WARNING,
                  BaseMessages.getString(
                      PKG, "FieldsChangeSequenceMeta.CheckResult.NoFieldsEntered"),
                  transformMeta);
          remarks.add(cr);
        }
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FieldsChangeSequenceMeta.CheckResult.TransformRecevingData2"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "FieldsChangeSequenceMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public FieldsChangeSequence createTransform(
      TransformMeta transformMeta,
      FieldsChangeSequenceData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new FieldsChangeSequence(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public FieldsChangeSequenceData getTransformData() {
    return new FieldsChangeSequenceData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
