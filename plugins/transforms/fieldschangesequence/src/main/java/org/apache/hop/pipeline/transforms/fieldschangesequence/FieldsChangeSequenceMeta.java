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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

/** Add sequence depending of fields value change. */
@Transform(
    id = "FieldsChangeSequence",
    image = "fieldschangesequence.svg",
    name = "i18n::FieldsChangeSequence.Name",
    description = "i18n::FieldsChangeSequence.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl =
        "https://hop.apache.org/manual/latest/pipeline/transforms/addfieldschangesequence.html")
public class FieldsChangeSequenceMeta extends BaseTransformMeta
    implements ITransformMeta<FieldsChangeSequence, FieldsChangeSequenceData> {
  private static final Class<?> PKG = FieldsChangeSequenceMeta.class; // For Translator

  /** by which fields to display? */
  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupDescription = "FieldsChangeSequenceMeta.Injection.Fields",
      injectionKeyDescription = "FieldsChangeSequenceMeta.Injection.Field")
  private List<FieldsChangeSequenceField> fields;

  @HopMetadataProperty(
      key = "resultfieldName",
      injectionKeyDescription = "FieldsChangeSequenceMeta.Injection.ResultFieldName")
  private String resultFieldName;

  @HopMetadataProperty(
      key = "start",
      injectionKeyDescription = "FieldsChangeSequenceMeta.Injection.Start")
  private String start;

  @HopMetadataProperty(
      key = "increment",
      injectionKeyDescription = "FieldsChangeSequenceMeta.Injection.Increment")
  private String increment;

  public FieldsChangeSequenceMeta() {
    super();
    fields = new ArrayList<>();
  }

  public FieldsChangeSequenceMeta(FieldsChangeSequenceMeta meta) {
    super();

    this.start = meta.getStart();
    this.increment = meta.getIncrement();
    this.resultFieldName = meta.getResultFieldName();
    this.fields = new ArrayList<>();
    for (FieldsChangeSequenceField field : meta.getFields()) {
      fields.add(new FieldsChangeSequenceField(field.getName()));
    }
  }

  public String getStart() {
    return start;
  }

  /** @return Returns the resultfieldName. */
  public String getResultFieldName() {
    return resultFieldName;
  }

  /** @param resultfieldName The resultfieldName to set. */
  public void setResultFieldName(String resultfieldName) {
    this.resultFieldName = resultfieldName;
  }

  @Override
  public Object clone() {
    return new FieldsChangeSequenceMeta(this);
  }

  /** @return Returns the fieldName. */
  public List<FieldsChangeSequenceField> getFields() {
    return fields;
  }

  /** @param fieldName The fieldName to set. */
  public void setFields(List<FieldsChangeSequenceField> fieldName) {
    this.fields = fieldName;
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

  @Override
  public void setDefault() {
    resultFieldName = null;
    start = "1";
    increment = "1";
    fields = new ArrayList<>();
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (!Utils.isEmpty(resultFieldName)) {
      IValueMeta v = new ValueMetaInteger(resultFieldName);
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

    if (Utils.isEmpty(resultFieldName)) {
      errorMessage =
          BaseMessages.getString(PKG, "FieldsChangeSequenceMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "FieldsChangeSequenceMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
    }
    remarks.add(cr);

    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(
                  PKG, "FieldsChangeSequenceMeta.CheckResult.NotReceivingFields"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "FieldsChangeSequenceMeta.CheckResult.TransformRecevingData",
                  prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      boolean errorFound = false;
      errorMessage = "";

      // Starting from selected fields in ...
      for (FieldsChangeSequenceField field : fields) {
        int idx = prev.indexOfValue(field.getName());
        if (idx < 0) {
          errorMessage += "\t\t" + field.getName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(
                PKG, "FieldsChangeSequenceMeta.CheckResult.FieldsFound", errorMessage);

        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        if (fields.isEmpty()) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_WARNING,
                  BaseMessages.getString(
                      PKG, "FieldsChangeSequenceMeta.CheckResult.NoFieldsEntered"),
                  transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG, "FieldsChangeSequenceMeta.CheckResult.AllFieldsFound"),
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
              BaseMessages.getString(
                  PKG, "FieldsChangeSequenceMeta.CheckResult.TransformRecevingData2"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
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
