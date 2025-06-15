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

package org.apache.hop.pipeline.transforms.concatfields;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/*
 * ConcatFieldsMeta
 */
@Transform(
    id = "ConcatFields",
    image = "concatfields.svg",
    name = "i18n::ConcatFields.Name",
    description = "i18n::ConcatFields.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::ConcatFieldsMeta.keyword",
    documentationUrl = "/pipeline/transforms/concatfields.html")
@Getter
@Setter
public class ConcatFieldsMeta extends BaseTransformMeta<ConcatFields, ConcatFieldsData> {
  private static final Class<?> PKG = ConcatFieldsMeta.class;

  /** The separator to choose for the CSV file */
  @HopMetadataProperty(key = "separator", injectionKey = "SEPARATOR")
  private String separator;

  /** The enclosure to use in case the separator is part of a field's value */
  @HopMetadataProperty(key = "enclosure", injectionKey = "ENCLOSURE")
  private String enclosure;

  @HopMetadataProperty(key = "force_enclosure", injectionKey = "FORCE_ENCLOSURE")
  private boolean forceEnclosure;

  /** The output fields */
  @HopMetadataProperty(groupKey = "fields", key = "field", injectionGroupKey = "OUTPUT_FIELDS")
  private List<ConcatField> outputFields;

  /** Extra fields that need to end up in an extra "ConcatFields" tag during serialization */
  @HopMetadataProperty(key = "ConcatFields")
  private ExtraFields extraFields;

  public ConcatFieldsMeta() {
    super();
    outputFields = new ArrayList<>();
    extraFields = new ExtraFields();
  }

  public ConcatFieldsMeta(ConcatFieldsMeta m) {
    this();
    this.separator = m.separator;
    this.enclosure = m.enclosure;
    this.extraFields = new ExtraFields(m.extraFields);
    for (ConcatField field : outputFields) {
      outputFields.add(new ConcatField(field));
    }
  }

  @Override
  public void setDefault() {
    separator = ";";
    enclosure = "\"";
    forceEnclosure = false;

    // set default for new properties specific to the concat fields
    extraFields.setTargetFieldName("");
    extraFields.setTargetFieldLength(0);
    extraFields.setRemoveSelectedFields(false);
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
    // remove selected fields from the stream when true
    if (extraFields.isRemoveSelectedFields()) {
      if (!getOutputFields().isEmpty()) {
        for (int i = 0; i < getOutputFields().size(); i++) {
          ConcatField field = getOutputFields().get(i);
          try {
            row.removeValueMeta(field.getName());
          } catch (HopValueException e) {
            // just ignore exceptions since missing fields are handled in the ConcatFields class
          }
        }
      } else { // no output fields selected, take them all, remove them all
        row.clear();
      }
    }

    // Check Target Field Name
    if (StringUtil.isEmpty(extraFields.getTargetFieldName())) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "ConcatFieldsMeta.CheckResult.TargetFieldNameMissing"));
    }
    // add targetFieldName
    IValueMeta vValue =
        new ValueMetaString(
            extraFields.getTargetFieldName(), extraFields.getTargetFieldLength(), 0);
    vValue.setOrigin(name);
    row.addValueMeta(vValue);
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

    // Check Target Field Name
    if (StringUtil.isEmpty(extraFields.getTargetFieldName())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ConcatFieldsMeta.CheckResult.TargetFieldNameMissing"),
              transformMeta);
      remarks.add(cr);
    }

    // Check Target Field Length when Fast Data Dump
    if (extraFields.getTargetFieldLength() <= 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(
                  PKG, "ConcatFieldsMeta.CheckResult.TargetFieldLengthMissingFastDataDump"),
              transformMeta);
      remarks.add(cr);
    }

    // Check output fields
    if (!Utils.isEmpty(prev)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ConcatFieldsMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (int i = 0; i < getOutputFields().size(); i++) {
        int idx = prev.indexOfValue(getOutputFields().get(i).getName());
        if (idx < 0) {
          errorMessage += "\t\t" + getOutputFields().get(i).getName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(
                PKG, "ConcatFieldsMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ConcatFieldsMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    }
  }
}
