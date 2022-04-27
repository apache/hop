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

package org.apache.hop.pipeline.transforms.denormaliser;

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
    id = "Denormaliser",
    image = "denormaliser.svg",
    name = "i18n::Denormaliser.Name",
    description = "i18n::Denormaliser.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::DenormaliserMeta.keyword",
    documentationUrl = "/pipeline/transforms/rowdenormaliser.html")
public class DenormaliserMeta extends BaseTransformMeta<Denormaliser, DenormaliserData> {
  private static final Class<?> PKG = DenormaliserMeta.class; // For Translator

  /** Fields to group over */
  @HopMetadataProperty(groupKey = "group", key = "field")
  private List<DenormaliserGroupField> groupFields;

  /** The key field */
  @HopMetadataProperty(
      key = "key_field",
      injectionKeyDescription = "DenormaliserDialog.KeyField.Label")
  private String keyField;

  /** The fields to unpivot */
  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<DenormaliserTargetField> denormaliserTargetFields;

  public DenormaliserMeta() {
    groupFields = new ArrayList<>();
    denormaliserTargetFields = new ArrayList<>();
  }

  public DenormaliserMeta(DenormaliserMeta m) {
    this.denormaliserTargetFields = m.denormaliserTargetFields;
    this.keyField = m.keyField;
    this.groupFields = m.groupFields;
  }

  @Override
  public DenormaliserMeta clone() {
    DenormaliserMeta meta = new DenormaliserMeta();

    for (DenormaliserTargetField target : denormaliserTargetFields) {
      meta.getDenormaliserTargetFields().add(new DenormaliserTargetField(target));
    }

    return meta;
  }

  /** @return Returns the keyField. */
  public String getKeyField() {
    return keyField;
  }

  /** @param keyField The keyField to set. */
  public void setKeyField(String keyField) {
    this.keyField = keyField;
  }

  /** @return Returns the groupField. */
  public List<DenormaliserGroupField> getGroupFields() {
    return groupFields;
  }

  /** @param groupFields The groupField to set. */
  public void setGroupFields(List<DenormaliserGroupField> groupFields) {
    this.groupFields = groupFields;
  }

  /** @return Return the Targetfields */
  public List<DenormaliserTargetField> getDenormaliserTargetFields() {
    return denormaliserTargetFields;
  }

  /** @param denormaliserTargetFields the denormaliserTargetField to set */
  public void setDenormaliserTargetFields(List<DenormaliserTargetField> denormaliserTargetFields) {
    this.denormaliserTargetFields = denormaliserTargetFields;
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

    // Remove the key value (there will be different entries for each output row)
    //
    if (keyField != null && keyField.length() > 0) {
      int idx = row.indexOfValue(keyField);
      if (idx < 0) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "DenormaliserMeta.Exception.UnableToLocateKeyField", keyField));
      }
      row.removeValueMeta(idx);
    } else {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "DenormaliserMeta.Exception.RequiredKeyField"));
    }

    // Remove all field value(s) (there will be different entries for each output row)
    //
    for (int i = 0; i < denormaliserTargetFields.size(); i++) {
      String fieldname = denormaliserTargetFields.get(i).getFieldName();
      if (fieldname != null && fieldname.length() > 0) {
        int idx = row.indexOfValue(fieldname);
        if (idx >= 0) {
          row.removeValueMeta(idx);
        }
      } else {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "DenormaliserMeta.Exception.RequiredTargetFieldName", (i + 1) + ""));
      }
    }

    // Re-add the target fields
    for (DenormaliserTargetField field : denormaliserTargetFields) {
      try {
        IValueMeta target =
            ValueMetaFactory.createValueMeta(
                field.getTargetName(), ValueMetaFactory.getIdForValueMeta(field.getTargetType()));
        target.setLength(field.getTargetLength(), field.getTargetPrecision());
        target.setOrigin(name);
        row.addValueMeta(target);
      } catch (Exception e) {
        throw new HopTransformException(e);
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

    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "DenormaliserMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DenormaliserMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }
}
