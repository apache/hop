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

package org.apache.hop.pipeline.transforms.normaliser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/*

DATE      PRODUCT1_NR  PRODUCT1_SL  PRODUCT2_NR PRODUCT2_SL PRODUCT3_NR PRODUCT3_SL
20030101            5          100           10         250           4         150

DATE      PRODUCT    Sales   Number
20030101  PRODUCT1     100        5
20030101  PRODUCT2     250       10
20030101  PRODUCT3     150        4

--> we need a mapping of fields with occurances.  (PRODUCT1_NR --> "PRODUCT1", PRODUCT1_SL --> "PRODUCT1", ...)
--> List of Fields with the type and the new fieldname to fill
--> PRODUCT1_NR, "PRODUCT1", Number
--> PRODUCT1_SL, "PRODUCT1", Sales
--> PRODUCT2_NR, "PRODUCT2", Number
--> PRODUCT2_SL, "PRODUCT2", Sales
--> PRODUCT3_NR, "PRODUCT3", Number
--> PRODUCT3_SL, "PRODUCT3", Sales

--> To parse this, we loop over the occurances of type: "PRODUCT1", "PRODUCT2" and "PRODUCT3"
--> For each of the occurance, we insert a record.

**/

@Transform(
    id = "Normaliser",
    name = "i18n::Normaliser.Name",
    description = "i18n::Normaliser.Description",
    image = "normaliser.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::NormaliserMeta.keyword",
    documentationUrl = "/pipeline/transforms/rownormaliser.html")
public class NormaliserMeta extends BaseTransformMeta<Normaliser, NormaliserData> {
  private static final Class<?> PKG = NormaliserMeta.class;

  /** Name of the new type-field */
  @HopMetadataProperty(
      key = "typefield",
      injectionKey = "TYPEFIELD",
      injectionKeyDescription = "NormaliserMeta.Injection.TYPEFIELD")
  private String typeField;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "NormaliserMeta.Injection.FIELDS")
  private List<NormaliserField> normaliserFields;

  public NormaliserMeta() {
    super();
    this.normaliserFields = new ArrayList<>();
  }

  public NormaliserMeta(NormaliserMeta meta) {
    this();
    for (NormaliserField field : meta.normaliserFields) {
      normaliserFields.add(new NormaliserField(field));
    }
  }

  /**
   * @return Returns the typeField.
   */
  public String getTypeField() {
    return typeField;
  }

  /**
   * @param typeField The typeField to set.
   */
  public void setTypeField(String typeField) {
    this.typeField = typeField;
  }

  public List<NormaliserField> getNormaliserFields() {
    return normaliserFields;
  }

  public void setNormaliserFields(List<NormaliserField> normaliserFields) {
    this.normaliserFields = normaliserFields;
  }

  public Set<String> getFieldNames() {
    Set<String> fieldNames = new HashSet<>();

    for (NormaliserField field : normaliserFields) {
      if (field.getName() != null) {
        fieldNames.add(field.getName().toLowerCase());
      }
    }
    return fieldNames;
  }

  @Override
  public Object clone() {
    return new NormaliserMeta(this);
  }

  @Override
  public void setDefault() {
    this.typeField = "typefield";
    this.normaliserFields = new ArrayList<>();
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

    // Get a unique list of the occurrences of the type
    //
    List<String> normOcc = new ArrayList<>();
    List<String> fieldOcc = new ArrayList<>();
    int maxlen = 0;
    // for (int i = 0; i < normaliserFields.length; i++) {
    for (NormaliserField field : normaliserFields) {
      if (!normOcc.contains(field.getNorm())) {
        normOcc.add(field.getNorm());
        fieldOcc.add(field.getName());
      }

      if (field.getValue().length() > maxlen) {
        maxlen = field.getValue().length();
      }
    }

    // Then add the type field!
    //
    IValueMeta typefieldValue = new ValueMetaString(typeField);
    typefieldValue.setOrigin(name);
    typefieldValue.setLength(maxlen);
    row.addValueMeta(typefieldValue);

    // Loop over the distinct list of fieldNorm[i]
    // Add the new fields that need to be created.
    // Use the same data type as the original fieldname...
    //
    for (int i = 0; i < normOcc.size(); i++) {
      String normname = normOcc.get(i);
      String fieldname = fieldOcc.get(i);
      IValueMeta v = row.searchValueMeta(fieldname);
      if (v != null) {
        v = v.clone();
      } else {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "NormaliserMeta.Exception.UnableToFindField", fieldname));
      }
      v.setName(normname);
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    // Now remove all the normalized fields...
    //
    for (NormaliserField field : normaliserFields) {
      int idx = row.indexOfValue(field.getName());
      if (idx >= 0) {
        row.removeValueMeta(idx);
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

    String errorMessage = "";
    CheckResult cr;

    // Look up fields in the input stream <prev>
    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "NormaliserMeta.CheckResult.TransformReceivingFieldsOK", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      boolean first = true;
      errorMessage = "";
      boolean errorFound = false;

      for (NormaliserField field : normaliserFields) {
        IValueMeta valueMeta = prev.searchValueMeta(field.getName());
        if (valueMeta == null) {
          if (first) {
            first = false;
            errorMessage +=
                BaseMessages.getString(PKG, "NormaliserMeta.CheckResult.FieldsNotFound") + Const.CR;
          }
          errorFound = true;
          errorMessage += "\t\t" + field.getName() + Const.CR;
        }
      }
      if (errorFound) {
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "NormaliserMeta.CheckResult.AllFieldsFound"),
                transformMeta);
      }
      remarks.add(cr);
    } else {
      errorMessage =
          BaseMessages.getString(
                  PKG, "NormaliserMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform")
              + Const.CR;
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "NormaliserMeta.CheckResult.TransformReceivingInfoOK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "NormaliserMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
      remarks.add(cr);
    }
  }
}
