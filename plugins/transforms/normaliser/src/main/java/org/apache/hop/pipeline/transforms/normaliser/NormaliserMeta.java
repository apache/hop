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
import java.util.Objects;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
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

--> we need a mapping of fields with occurrences.  (PRODUCT1_NR --> "PRODUCT1", PRODUCT1_SL --> "PRODUCT1", ...)
--> List of Fields with the type and the new fieldname to fill
--> PRODUCT1_NR, "PRODUCT1", Number
--> PRODUCT1_SL, "PRODUCT1", Sales
--> PRODUCT2_NR, "PRODUCT2", Number
--> PRODUCT2_SL, "PRODUCT2", Sales
--> PRODUCT3_NR, "PRODUCT3", Number
--> PRODUCT3_SL, "PRODUCT3", Sales

--> To parse this, we loop over the occurrences of type: "PRODUCT1", "PRODUCT2" and "PRODUCT3"
--> For each of the occurrence, we insert a record.

**/

@Transform(
    id = "Normaliser",
    name = "i18n::Normaliser.Name",
    description = "i18n::Normaliser.Description",
    image = "normaliser.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::NormaliserMeta.keyword",
    documentationUrl = "/pipeline/transforms/rownormaliser.html")
@Getter
@Setter
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
  public void setDefault() {
    this.typeField = "typefield";
    this.normaliserFields = new ArrayList<>();
  }

  /** The names of the normalised fields, in output order: in the order they first appear. */
  public List<String> getNormalisedFieldNames() {
    List<String> names = new ArrayList<>();
    for (NormaliserField field : normaliserFields) {
      if (!names.contains(field.getNorm())) {
        names.add(field.getNorm());
      }
    }
    return names;
  }

  /**
   * The normalised fields, in output order.
   *
   * <p>A normalised field is filled from a different input field on every row it writes, so it has
   * to describe all of them. When they share a type it takes that type, from the first of them, as
   * it always has. When they do not, it is a String, and the transform writes the text of each
   * value into it: a field that is declared one type and holds another on some rows fails the first
   * transform that serializes or renders it. See issue #3636.
   *
   * @param inputRowMeta the fields entering the transform
   * @return one value metadata per name of {@link #getNormalisedFieldNames()}, in the same order
   * @throws HopTransformException when the first input field of a normalised field is missing
   */
  public List<IValueMeta> getNormalisedValueMetas(IRowMeta inputRowMeta)
      throws HopTransformException {
    List<IValueMeta> valueMetas = new ArrayList<>();
    for (String normName : getNormalisedFieldNames()) {
      List<NormaliserField> fields = getFieldsOf(normName);
      IValueMeta first = inputRowMeta.searchValueMeta(fields.get(0).getName());
      if (first == null) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "NormaliserMeta.Exception.UnableToFindField", fields.get(0).getName()));
      }
      boolean sameType = true;
      boolean sameStorage = true;
      for (NormaliserField field : fields) {
        IValueMeta source = inputRowMeta.searchValueMeta(field.getName());
        if (source != null) {
          sameType &= source.getType() == first.getType();
          sameStorage &= source.getStorageType() == first.getStorageType();
        }
      }

      IValueMeta v;
      if (!sameType) {
        v = new ValueMetaString(normName);
      } else {
        v = first.clone();
        if (!sameStorage) {
          v.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
          v.setStorageMetadata(null);
        }
      }
      v.setName(normName);
      valueMetas.add(v);
    }
    return valueMetas;
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

    int maxlen = 0;
    for (NormaliserField field : normaliserFields) {
      if (field.getValue().length() > maxlen) {
        maxlen = field.getValue().length();
      }
    }

    // Take the normalised fields from the input before adding anything to it.
    //
    List<IValueMeta> normalisedValueMetas = getNormalisedValueMetas(row);

    // Then add the type field!
    //
    IValueMeta typefieldValue = new ValueMetaString(typeField);
    typefieldValue.setOrigin(name);
    typefieldValue.setLength(maxlen);
    row.addValueMeta(typefieldValue);

    // Add the new fields that need to be created.
    //
    for (IValueMeta v : normalisedValueMetas) {
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

      for (String normName : getNormalisedFieldNames()) {
        if (hasMixedTypes(prev, normName)) {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_WARNING,
                  BaseMessages.getString(PKG, "NormaliserMeta.CheckResult.MixedTypes", normName),
                  transformMeta));
        }
      }
    } else {
      errorMessage =
          BaseMessages.getString(
                  PKG, "NormaliserMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform")
              + Const.CR;
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    String duplicate = getDuplicateMapping();
    if (duplicate != null) {
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, duplicate, transformMeta));
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

  /** The fields filling a normalised field, in the order they are listed. */
  private List<NormaliserField> getFieldsOf(String normName) {
    return normaliserFields.stream()
        .filter(field -> Objects.equals(normName, field.getNorm()))
        .toList();
  }

  /** True when the input fields filling this normalised field are not all of one type. */
  private boolean hasMixedTypes(IRowMeta inputRowMeta, String normName) {
    return getFieldsOf(normName).stream()
            .map(field -> inputRowMeta.searchValueMeta(field.getName()))
            .filter(Objects::nonNull)
            .map(IValueMeta::getType)
            .distinct()
            .count()
        > 1;
  }

  /**
   * Two input fields filling the same normalised field on the same row leave no room for one of
   * them.
   *
   * @return a description of the first such pair, or null when there is none
   */
  public String getDuplicateMapping() {
    for (int i = 0; i < normaliserFields.size(); i++) {
      NormaliserField one = normaliserFields.get(i);
      for (int j = i + 1; j < normaliserFields.size(); j++) {
        NormaliserField other = normaliserFields.get(j);
        if (Objects.equals(one.getValue(), other.getValue())
            && Objects.equals(one.getNorm(), other.getNorm())) {
          return BaseMessages.getString(
              PKG,
              "NormaliserMeta.CheckResult.DuplicateMapping",
              one.getName(),
              other.getName(),
              one.getNorm(),
              one.getValue());
        }
      }
    }
    return null;
  }
}
