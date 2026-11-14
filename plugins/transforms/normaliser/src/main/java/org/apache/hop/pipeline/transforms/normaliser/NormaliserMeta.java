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
import java.util.List;
import java.util.Objects;
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
import org.apache.hop.core.util.Utils;
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

  public static final String DEFAULT_VALUE_FIELD = "value";

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

  /** Name of the field that holds the values when every input field is normalised. */
  @HopMetadataProperty(
      key = "valuefield",
      injectionKey = "VALUEFIELD",
      injectionKeyDescription = "NormaliserMeta.Injection.VALUEFIELD")
  private String valueField;

  public NormaliserMeta() {
    super();
    this.normaliserFields = new ArrayList<>();
  }

  @Override
  public void setDefault() {
    this.typeField = "typefield";
    this.valueField = DEFAULT_VALUE_FIELD;
    this.normaliserFields = new ArrayList<>();
  }

  /**
   * @param normaliserFields the fields to normalise; null, from metadata injection for example, is
   *     an empty list: every input field is normalised then
   */
  public void setNormaliserFields(List<NormaliserField> normaliserFields) {
    this.normaliserFields = normaliserFields == null ? new ArrayList<>() : normaliserFields;
  }

  /** True when no fields are listed: every input field is normalised then. */
  public boolean isNormalisingAllFields() {
    return normaliserFields == null || normaliserFields.isEmpty();
  }

  /** The name of the value field, or its default when none is set. */
  public String getValueFieldName() {
    return Utils.isEmpty(valueField) ? DEFAULT_VALUE_FIELD : valueField;
  }

  /**
   * The fields to normalise: the listed fields, or with an empty list every input field. An input
   * field normalised that way gets its own name as type and puts its value in the value field.
   *
   * @param inputRowMeta the fields entering the transform
   * @return the fields to normalise
   */
  public List<NormaliserField> getFieldsToNormalise(IRowMeta inputRowMeta) {
    if (!isNormalisingAllFields()) {
      return normaliserFields;
    }
    String norm = getValueFieldName();
    List<NormaliserField> fields = new ArrayList<>(inputRowMeta.size());
    for (IValueMeta valueMeta : inputRowMeta.getValueMetaList()) {
      NormaliserField field = new NormaliserField();
      field.setName(valueMeta.getName());
      field.setValue(valueMeta.getName());
      field.setNorm(norm);
      fields.add(field);
    }
    return fields;
  }

  /**
   * The names of the normalised fields, in output order: in the order they first appear.
   *
   * @param fields the fields to normalise, see {@link #getFieldsToNormalise(IRowMeta)}
   */
  public static List<String> getNormalisedFieldNames(List<NormaliserField> fields) {
    List<String> names = new ArrayList<>();
    for (NormaliserField field : fields) {
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
   * @return one value metadata per name of {@link #getNormalisedFieldNames(List)}, in the same
   *     order
   * @throws HopTransformException when the first input field of a normalised field is missing
   */
  public List<IValueMeta> getNormalisedValueMetas(IRowMeta inputRowMeta)
      throws HopTransformException {
    return getNormalisedValueMetas(inputRowMeta, getFieldsToNormalise(inputRowMeta));
  }

  private List<IValueMeta> getNormalisedValueMetas(
      IRowMeta inputRowMeta, List<NormaliserField> fieldsToNormalise) throws HopTransformException {
    boolean allFields = isNormalisingAllFields();
    List<IValueMeta> valueMetas = new ArrayList<>();
    if (allFields && fieldsToNormalise.isEmpty()) {
      // No input fields are known, while designing a pipeline whose input is only known when it
      // runs for example. The value field is there all the same, and it can hold any value as text.
      //
      valueMetas.add(new ValueMetaString(getValueFieldName()));
      return valueMetas;
    }
    for (String normName : getNormalisedFieldNames(fieldsToNormalise)) {
      List<NormaliserField> fields = getFieldsOf(fieldsToNormalise, normName);
      IValueMeta first = inputRowMeta.searchValueMeta(fields.get(0).getName());
      if (first == null) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "NormaliserMeta.Exception.UnableToFindField", fields.get(0).getName()));
      }
      boolean sameType = true;
      boolean sameStorage = true;
      boolean sameLength = true;
      boolean samePrecision = true;
      for (NormaliserField field : fields) {
        IValueMeta source = inputRowMeta.searchValueMeta(field.getName());
        if (source != null) {
          sameType &= source.getType() == first.getType();
          sameStorage &= source.getStorageType() == first.getStorageType();
          sameLength &= source.getLength() == first.getLength();
          samePrecision &= source.getPrecision() == first.getPrecision();
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
        // With every input field normalised, which one comes first is up to the input. The value
        // field keeps only a length or precision they all share: the first field's could be too
        // short for the values of the others, in a database table or a fixed width file.
        //
        if (allFields && !sameLength) {
          v.setLength(-1);
        }
        if (allFields && !samePrecision) {
          v.setPrecision(-1);
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

    boolean allFields = isNormalisingAllFields();
    List<NormaliserField> fields = getFieldsToNormalise(row);

    int maxlen = 0;
    for (NormaliserField field : fields) {
      if (field.getValue().length() > maxlen) {
        maxlen = field.getValue().length();
      }
    }

    // Take the normalised fields from the input before changing it.
    //
    List<IValueMeta> normalisedValueMetas = getNormalisedValueMetas(row, fields);

    // With every input field normalised, remove them before adding the new fields: the value field
    // may have the name of an input field, value for example, and keeps it.
    //
    if (allFields) {
      removeFields(row, fields);
    }

    // Then add the type field!
    //
    IValueMeta typefieldValue = new ValueMetaString(typeField);
    typefieldValue.setOrigin(name);
    if (!fields.isEmpty()) {
      typefieldValue.setLength(maxlen);
    }
    row.addValueMeta(typefieldValue);

    // Add the new fields that need to be created.
    //
    for (IValueMeta v : normalisedValueMetas) {
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    // Listed fields are removed after the new fields are added, as they always were: a new field
    // with the name of a listed field is called value_1 then, and pipelines rely on that name.
    //
    if (!allFields) {
      removeFields(row, fields);
    }
  }

  private static void removeFields(IRowMeta row, List<NormaliserField> fields) {
    for (NormaliserField field : fields) {
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

      List<NormaliserField> fields = getFieldsToNormalise(prev);
      if (isNormalisingAllFields()) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "NormaliserMeta.CheckResult.AllInputFieldsNormalised",
                    prev.size() + "",
                    getValueFieldName()),
                transformMeta));
      }

      boolean first = true;
      errorMessage = "";
      boolean errorFound = false;

      for (NormaliserField field : fields) {
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

      for (String normName : getNormalisedFieldNames(fields)) {
        if (hasMixedTypes(prev, fields, normName)) {
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

    String duplicate = getDuplicateMapping(normaliserFields);
    if (duplicate != null) {
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, duplicate, transformMeta));
    }

    // The type field comes first, so a new field with its name is renamed, to value_1 for example.
    //
    List<String> newFieldNames =
        isNormalisingAllFields()
            ? List.of(getValueFieldName())
            : getNormalisedFieldNames(normaliserFields);
    for (String newFieldName : newFieldNames) {
      if (newFieldName != null && newFieldName.equalsIgnoreCase(typeField)) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_WARNING,
                BaseMessages.getString(
                    PKG, "NormaliserMeta.CheckResult.SameNameAsTypeField", newFieldName),
                transformMeta));
      }
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
  private static List<NormaliserField> getFieldsOf(List<NormaliserField> fields, String normName) {
    return fields.stream().filter(field -> Objects.equals(normName, field.getNorm())).toList();
  }

  /** True when the input fields filling this normalised field are not all of one type. */
  private static boolean hasMixedTypes(
      IRowMeta inputRowMeta, List<NormaliserField> fields, String normName) {
    return getFieldsOf(fields, normName).stream()
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
   * @param fields the fields to normalise, see {@link #getFieldsToNormalise(IRowMeta)}
   * @return a description of the first such pair, or null when there is none
   */
  public static String getDuplicateMapping(List<NormaliserField> fields) {
    if (fields == null) {
      return null;
    }
    for (int i = 0; i < fields.size(); i++) {
      NormaliserField one = fields.get(i);
      for (int j = i + 1; j < fields.size(); j++) {
        NormaliserField other = fields.get(j);
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
