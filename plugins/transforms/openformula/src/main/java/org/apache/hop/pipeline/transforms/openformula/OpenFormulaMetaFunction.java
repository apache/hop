/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.openformula;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * One OpenFormula calculation: output field, expression, target type, and optional replace field.
 *
 * <p>XML keys match PDI Formula after Hop's Kettle importer ({@code formula_string} renamed to
 * {@code formula}, {@code value_type} stored as a Hop type id).
 */
@Getter
@Setter
public class OpenFormulaMetaFunction {
  public static final String XML_TAG = "formula";

  @HopMetadataProperty(
      key = "field_name",
      injectionKeyDescription = "OpenFormulaMeta.Injection.FieldName")
  private String fieldName;

  @HopMetadataProperty(
      key = "formula",
      injectionKeyDescription = "OpenFormulaMeta.Injection.FormulaString")
  private String formula;

  @HopMetadataProperty(
      key = "value_type",
      injectionKeyDescription = "OpenFormulaMeta.Injection.ValueType",
      injectionConverter = StringToTypeConverter.class)
  private int valueType;

  @HopMetadataProperty(
      key = "value_length",
      injectionKeyDescription = "OpenFormulaMeta.Injection.ValueLength")
  private int valueLength;

  @HopMetadataProperty(
      key = "value_precision",
      injectionKeyDescription = "OpenFormulaMeta.Injection.ValuePrecision")
  private int valuePrecision;

  @HopMetadataProperty(
      key = "replace_field",
      injectionKeyDescription = "OpenFormulaMeta.Injection.ReplaceField")
  private String replaceField;

  public OpenFormulaMetaFunction() {
    valueLength = -1;
    valuePrecision = -1;
  }

  public OpenFormulaMetaFunction(
      String fieldName,
      String formula,
      int valueType,
      int valueLength,
      int valuePrecision,
      String replaceField) {
    this.fieldName = fieldName;
    this.formula = formula;
    this.valueType = valueType;
    this.valueLength = valueLength;
    this.valuePrecision = valuePrecision;
    this.replaceField = replaceField;
  }

  public OpenFormulaMetaFunction(OpenFormulaMetaFunction other) {
    this.fieldName = other.fieldName;
    this.formula = other.formula;
    this.valueType = other.valueType;
    this.valueLength = other.valueLength;
    this.valuePrecision = other.valuePrecision;
    this.replaceField = other.replaceField;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OpenFormulaMetaFunction that)) {
      return false;
    }
    return valueType == that.valueType
        && valueLength == that.valueLength
        && valuePrecision == that.valuePrecision
        && Objects.equals(fieldName, that.fieldName)
        && Objects.equals(formula, that.formula)
        && Objects.equals(replaceField, that.replaceField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, formula, valueType, valueLength, valuePrecision, replaceField);
  }
}
