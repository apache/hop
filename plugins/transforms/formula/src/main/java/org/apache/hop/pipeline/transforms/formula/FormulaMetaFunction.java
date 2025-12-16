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

package org.apache.hop.pipeline.transforms.formula;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transforms.formula.util.StringToTypeConverter;

@Getter
@Setter
public class FormulaMetaFunction {
  public static final String XML_TAG = "formula";

  @HopMetadataProperty(
      key = "field_name",
      injectionKeyDescription = "FormulaMeta.Injection.FieldName")
  private String fieldName;

  @HopMetadataProperty(
      key = "formula",
      injectionKeyDescription = "FormulaMeta.Injection.FormulaString")
  private String formula;

  @HopMetadataProperty(
      key = "value_type",
      injectionKeyDescription = "FormulaMeta.Injection.ValueType",
      injectionConverter = StringToTypeConverter.class)
  private int valueType;

  @HopMetadataProperty(
      key = "value_length",
      injectionKeyDescription = "FormulaMeta.Injection.ValueLength")
  private int valueLength;

  @HopMetadataProperty(
      key = "value_precision",
      injectionKeyDescription = "FormulaMeta.Injection.ValuePrecision")
  private int valuePrecision;

  @HopMetadataProperty(
      key = "replace_field",
      injectionKeyDescription = "FormulaMeta.Injection.ReplaceField")
  private String replaceField;

  @HopMetadataProperty(key = "set_na", injectionKeyDescription = "FormulaMeta.Injection.setNa")
  private boolean setNa;

  /** This value will be discovered on runtime and need not to be persisted into xml or rep. */
  @SuppressWarnings("java:S2065") // disable sonar warning on transient
  private transient boolean needDataConversion = false;

  /**
   * @param fieldName
   * @param formula
   * @param valueType
   * @param valueLength
   * @param valuePrecision
   * @param replaceField
   */
  public FormulaMetaFunction(
      String fieldName,
      String formula,
      int valueType,
      int valueLength,
      int valuePrecision,
      String replaceField,
      boolean setNa) {
    this.fieldName = fieldName;
    this.formula = formula;
    this.valueType = valueType;
    this.valueLength = valueLength;
    this.valuePrecision = valuePrecision;
    this.replaceField = replaceField;
    this.setNa = setNa;

    ValueMetaFactory.getIdForValueMeta("Boolean");
  }

  public FormulaMetaFunction() {
    valueLength = -1;
    valuePrecision = -1;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fieldName, formula, valueType, valueLength, valuePrecision, replaceField, setNa);
  }

  @Override
  public Object clone() {
    try {
      FormulaMetaFunction retval = (FormulaMetaFunction) super.clone();
      return retval;
    } catch (CloneNotSupportedException e) {
      return null;
    }
  }
}
