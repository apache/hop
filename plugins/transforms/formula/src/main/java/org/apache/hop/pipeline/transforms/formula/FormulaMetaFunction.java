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

import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

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
      injectionKeyDescription = "FormulaMeta.Injection.ValueType")
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
      String replaceField) {
    this.fieldName = fieldName;
    this.formula = formula;
    this.valueType = valueType;
    this.valueLength = valueLength;
    this.valuePrecision = valuePrecision;
    this.replaceField = replaceField;

    ValueMetaFactory.getIdForValueMeta("Boolean");
  }

  public FormulaMetaFunction() {
    valueLength = -1;
    valuePrecision = -1;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, formula, valueType, valueLength, valuePrecision, replaceField);
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

  /**
   * @return Returns the fieldName.
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName The fieldName to set.
   */
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  /**
   * @return Returns the valueLength.
   */
  public int getValueLength() {
    return valueLength;
  }

  /**
   * @param valueLength The valueLength to set.
   */
  public void setValueLength(int valueLength) {
    this.valueLength = valueLength;
  }

  /**
   * @return Returns the valuePrecision.
   */
  public int getValuePrecision() {
    return valuePrecision;
  }

  /**
   * @param valuePrecision The valuePrecision to set.
   */
  public void setValuePrecision(int valuePrecision) {
    this.valuePrecision = valuePrecision;
  }

  /**
   * @return Returns the valueType.
   */
  public int getValueType() {
    return valueType;
  }

  /**
   * @param valueType The valueType to set.
   */
  public void setValueType(int valueType) {
    this.valueType = valueType;
  }

  /**
   * @return the formula
   */
  public String getFormula() {
    return formula;
  }

  /**
   * @param formula the formula to set
   */
  public void setFormula(String formula) {
    this.formula = formula;
  }

  /**
   * @return the replaceField
   */
  public String getReplaceField() {
    return replaceField;
  }

  /**
   * @param replaceField the replaceField to set
   */
  public void setReplaceField(String replaceField) {
    this.replaceField = replaceField;
  }

  /**
   * @return the needDataConversion
   */
  public boolean isNeedDataConversion() {
    return needDataConversion;
  }

  /**
   * @param needDataConversion the needDataConversion to set
   */
  public void setNeedDataConversion(boolean needDataConversion) {
    this.needDataConversion = needDataConversion;
  }
}
