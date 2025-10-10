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

package org.apache.hop.pipeline.transforms.calculator;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class CalculatorMetaFunction implements Cloneable {

  @HopMetadataProperty(
      key = "field_name",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.FieldName")
  private String fieldName;

  @HopMetadataProperty(
      key = "calc_type",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.CalculationType",
      storeWithCode = true)
  private CalculationType calcType;

  @HopMetadataProperty(
      key = "field_a",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.FieldA")
  private String fieldA;

  @HopMetadataProperty(
      key = "field_b",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.FieldB")
  private String fieldB;

  @HopMetadataProperty(
      key = "field_c",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.FieldC")
  private String fieldC;

  @HopMetadataProperty(
      key = "value_type",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.ValueType")
  private String valueType;

  @HopMetadataProperty(
      key = "value_length",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.ValueLength")
  private int valueLength;

  @HopMetadataProperty(
      key = "value_precision",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.ValuePrecision")
  private int valuePrecision;

  @HopMetadataProperty(
      key = "conversion_mask",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.ValueFormat")
  private String conversionMask;

  @HopMetadataProperty(
      key = "decimal_symbol",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.ValueDecimal")
  private String decimalSymbol;

  @HopMetadataProperty(
      key = "grouping_symbol",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.ValueGroup")
  private String groupingSymbol;

  @HopMetadataProperty(
      key = "currency_symbol",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.ValueCurrency")
  private String currencySymbol;

  @HopMetadataProperty(
      key = "remove",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.Remove")
  private boolean removedFromResult;

  public CalculatorMetaFunction() {
    this.calcType = CalculationType.NONE;
  }

  /**
   * @param fieldName out field name
   * @param calcType calculation type, see CALC_* set of constants defined
   * @param fieldA name of field "A"
   * @param fieldB name of field "B"
   * @param fieldC name of field "C"
   * @param valueType out value type
   * @param valueLength out value length
   * @param valuePrecision out value precision
   * @param conversionMask out value conversion mask
   * @param decimalSymbol out value decimal symbol
   * @param groupingSymbol out value grouping symbol
   * @param currencySymbol out value currency symbol
   * @param removedFromResult If this result needs to be removed from the output
   */
  public CalculatorMetaFunction(
      String fieldName,
      CalculationType calcType,
      String fieldA,
      String fieldB,
      String fieldC,
      String valueType,
      int valueLength,
      int valuePrecision,
      String conversionMask,
      String decimalSymbol,
      String groupingSymbol,
      String currencySymbol,
      boolean removedFromResult) {
    this.fieldName = fieldName;
    this.calcType = calcType;
    this.fieldA = fieldA;
    this.fieldB = fieldB;
    this.fieldC = fieldC;
    this.valueType = valueType;
    this.valueLength = valueLength;
    this.valuePrecision = valuePrecision;
    this.conversionMask = conversionMask;
    this.decimalSymbol = decimalSymbol;
    this.groupingSymbol = groupingSymbol;
    this.currencySymbol = currencySymbol;
    this.removedFromResult = removedFromResult;
  }

  public CalculatorMetaFunction(CalculatorMetaFunction f) {
    this.fieldName = f.fieldName;
    this.calcType = f.calcType;
    this.fieldA = f.fieldA;
    this.fieldB = f.fieldB;
    this.fieldC = f.fieldC;
    this.valueType = f.valueType;
    this.valueLength = f.valueLength;
    this.valuePrecision = f.valuePrecision;
    this.conversionMask = f.conversionMask;
    this.decimalSymbol = f.decimalSymbol;
    this.groupingSymbol = f.groupingSymbol;
    this.currencySymbol = f.currencySymbol;
    this.removedFromResult = f.removedFromResult;
  }

  @Override
  public CalculatorMetaFunction clone() {
    return new CalculatorMetaFunction(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CalculatorMetaFunction that = (CalculatorMetaFunction) o;
    return valueLength == that.valueLength
        && valuePrecision == that.valuePrecision
        && removedFromResult == that.removedFromResult
        && Objects.equals(fieldName, that.fieldName)
        && calcType == that.calcType
        && Objects.equals(fieldA, that.fieldA)
        && Objects.equals(fieldB, that.fieldB)
        && Objects.equals(fieldC, that.fieldC)
        && Objects.equals(valueType, that.valueType)
        && Objects.equals(conversionMask, that.conversionMask)
        && Objects.equals(decimalSymbol, that.decimalSymbol)
        && Objects.equals(groupingSymbol, that.groupingSymbol)
        && Objects.equals(currencySymbol, that.currencySymbol);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fieldName,
        calcType,
        fieldA,
        fieldB,
        fieldC,
        valueType,
        valueLength,
        valuePrecision,
        conversionMask,
        decimalSymbol,
        groupingSymbol,
        currencySymbol,
        removedFromResult);
  }
}
