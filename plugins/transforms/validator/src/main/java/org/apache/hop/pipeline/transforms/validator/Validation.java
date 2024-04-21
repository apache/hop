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
 *
 */

package org.apache.hop.pipeline.transforms.validator;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transform.TransformMeta;

public class Validation implements Cloneable {
  public static final String XML_TAG = "validator_field";
  public static final String XML_TAG_ALLOWED = "allowed_value";

  @HopMetadataProperty(
      key = "validation_name",
      injectionKey = "NAME",
      injectionKeyDescription = "Validator.Injection.NAME")
  private String name;

  @HopMetadataProperty(
      key = "name",
      injectionKey = "FIELD_NAME",
      injectionKeyDescription = "Validator.Injection.FIELD_NAME")
  private String fieldName;

  @HopMetadataProperty(
      key = "max_length",
      injectionKey = "MAX_LENGTH",
      injectionKeyDescription = "Validator.Injection.MAX_LENGTH")
  private String maximumLength;

  @HopMetadataProperty(
      key = "min_length",
      injectionKey = "MIN_LENGTH",
      injectionKeyDescription = "Validator.Injection.MIN_LENGTH")
  private String minimumLength;

  @HopMetadataProperty(
      key = "null_allowed",
      injectionKey = "NULL_ALLOWED",
      injectionKeyDescription = "Validator.Injection.NULL_ALLOWED")
  private boolean nullAllowed;

  @HopMetadataProperty(
      key = "only_null_allowed",
      injectionKey = "ONLY_NULL_ALLOWED",
      injectionKeyDescription = "Validator.Injection.ONLY_NULL_ALLOWED")
  private boolean onlyNullAllowed;

  @HopMetadataProperty(
      key = "only_numeric_allowed",
      injectionKey = "ONLY_NUMERIC_ALLOWED",
      injectionKeyDescription = "Validator.Injection.ONLY_NUMERIC_ALLOWED")
  private boolean onlyNumericAllowed;

  @HopMetadataProperty(
      key = "data_type",
      injectionKey = "DATA_TYPE",
      injectionKeyDescription = "Validator.Injection.DATA_TYPE")
  private String dataType;

  @HopMetadataProperty(
      key = "data_type_verified",
      injectionKey = "DATA_TYPE_VERIFIED",
      injectionKeyDescription = "Validator.Injection.DATA_TYPE_VERIFIED")
  private boolean dataTypeVerified;

  @HopMetadataProperty(
      key = "conversion_mask",
      injectionKey = "CONVERSION_MASK",
      injectionKeyDescription = "Validator.Injection.CONVERSION_MASK")
  private String conversionMask;

  @HopMetadataProperty(
      key = "decimal_symbol",
      injectionKey = "DECIMAL_SYMBOL",
      injectionKeyDescription = "Validator.Injection.DECIMAL_SYMBOL")
  private String decimalSymbol;

  @HopMetadataProperty(
      key = "grouping_symbol",
      injectionKey = "GROUPING_SYMBOL",
      injectionKeyDescription = "Validator.Injection.GROUPING_SYMBOL")
  private String groupingSymbol;

  @HopMetadataProperty(
      key = "is_sourcing_values",
      injectionKey = "SOURCING_VALUES",
      injectionKeyDescription = "Validator.Injection.SOURCING_VALUES")
  private boolean sourcingValues;

  @HopMetadataProperty(
      key = "sourcing_transform",
      injectionKey = "SOURCING_TRANSFORM_NAME",
      injectionKeyDescription = "Validator.Injection.SOURCING_TRANSFORM_NAME")
  private String sourcingTransformName;

  @HopMetadataProperty(
      key = "sourcing_field",
      injectionKey = "SOURCING_FIELD",
      injectionKeyDescription = "Validator.Injection.SOURCING_FIELD")
  private String sourcingField;

  @HopMetadataProperty(
      key = "min_value",
      injectionKey = "MIN_VALUE",
      injectionKeyDescription = "Validator.Injection.MIN_VALUE")
  private String minimumValue;

  @HopMetadataProperty(
      key = "max_value",
      injectionKey = "MAX_VALUE",
      injectionKeyDescription = "Validator.Injection.MAX_VALUE")
  private String maximumValue;

  @HopMetadataProperty(
      key = "start_string",
      injectionKey = "START_STRING",
      injectionKeyDescription = "Validator.Injection.START_STRING")
  private String startString;

  @HopMetadataProperty(
      key = "start_string_not_allowed",
      injectionKey = "START_STRING_NOT_ALLOWED",
      injectionKeyDescription = "Validator.Injection.START_STRING_NOT_ALLOWED")
  private String startStringNotAllowed;

  @HopMetadataProperty(
      key = "end_string",
      injectionKey = "END_STRING",
      injectionKeyDescription = "Validator.Injection.END_STRING")
  private String endString;

  @HopMetadataProperty(
      key = "end_string_not_allowed",
      injectionKey = "END_STRING_NOT_ALLOWED",
      injectionKeyDescription = "Validator.Injection.END_STRING_NOT_ALLOWED")
  private String endStringNotAllowed;

  @HopMetadataProperty(
      key = "regular_expression",
      injectionKey = "REGULAR_EXPRESSION_EXPECTED",
      injectionKeyDescription = "Validator.Injection.REGULAR_EXPRESSION_EXPECTED")
  private String regularExpression;

  @HopMetadataProperty(
      key = "regular_expression_not_allowed",
      injectionKey = "REGULAR_EXPRESSION_NOT_ALLOWED",
      injectionKeyDescription = "Validator.Injection.REGULAR_EXPRESSION_NOT_ALLOWED")
  private String regularExpressionNotAllowed;

  @HopMetadataProperty(
      key = "error_code",
      injectionKey = "ERROR_CODE",
      injectionKeyDescription = "Validator.Injection.ERROR_CODE")
  private String errorCode;

  @HopMetadataProperty(
      key = "error_description",
      injectionKey = "ERROR_CODE_DESCRIPTION",
      injectionKeyDescription = "Validator.Injection.ERROR_CODE_DESCRIPTION")
  private String errorDescription;

  @HopMetadataProperty(groupKey = "allowed_value", key = "value")
  private List<String> allowedValues;

  // Non-serialized
  private TransformMeta sourcingTransform;

  public Validation() {
    maximumLength = "";
    minimumLength = "";
    nullAllowed = true;
    onlyNullAllowed = false;
    onlyNumericAllowed = false;
    allowedValues = new ArrayList<>();
  }

  public Validation(Validation v) {
    this.name = v.name;
    this.fieldName = v.fieldName;
    this.maximumLength = v.maximumLength;
    this.minimumLength = v.minimumLength;
    this.nullAllowed = v.nullAllowed;
    this.onlyNullAllowed = v.onlyNullAllowed;
    this.onlyNumericAllowed = v.onlyNumericAllowed;
    this.dataType = v.dataType;
    this.dataTypeVerified = v.dataTypeVerified;
    this.conversionMask = v.conversionMask;
    this.decimalSymbol = v.decimalSymbol;
    this.groupingSymbol = v.groupingSymbol;
    this.sourcingValues = v.sourcingValues;
    this.sourcingTransformName = v.sourcingTransformName;
    this.sourcingField = v.sourcingField;
    this.minimumValue = v.minimumValue;
    this.maximumValue = v.maximumValue;
    this.startString = v.startString;
    this.startStringNotAllowed = v.startStringNotAllowed;
    this.endString = v.endString;
    this.endStringNotAllowed = v.endStringNotAllowed;
    this.regularExpression = v.regularExpression;
    this.regularExpressionNotAllowed = v.regularExpressionNotAllowed;
    this.errorCode = v.errorCode;
    this.errorDescription = v.errorDescription;
    this.allowedValues = new ArrayList<>(v.allowedValues);
    this.sourcingTransform = v.sourcingTransform;
  }

  public Validation(String name) {
    this();
    this.fieldName = name;
  }

  @Override
  public Validation clone() {
    return new Validation(this);
  }

  public boolean equals(Validation validation) {
    return validation.getName().equalsIgnoreCase(name);
  }

  /**
   * Find a validation by name in a list of validations
   *
   * @param validations The list to search
   * @param name the name to search for
   * @return the validation if one matches or null if none is found.
   */
  public static Validation findValidation(List<Validation> validations, String name) {
    for (Validation validation : validations) {
      if (validation.getName().equalsIgnoreCase(name)) {
        return validation;
      }
    }
    return null;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * Sets name
   *
   * @param name value of name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets fieldName
   *
   * @return value of fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * Sets fieldName
   *
   * @param fieldName value of fieldName
   */
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  /**
   * Gets maximumLength
   *
   * @return value of maximumLength
   */
  public String getMaximumLength() {
    return maximumLength;
  }

  /**
   * Sets maximumLength
   *
   * @param maximumLength value of maximumLength
   */
  public void setMaximumLength(String maximumLength) {
    this.maximumLength = maximumLength;
  }

  /**
   * Gets minimumLength
   *
   * @return value of minimumLength
   */
  public String getMinimumLength() {
    return minimumLength;
  }

  /**
   * Sets minimumLength
   *
   * @param minimumLength value of minimumLength
   */
  public void setMinimumLength(String minimumLength) {
    this.minimumLength = minimumLength;
  }

  /**
   * Gets nullAllowed
   *
   * @return value of nullAllowed
   */
  public boolean isNullAllowed() {
    return nullAllowed;
  }

  /**
   * Sets nullAllowed
   *
   * @param nullAllowed value of nullAllowed
   */
  public void setNullAllowed(boolean nullAllowed) {
    this.nullAllowed = nullAllowed;
  }

  /**
   * Gets onlyNullAllowed
   *
   * @return value of onlyNullAllowed
   */
  public boolean isOnlyNullAllowed() {
    return onlyNullAllowed;
  }

  /**
   * Sets onlyNullAllowed
   *
   * @param onlyNullAllowed value of onlyNullAllowed
   */
  public void setOnlyNullAllowed(boolean onlyNullAllowed) {
    this.onlyNullAllowed = onlyNullAllowed;
  }

  /**
   * Gets onlyNumericAllowed
   *
   * @return value of onlyNumericAllowed
   */
  public boolean isOnlyNumericAllowed() {
    return onlyNumericAllowed;
  }

  /**
   * Sets onlyNumericAllowed
   *
   * @param onlyNumericAllowed value of onlyNumericAllowed
   */
  public void setOnlyNumericAllowed(boolean onlyNumericAllowed) {
    this.onlyNumericAllowed = onlyNumericAllowed;
  }

  /**
   * Gets dataType
   *
   * @return value of dataType
   */
  public String getDataType() {
    return dataType;
  }

  /**
   * Sets dataType
   *
   * @param dataType value of dataType
   */
  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  /**
   * Gets dataTypeVerified
   *
   * @return value of dataTypeVerified
   */
  public boolean isDataTypeVerified() {
    return dataTypeVerified;
  }

  /**
   * Sets dataTypeVerified
   *
   * @param dataTypeVerified value of dataTypeVerified
   */
  public void setDataTypeVerified(boolean dataTypeVerified) {
    this.dataTypeVerified = dataTypeVerified;
  }

  /**
   * Gets conversionMask
   *
   * @return value of conversionMask
   */
  public String getConversionMask() {
    return conversionMask;
  }

  /**
   * Sets conversionMask
   *
   * @param conversionMask value of conversionMask
   */
  public void setConversionMask(String conversionMask) {
    this.conversionMask = conversionMask;
  }

  /**
   * Gets decimalSymbol
   *
   * @return value of decimalSymbol
   */
  public String getDecimalSymbol() {
    return decimalSymbol;
  }

  /**
   * Sets decimalSymbol
   *
   * @param decimalSymbol value of decimalSymbol
   */
  public void setDecimalSymbol(String decimalSymbol) {
    this.decimalSymbol = decimalSymbol;
  }

  /**
   * Gets groupingSymbol
   *
   * @return value of groupingSymbol
   */
  public String getGroupingSymbol() {
    return groupingSymbol;
  }

  /**
   * Sets groupingSymbol
   *
   * @param groupingSymbol value of groupingSymbol
   */
  public void setGroupingSymbol(String groupingSymbol) {
    this.groupingSymbol = groupingSymbol;
  }

  /**
   * Gets sourcingValues
   *
   * @return value of sourcingValues
   */
  public boolean isSourcingValues() {
    return sourcingValues;
  }

  /**
   * Sets sourcingValues
   *
   * @param sourcingValues value of sourcingValues
   */
  public void setSourcingValues(boolean sourcingValues) {
    this.sourcingValues = sourcingValues;
  }

  /**
   * Gets sourcingTransformName
   *
   * @return value of sourcingTransformName
   */
  public String getSourcingTransformName() {
    return sourcingTransformName;
  }

  /**
   * Sets sourcingTransformName
   *
   * @param sourcingTransformName value of sourcingTransformName
   */
  public void setSourcingTransformName(String sourcingTransformName) {
    this.sourcingTransformName = sourcingTransformName;
  }

  /**
   * Gets sourcingField
   *
   * @return value of sourcingField
   */
  public String getSourcingField() {
    return sourcingField;
  }

  /**
   * Sets sourcingField
   *
   * @param sourcingField value of sourcingField
   */
  public void setSourcingField(String sourcingField) {
    this.sourcingField = sourcingField;
  }

  /**
   * Gets minimumValue
   *
   * @return value of minimumValue
   */
  public String getMinimumValue() {
    return minimumValue;
  }

  /**
   * Sets minimumValue
   *
   * @param minimumValue value of minimumValue
   */
  public void setMinimumValue(String minimumValue) {
    this.minimumValue = minimumValue;
  }

  /**
   * Gets maximumValue
   *
   * @return value of maximumValue
   */
  public String getMaximumValue() {
    return maximumValue;
  }

  /**
   * Sets maximumValue
   *
   * @param maximumValue value of maximumValue
   */
  public void setMaximumValue(String maximumValue) {
    this.maximumValue = maximumValue;
  }

  /**
   * Gets startString
   *
   * @return value of startString
   */
  public String getStartString() {
    return startString;
  }

  /**
   * Sets startString
   *
   * @param startString value of startString
   */
  public void setStartString(String startString) {
    this.startString = startString;
  }

  /**
   * Gets startStringNotAllowed
   *
   * @return value of startStringNotAllowed
   */
  public String getStartStringNotAllowed() {
    return startStringNotAllowed;
  }

  /**
   * Sets startStringNotAllowed
   *
   * @param startStringNotAllowed value of startStringNotAllowed
   */
  public void setStartStringNotAllowed(String startStringNotAllowed) {
    this.startStringNotAllowed = startStringNotAllowed;
  }

  /**
   * Gets endString
   *
   * @return value of endString
   */
  public String getEndString() {
    return endString;
  }

  /**
   * Sets endString
   *
   * @param endString value of endString
   */
  public void setEndString(String endString) {
    this.endString = endString;
  }

  /**
   * Gets endStringNotAllowed
   *
   * @return value of endStringNotAllowed
   */
  public String getEndStringNotAllowed() {
    return endStringNotAllowed;
  }

  /**
   * Sets endStringNotAllowed
   *
   * @param endStringNotAllowed value of endStringNotAllowed
   */
  public void setEndStringNotAllowed(String endStringNotAllowed) {
    this.endStringNotAllowed = endStringNotAllowed;
  }

  /**
   * Gets regularExpression
   *
   * @return value of regularExpression
   */
  public String getRegularExpression() {
    return regularExpression;
  }

  /**
   * Sets regularExpression
   *
   * @param regularExpression value of regularExpression
   */
  public void setRegularExpression(String regularExpression) {
    this.regularExpression = regularExpression;
  }

  /**
   * Gets regularExpressionNotAllowed
   *
   * @return value of regularExpressionNotAllowed
   */
  public String getRegularExpressionNotAllowed() {
    return regularExpressionNotAllowed;
  }

  /**
   * Sets regularExpressionNotAllowed
   *
   * @param regularExpressionNotAllowed value of regularExpressionNotAllowed
   */
  public void setRegularExpressionNotAllowed(String regularExpressionNotAllowed) {
    this.regularExpressionNotAllowed = regularExpressionNotAllowed;
  }

  /**
   * Gets errorCode
   *
   * @return value of errorCode
   */
  public String getErrorCode() {
    return errorCode;
  }

  /**
   * Sets errorCode
   *
   * @param errorCode value of errorCode
   */
  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  /**
   * Gets errorDescription
   *
   * @return value of errorDescription
   */
  public String getErrorDescription() {
    return errorDescription;
  }

  /**
   * Sets errorDescription
   *
   * @param errorDescription value of errorDescription
   */
  public void setErrorDescription(String errorDescription) {
    this.errorDescription = errorDescription;
  }

  /**
   * Gets allowedValues
   *
   * @return value of allowedValues
   */
  public List<String> getAllowedValues() {
    return allowedValues;
  }

  /**
   * Sets allowedValues
   *
   * @param allowedValues value of allowedValues
   */
  public void setAllowedValues(List<String> allowedValues) {
    this.allowedValues = allowedValues;
  }

  /**
   * Gets sourcingTransform
   *
   * @return value of sourcingTransform
   */
  public TransformMeta getSourcingTransform() {
    return sourcingTransform;
  }

  /**
   * Sets sourcingTransform
   *
   * @param sourcingTransform value of sourcingTransform
   */
  public void setSourcingTransform(TransformMeta sourcingTransform) {
    this.sourcingTransform = sourcingTransform;
  }
}
