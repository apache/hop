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

import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;

/** Contains the properties of the target field, conversion mask, type, aggregation method, etc. */
public class DenormaliserTargetField implements Cloneable {
  private static final Class<?> PKG = DenormaliserMeta.class; // For Translator

  @HopMetadataProperty(
      key = "field_name",
      injectionKey = "NAME",
      injectionKeyDescription = "DenormaliserDialog.ColumnInfo.ValueFieldname")
  private String fieldName;

  @HopMetadataProperty(
      key = "key_value",
      injectionKey = "KEY_VALUE",
      injectionKeyDescription = "DenormaliserDialog.ColumnInfo.Keyvalue")
  private String keyValue;

  @HopMetadataProperty(
      key = "target_name",
      injectionKey = "TARGET_NAME",
      injectionKeyDescription = "DenormaliserDialog.ColumnInfo.TargetFieldname")
  private String targetName;

  @HopMetadataProperty(
      key = "target_type",
      injectionKey = "TARGET_TYPE",
      injectionKeyDescription = "DenormaliserDialog.ColumnInfo.Type")
  private String targetType;

  @HopMetadataProperty(
      key = "target_length",
      injectionKey = "TARGET_LENGTH",
      injectionKeyDescription = "DenormaliserDialog.ColumnInfo.Length")
  private int targetLength;

  @HopMetadataProperty(
      key = "target_precision",
      injectionKey = "TARGET_PRECISION",
      injectionKeyDescription = "DenormaliserDialog.ColumnInfo.Precision")
  private int targetPrecision;

  @HopMetadataProperty(
      key = "target_currency_symbol",
      injectionKey = "TARGET_CURRENCY",
      injectionKeyDescription = "DenormaliserDialog.ColumnInfo.Currency")
  private String targetCurrencySymbol;

  @HopMetadataProperty(
      key = "target_decimal_symbol",
      injectionKey = "TARGET_DECIMAL",
      injectionKeyDescription = "DenormaliserDialog.ColumnInfo.Decimal")
  private String targetDecimalSymbol;

  @HopMetadataProperty(
      key = "target_grouping_symbol",
      injectionKey = "TARGET_GROUP",
      injectionKeyDescription = "DenormaliserDialog.ColumnInfo.Group")
  private String targetGroupingSymbol;

  @HopMetadataProperty(
      key = "target_null_string",
      injectionKeyDescription = "DenormaliserDialog.ColumnInfo.NullIf")
  private String targetNullString;

  @HopMetadataProperty(
      key = "target_format",
      injectionKey = "TARGET_FORMAT",
      injectionKeyDescription = "DenormaliserDialog.ColumnInfo.Format")
  private String targetFormat;

  @HopMetadataProperty(
      key = "target_aggregation_type",
      injectionKey = "TARGET_AGGREGATION",
      injectionKeyDescription = "DenormaliserDialog.ColumnInfo.Aggregation",
      storeWithCode = true)
  private DenormaliseAggregation targetAggregationType;

  /** enum for the Aggregation type */
  public enum DenormaliseAggregation implements IEnumHasCode {
    TYPE_AGGR_NONE("-", "-", 0),
    TYPE_AGGR_SUM(
        "SUM", BaseMessages.getString(PKG, "DenormaliserTargetField.TypeAggrLongDesc.Sum"), 1),
    TYPE_AGGR_AVERAGE(
        "AVERAGE",
        BaseMessages.getString(PKG, "DenormaliserTargetField.TypeAggrLongDesc.Average"),
        2),
    TYPE_AGGR_MIN(
        "MIN", BaseMessages.getString(PKG, "DenormaliserTargetField.TypeAggrLongDesc.Min"), 3),
    TYPE_AGGR_MAX(
        "MAX", BaseMessages.getString(PKG, "DenormaliserTargetField.TypeAggrLongDesc.Max"), 4),
    TYPE_AGGR_COUNT_ALL(
        "COUNT_ALL",
        BaseMessages.getString(PKG, "DenormaliserTargetField.TypeAggrLongDesc.CountAll"),
        5),
    TYPE_AGGR_CONCAT_COMMA(
        "CONCAT_COMMA",
        BaseMessages.getString(PKG, "DenormaliserTargetField.TypeAggrLongDesc.ConcatComma"),
        6);

    private final String code;
    private final String description;
    private final int defaultResultType;

    DenormaliseAggregation(String code, String description, int defaultResultType) {
      this.code = code;
      this.description = description;
      this.defaultResultType = defaultResultType;
    }

    /**
     * Get the Descriptions
     *
     * @return The Aggregation type descriptions
     */
    public static String[] getDescriptions() {
      String[] descriptions = new String[values().length];
      for (int i = 0; i < descriptions.length; i++) {
        descriptions[i] = values()[i].getDescription();
      }
      return descriptions;
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    public String getCode() {
      return code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    public String getDescription() {
      return description;
    }

    /**
     * Gets defaultResultType
     *
     * @return value of defaultResultType
     */
    public int getDefaultResultType() {
      return defaultResultType;
    }

    public static DenormaliseAggregation getTypeWithDescription(String description) {
      for (DenormaliseAggregation value : values()) {
        if (value.getDescription().equals(description)) {
          return value;
        }
      }
      return TYPE_AGGR_NONE;
    }
  }

  /** Create an empty pivot target field */
  public DenormaliserTargetField() {
    this.targetAggregationType = DenormaliseAggregation.TYPE_AGGR_NONE;
  }

  public DenormaliserTargetField(DenormaliserTargetField t) {
    this.fieldName = t.fieldName;
    this.keyValue = t.keyValue;
    this.targetName = t.targetName;
    this.targetType = t.targetType;
    this.targetLength = t.targetLength;
    this.targetPrecision = t.targetPrecision;
    this.targetCurrencySymbol = t.targetCurrencySymbol;
    this.targetDecimalSymbol = t.targetDecimalSymbol;
    this.targetGroupingSymbol = t.targetGroupingSymbol;
    this.targetNullString = t.targetNullString;
    this.targetFormat = t.targetFormat;
    this.targetAggregationType = t.targetAggregationType;
  }

  public DenormaliserTargetField(
      String fieldName,
      String keyValue,
      String targetName,
      String targetType,
      int targetLength,
      int targetPrecision,
      String targetCurrencySymbol,
      String targetDecimalSymbol,
      String targetGroupingSymbol,
      String targetNullString,
      String targetFormat,
      DenormaliseAggregation targetAggregationType) {
    this.fieldName = fieldName;
    this.keyValue = keyValue;
    this.targetName = targetName;
    this.targetType = targetType;
    this.targetLength = targetLength;
    this.targetPrecision = targetPrecision;
    this.targetCurrencySymbol = targetCurrencySymbol;
    this.targetDecimalSymbol = targetDecimalSymbol;
    this.targetGroupingSymbol = targetGroupingSymbol;
    this.targetNullString = targetNullString;
    this.targetFormat = targetFormat;
    this.targetAggregationType = targetAggregationType;
  }

  public DenormaliserTargetField clone() {
    return new DenormaliserTargetField(this);
  }

  /** @return Returns the fieldName. */
  public String getFieldName() {
    return fieldName;
  }

  /** @param fieldName The fieldName to set. */
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  /** @return Returns the targetFormat. */
  public String getTargetFormat() {
    return targetFormat;
  }

  /** @param targetFormat The targetFormat to set. */
  public void setTargetFormat(String targetFormat) {
    this.targetFormat = targetFormat;
  }

  /** @return Returns the keyValue. */
  public String getKeyValue() {
    return keyValue;
  }

  /** @param keyValue The keyValue to set. */
  public void setKeyValue(String keyValue) {
    this.keyValue = keyValue;
  }

  /** @return Returns the targetCurrencySymbol. */
  public String getTargetCurrencySymbol() {
    return targetCurrencySymbol;
  }

  /** @param targetCurrencySymbol The targetCurrencySymbol to set. */
  public void setTargetCurrencySymbol(String targetCurrencySymbol) {
    this.targetCurrencySymbol = targetCurrencySymbol;
  }

  /** @return Returns the targetDecimalSymbol. */
  public String getTargetDecimalSymbol() {
    return targetDecimalSymbol;
  }

  /** @param targetDecimalSymbol The targetDecimalSymbol to set. */
  public void setTargetDecimalSymbol(String targetDecimalSymbol) {
    this.targetDecimalSymbol = targetDecimalSymbol;
  }

  /** @return Returns the targetGroupingSymbol. */
  public String getTargetGroupingSymbol() {
    return targetGroupingSymbol;
  }

  /** @param targetGroupingSymbol The targetGroupingSymbol to set. */
  public void setTargetGroupingSymbol(String targetGroupingSymbol) {
    this.targetGroupingSymbol = targetGroupingSymbol;
  }

  /** @return Returns the targetLength. */
  public int getTargetLength() {
    return targetLength;
  }

  /** @param targetLength The targetLength to set. */
  public void setTargetLength(int targetLength) {
    this.targetLength = targetLength;
  }

  /** @return Returns the targetName. */
  public String getTargetName() {
    return targetName;
  }

  /** @param targetName The targetName to set. */
  public void setTargetName(String targetName) {
    this.targetName = targetName;
  }

  /** @return Returns the targetNullString. */
  public String getTargetNullString() {
    return targetNullString;
  }

  /** @param targetNullString The targetNullString to set. */
  public void setTargetNullString(String targetNullString) {
    this.targetNullString = targetNullString;
  }

  /** @return Returns the targetPrecision. */
  public int getTargetPrecision() {
    return targetPrecision;
  }

  /** @param targetPrecision The targetPrecision to set. */
  public void setTargetPrecision(int targetPrecision) {
    this.targetPrecision = targetPrecision;
  }

  /** @return Returns the targetType. */
  public String getTargetType() {
    return targetType;
  }

  /** @param targetType The targetType to set. */
  public void setTargetType(String targetType) {
    this.targetType = targetType;
  }

  /**
   * Set the target type
   *
   * @param typeDesc the target value type description
   */
  public void setTargetType(int typeDesc) {
    targetType = ValueMetaFactory.getValueMetaName(typeDesc);
  }

  /**
   * @return The target aggregation type: when a key-value collision occurs, what it the aggregation
   *     to use.
   */
  public DenormaliseAggregation getTargetAggregationType() {
    return targetAggregationType;
  }

  /**
   * @param targetAggregationType Specify the The aggregation type: when a key-value collision
   *     occurs, what it the aggregation to use.
   */
  public void setTargetAggregationType(DenormaliseAggregation targetAggregationType) {
    this.targetAggregationType = targetAggregationType;
  }
}
