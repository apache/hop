/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hop.pipeline.transforms.constant;


import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class ConstantField {

    @HopMetadataProperty(injectionKeyDescription = "ConstantMeta.Injection.Currency.Field")
    private String currency;

    @HopMetadataProperty(injectionKeyDescription = "ConstantMeta.Injection.Decimal.Field")
    private String decimal;

    @HopMetadataProperty(injectionKeyDescription = "ConstantMeta.Injection.Group.Field")
    private String group;

    @HopMetadataProperty(key = "nullif",
            injectionKeyDescription = "ConstantMeta.Injection.Value.Field")
    private String value; // Null-if

    @HopMetadataProperty(key = "name",
            injectionKeyDescription = "ConstantMeta.Injection.FieldName.Field")
    private String fieldName;

    @HopMetadataProperty(key = "type",
            injectionKeyDescription = "ConstantMeta.Injection.FieldType.Field")
    private String fieldType;

    @HopMetadataProperty(key = "format",
            injectionKeyDescription = "ConstantMeta.Injection.FieldFormat.Field")
    private String fieldFormat;

    @HopMetadataProperty(key = "length",
            injectionKeyDescription = "ConstantMeta.Injection.FieldLength.Field")
    private int fieldLength;

    @HopMetadataProperty(key = "precision",
            injectionKeyDescription = "ConstantMeta.Injection.FieldPrecision.Field")
    private int fieldPrecision;
    /** Flag : set empty string */

    @HopMetadataProperty(key = "set_empty_string",
            injectionKeyDescription = "ConstantMeta.Injection.SetEmptyString.Field")
    private boolean emptyString;

    public ConstantField() {
    }

    public ConstantField(String fieldName, String fieldType, String value) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.value = value;
        this.emptyString = false;
    }

    public ConstantField(String fieldName, String fieldType, boolean setEmptyString) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.emptyString = setEmptyString;
        this.value = "";
    }

    /** @return Returns the currency. */
    public String getCurrency() {
        return currency;
    }

    /** @param currency The currency to set. */
    public void setCurrency(String currency) {
        this.currency = currency;
    }

    /** @return Returns the decimal. */
    public String getDecimal() {
        return decimal;
    }

    /** @param decimal The decimal to set. */
    public void setDecimal(String decimal) {
        this.decimal = decimal;
    }

    /** @return Returns the fieldFormat. */
    public String getFieldFormat() {
        return fieldFormat;
    }

    /** @param fieldFormat The fieldFormat to set. */
    public void setFieldFormat(String fieldFormat) {
        this.fieldFormat = fieldFormat;
    }

    /** @return Returns the fieldLength. */
    public int getFieldLength() {
        return fieldLength;
    }

    /** @param fieldLength The fieldLength to set. */
    public void setFieldLength(int fieldLength) {
        this.fieldLength = fieldLength;
    }

    /** @return Returns the fieldName. */
    public String getFieldName() {
        return fieldName;
    }

    /** @param fieldName The fieldName to set. */
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    /** @return Returns the fieldPrecision. */
    public int getFieldPrecision() {
        return fieldPrecision;
    }

    /** @param fieldPrecision The fieldPrecision to set. */
    public void setFieldPrecision(int fieldPrecision) {
        this.fieldPrecision = fieldPrecision;
    }

    /** @return Returns the fieldType. */
    public String getFieldType() {
        return fieldType;
    }

    /** @param fieldType The fieldType to set. */
    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    /**
     * @return the setEmptyString
     */
    public boolean isEmptyString() {
        return emptyString;
    }

    /** @param setEmptyString the setEmptyString to set */
    public void setEmptyString(boolean setEmptyString) {
        this.emptyString = setEmptyString;
    }

    /** @return Returns the group. */
    public String getGroup() {
        return group;
    }

    /** @param group The group to set. */
    public void setGroup(String group) {
        this.group = group;
    }

    /** @return Returns the value. */
    public String getValue() {
        return value;
    }

    /** @param value The value to set. */
    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConstantField that = (ConstantField) o;
        return fieldLength == that.fieldLength && fieldPrecision == that.fieldPrecision && emptyString == that.emptyString && Objects.equals(currency, that.currency) && Objects.equals(decimal, that.decimal) && Objects.equals(group, that.group) && Objects.equals(value, that.value) && fieldName.equals(that.fieldName) && fieldType.equals(that.fieldType) && Objects.equals(fieldFormat, that.fieldFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currency, decimal, group, value, fieldName, fieldType, fieldFormat, fieldLength, fieldPrecision, emptyString);
    }
}
