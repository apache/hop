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

package org.apache.hop.pipeline.transforms.textfileoutput;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Describes a single field in a text file */
@Getter
@Setter
public class TextFileField implements Cloneable {
  @HopMetadataProperty(
      key = "name",
      injectionKey = "OUTPUT_FIELDNAME",
      injectionKeyDescription = "TextFileOutput.Injection.OUTPUT_FIELDNAME")
  private String name;

  @HopMetadataProperty(
      key = "type",
      intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class,
      injectionKey = "OUTPUT_TYPE",
      injectionKeyDescription = "TextFileOutput.Injection.OUTPUT_TYPE")
  private int type;

  @HopMetadataProperty(
      key = "format",
      injectionKey = "OUTPUT_FORMAT",
      injectionKeyDescription = "TextFileOutput.Injection.OUTPUT_FORMAT")
  private String format;

  @HopMetadataProperty(
      key = "length",
      injectionKey = "OUTPUT_LENGTH",
      injectionKeyDescription = "TextFileOutput.Injection.OUTPUT_LENGTH")
  private int length = -1;

  @HopMetadataProperty(
      key = "precision",
      injectionKey = "OUTPUT_PRECISION",
      injectionKeyDescription = "TextFileOutput.Injection.OUTPUT_PRECISION")
  private int precision = -1;

  @HopMetadataProperty(
      key = "currency",
      injectionKey = "OUTPUT_CURRENCY",
      injectionKeyDescription = "TextFileOutput.Injection.OUTPUT_CURRENCY")
  private String currencySymbol;

  @HopMetadataProperty(
      key = "decimal",
      injectionKey = "OUTPUT_DECIMAL",
      injectionKeyDescription = "TextFileOutput.Injection.OUTPUT_DECIMAL")
  private String decimalSymbol;

  @HopMetadataProperty(
      key = "group",
      injectionKey = "OUTPUT_GROUP",
      injectionKeyDescription = "TextFileOutput.Injection.OUTPUT_GROUP")
  private String groupingSymbol;

  @HopMetadataProperty(
      key = "nullif",
      injectionKey = "OUTPUT_NULL",
      injectionKeyDescription = "TextFileOutput.Injection.OUTPUT_NULL")
  private String nullString;

  @HopMetadataProperty(
      key = "trim_type",
      intCodeConverter = ValueMetaBase.TrimTypeCodeConverter.class,
      injectionKey = "OUTPUT_TRIM",
      injectionKeyDescription = "TextFileOutput.Injection.OUTPUT_TRIM")
  private int trimType;

  @HopMetadataProperty(
      key = "roundingType",
      injectionKey = "OUTPUT_ROUNDING",
      injectionKeyDescription = "TextFileOutput.Injection.OUTPUT_ROUNDING")
  private String roundingType;

  public TextFileField() {
    roundingType = "half_even";
  }

  public TextFileField(
      String name,
      int type,
      String format,
      int length,
      int precision,
      String currencySymbol,
      String decimalSymbol,
      String groupSymbol,
      String nullString,
      String roundingType) {
    this.name = name;
    this.type = type;
    this.format = format;
    this.length = length;
    this.precision = precision;
    this.currencySymbol = currencySymbol;
    this.decimalSymbol = decimalSymbol;
    this.groupingSymbol = groupSymbol;
    this.nullString = nullString;
    this.roundingType = roundingType;
  }

  public TextFileField(TextFileField f) {
    this();
    this.currencySymbol = f.currencySymbol;
    this.decimalSymbol = f.decimalSymbol;
    this.format = f.format;
    this.groupingSymbol = f.groupingSymbol;
    this.length = f.length;
    this.name = f.name;
    this.nullString = f.nullString;
    this.precision = f.precision;
    this.roundingType = f.roundingType;
    this.trimType = f.trimType;
    this.type = f.type;
  }

  public int compare(Object obj) {
    TextFileField field = (TextFileField) obj;

    return name.compareTo(field.getName());
  }

  public boolean equal(Object obj) {
    TextFileField field = (TextFileField) obj;

    return name.equals(field.getName());
  }

  @Override
  public Object clone() {
    return new TextFileField(this);
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName(type);
  }

  public void setType(int type) {
    this.type = type;
  }

  public void setType(String typeDesc) {
    this.type = ValueMetaFactory.getIdForValueMeta(typeDesc);
  }

  @Override
  public String toString() {
    return name + ":" + getTypeDesc();
  }

  public void setTrimTypeByDesc(String value) {
    this.trimType = ValueMetaBase.getTrimTypeByDesc(value);
  }

  public String getTrimTypeCode() {
    return ValueMetaBase.getTrimTypeCode(trimType);
  }

  public String getTrimTypeDesc() {
    return ValueMetaBase.getTrimTypeDesc(trimType);
  }
}
