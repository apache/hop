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
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;

/** Describes a single field in a text file */
@Getter
@Setter
public class TextFileField implements Cloneable {
  @Injection(name = "OUTPUT_FIELDNAME", group = "OUTPUT_FIELDS")
  private String name;

  private int type;

  @Injection(name = "OUTPUT_FORMAT", group = "OUTPUT_FIELDS")
  private String format;

  @Injection(name = "OUTPUT_LENGTH", group = "OUTPUT_FIELDS")
  private int length = -1;

  @Injection(name = "OUTPUT_PRECISION", group = "OUTPUT_FIELDS")
  private int precision = -1;

  @Injection(name = "OUTPUT_CURRENCY", group = "OUTPUT_FIELDS")
  private String currencySymbol;

  @Injection(name = "OUTPUT_DECIMAL", group = "OUTPUT_FIELDS")
  private String decimalSymbol;

  @Injection(name = "OUTPUT_GROUP", group = "OUTPUT_FIELDS")
  private String groupingSymbol;

  @Injection(name = "OUTPUT_NULL", group = "OUTPUT_FIELDS")
  private String nullString;

  private int trimType;

  @Getter @Setter private String roundingType;

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

  public TextFileField() {}

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
    try {
      Object retval = super.clone();
      return retval;
    } catch (CloneNotSupportedException e) {
      return null;
    }
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName(type);
  }

  public void setType(int type) {
    this.type = type;
  }

  @Injection(name = "OUTPUT_TYPE", group = "OUTPUT_FIELDS")
  public void setType(String typeDesc) {
    this.type = ValueMetaFactory.getIdForValueMeta(typeDesc);
  }

  @Override
  public String toString() {
    return name + ":" + getTypeDesc();
  }

  @Injection(name = "OUTPUT_TRIM", group = "OUTPUT_FIELDS")
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
