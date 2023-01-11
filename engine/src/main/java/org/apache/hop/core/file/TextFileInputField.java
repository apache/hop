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

package org.apache.hop.core.file;

import org.apache.hop.core.Const;
import org.apache.hop.core.gui.ITextFileInputField;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Describes a single field in a text file */
public class TextFileInputField implements Cloneable, ITextFileInputField {

  @HopMetadataProperty(
      key = "name",
      injectionKey = "INPUT_NAME",
      injectionKeyDescription = "CsvInputMeta.Injection.INPUT_NAME")
  private String name;

  @HopMetadataProperty(
      key = "length",
      injectionKey = "INPUT_LENGTH",
      injectionKeyDescription = "CsvInputMeta.Injection.INPUT_LENGTH")
  private int length;

  @HopMetadataProperty(
      key = "type",
      injectionKey = "FIELD_TYPE",
      injectionKeyDescription = "CsvInputMeta.Injection.FIELD_TYPE")
  private int type;

  @HopMetadataProperty(
      key = "format",
      injectionKey = "INPUT_FORMAT",
      injectionKeyDescription = "CsvInputMeta.Injection.INPUT_FORMAT")
  private String format;

  @HopMetadataProperty(
      key = "trim_type",
      injectionKey = "FIELD_TRIM_TYPE",
      injectionKeyDescription = "CsvInputMeta.Injection.FIELD_TRIM_TYPE")
  private int trimtype;

  @HopMetadataProperty(
      key = "precision",
      injectionKey = "INPUT_PRECISION",
      injectionKeyDescription = "CsvInputMeta.Injection.INPUT_PRECISION")
  private int precision;

  @HopMetadataProperty(
      key = "currency",
      injectionKey = "INPUT_CURRENCY",
      injectionKeyDescription = "CsvInputMeta.Injection.INPUT_CURRENCY")
  private String currencySymbol;

  @HopMetadataProperty(
      key = "decimal",
      injectionKey = "INPUT_DECIMAL",
      injectionKeyDescription = "CsvInputMeta.Injection.INPUT_DECIMAL")
  private String decimalSymbol;

  @HopMetadataProperty(
      key = "group",
      injectionKey = "INPUT_GROUP",
      injectionKeyDescription = "CsvInputMeta.Injection.INPUT_GROUP")
  private String groupSymbol;

  private boolean ignore;

  private int position;

  private boolean repeat;

  private String nullString;

  private String ifNullValue;

  private String[] samples;

  private static final String[] dateFormats =
      new String[] {
        "yyyy/MM/dd HH:mm:ss.SSS", "yyyy/MM/dd HH:mm:ss", "dd/MM/yyyy", "dd-MM-yyyy", "yyyy/MM/dd",
            "yyyy-MM-dd",
        "yyyyMMdd", "ddMMyyyy", "d-M-yyyy", "d/M/yyyy", "d-M-yy", "d/M/yy",
      };

  private static final String[] numberFormats =
      new String[] {
        "",
        "#",
        Const.DEFAULT_NUMBER_FORMAT,
        "0.00",
        "0000000000000",
        "###,###,###.#######",
        "###############.###############",
        "#####.###############%",
      };

  public TextFileInputField(String fieldname, int position, int length) {
    this.name = fieldname;
    this.position = position;
    this.length = length;
    this.type = IValueMeta.TYPE_STRING;
    this.ignore = false;
    this.format = "";
    this.trimtype = IValueMeta.TRIM_TYPE_NONE;
    this.groupSymbol = "";
    this.decimalSymbol = "";
    this.currencySymbol = "";
    this.precision = -1;
    this.repeat = false;
    this.nullString = "";
    this.ifNullValue = "";
  }

  public TextFileInputField() {
    this(null, -1, -1);
  }

  public int compare(Object obj) {
    TextFileInputField field = (TextFileInputField) obj;

    return position - field.getPosition();
  }

  @Override
  public int compareTo(ITextFileInputField field) {
    return position - field.getPosition();
  }

  public boolean equal(Object obj) {
    TextFileInputField field = (TextFileInputField) obj;

    return (position == field.getPosition());
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

  @Override
  public int getPosition() {
    return position;
  }

  public void setPosition(int position) {
    this.position = position;
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public void setLength(int length) {
    this.length = length;
  }

  @Override
  public String getName() {
    return name;
  }

  public void setName(String fieldname) {
    this.name = fieldname;
  }

  public int getType() {
    return type;
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName(type);
  }

  public void setType(int type) {
    this.type = type;
  }

  public void setType(String value) {
    this.type = ValueMetaFactory.getIdForValueMeta(value);
  }

  public boolean isIgnored() {
    return ignore;
  }

  public void setIgnored(boolean ignore) {
    this.ignore = ignore;
  }

  public void flipIgnored() {
    ignore = !ignore;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public void setSamples(String[] samples) {
    this.samples = samples;
  }

  public String[] getSamples() {
    return this.samples;
  }

  public int getTrimType() {
    return trimtype;
  }

  public void setTrimType(int trimtype) {
    this.trimtype = trimtype;
  }

  public void setTrimType(String value) {
    this.trimtype = ValueMetaString.getTrimTypeByCode(value);
  }

  public String getGroupSymbol() {
    return groupSymbol;
  }

  public void setGroupSymbol(String groupSymbol) {
    this.groupSymbol = groupSymbol;
  }

  public String getDecimalSymbol() {
    return decimalSymbol;
  }

  public void setDecimalSymbol(String decimalSymbol) {
    this.decimalSymbol = decimalSymbol;
  }

  public String getCurrencySymbol() {
    return currencySymbol;
  }

  public void setCurrencySymbol(String currencySymbol) {
    this.currencySymbol = currencySymbol;
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(int precision) {
    this.precision = precision;
  }

  public boolean isRepeated() {
    return repeat;
  }

  public void setRepeated(boolean repeat) {
    this.repeat = repeat;
  }

  public void flipRepeated() {
    repeat = !repeat;
  }

  public String getNullString() {
    return nullString;
  }

  public void setNullString(String nullString) {
    this.nullString = nullString;
  }

  public String getIfNullValue() {
    return ifNullValue;
  }

  public void setIfNullValue(String ifNullValue) {
    this.ifNullValue = ifNullValue;
  }

  @Override
  public String toString() {
    return name + "@" + position + ":" + length;
  }

  @Override
  public ITextFileInputField createNewInstance(String newFieldname, int x, int newlength) {
    return new TextFileInputField(newFieldname, x, newlength);
  }
}
