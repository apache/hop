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

package org.apache.hop.pipeline.transforms.selectvalues;

import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class SelectMetadataChange {

  // META-DATA mode
  /** Fields of which we want to change the meta data */
  // @Injection(name = "META_NAME", group = "METAS")
  @HopMetadataProperty(key = "name", injectionKey = "META_NAME", injectionGroupKey = "METAS")
  private String name;

  /** Meta: new name of field */
  // @Injection(name = "META_RENAME", group = "METAS")
  @HopMetadataProperty(key = "rename", injectionKey = "META_RENAME", injectionGroupKey = "METAS")
  private String rename;

  /** Meta: new Value type for this field or TYPE_NONE if no change needed! */
  @HopMetadataProperty(key = "type")
  private String type;

  /** Meta: new length of field */
  @HopMetadataProperty(key = "length", injectionKey = "META_LENGTH", injectionGroupKey = "METAS")
  private int length = -1;

  /** Meta: new precision of field (for numbers) */
  @HopMetadataProperty(
      key = "precision",
      injectionKey = "META_PRECISION",
      injectionGroupKey = "METAS")
  private int precision = -1;

  /** Meta: the storage type, NORMAL or BINARY_STRING */
  @HopMetadataProperty(key = "storage_type")
  private String storageType;

  /** The conversion metadata if any conversion needs to take place */
  @HopMetadataProperty(
      key = "conversion_mask",
      injectionKey = "META_CONVERSION_MASK",
      injectionGroupKey = "METAS")
  private String conversionMask;

  /** Treat the date format as lenient */
  @HopMetadataProperty(
      key = "date_format_lenient",
      injectionKey = "META_DATE_FORMAT_LENIENT",
      injectionGroupKey = "METAS")
  private boolean dateFormatLenient;

  /** This is the locale to use for date parsing */
  @HopMetadataProperty(
      key = "date_format_locale",
      injectionKey = "META_DATE_FORMAT_LOCALE",
      injectionGroupKey = "METAS")
  private String dateFormatLocale;

  /** This is the time zone to use for date parsing */
  @HopMetadataProperty(
      key = "date_format_timezone",
      injectionKey = "META_DATE_FORMAT_TIMEZONE",
      injectionGroupKey = "METAS")
  private String dateFormatTimeZone;

  /** Treat string to number format as lenient */
  @HopMetadataProperty(
      key = "lenient_string_to_number",
      injectionKey = "META_LENIENT_STRING_TO_NUMBER",
      injectionGroupKey = "METAS")
  private boolean lenientStringToNumber;

  /** The decimal symbol for number conversions */
  @HopMetadataProperty(
      key = "decimal_symbol",
      injectionKey = "META_DECIMAL",
      injectionGroupKey = "METAS")
  private String decimalSymbol;

  /** The grouping symbol for number conversions */
  @HopMetadataProperty(
      key = "grouping_symbol",
      injectionKey = "META_GROUPING",
      injectionGroupKey = "METAS")
  private String groupingSymbol;

  /** The currency symbol for number conversions */
  @HopMetadataProperty(
      key = "currency_symbol",
      injectionKey = "META_CURRENCY",
      injectionGroupKey = "METAS")
  private String currencySymbol;

  /** The encoding to use when decoding binary data to Strings */
  @HopMetadataProperty(
      key = "encoding",
      injectionKey = "META_ENCODING",
      injectionGroupKey = "METAS")
  private String encoding;

  public SelectMetadataChange() {
    storageType = "";
  }

  public SelectMetadataChange(
      String name,
      String rename,
      String type,
      int length,
      int precision,
      String storageType,
      String conversionMask,
      boolean dateFormatLenient,
      String dateFormatLocale,
      String dateFormatTimeZone,
      boolean lenientStringToNumber,
      String decimalSymbol,
      String groupingSymbol,
      String currencySymbol) {
    this();
    this.name = name;
    this.rename = rename;
    this.type = type;
    this.length = length;
    this.precision = precision;
    this.storageType = storageType == null ? ValueMetaFactory.getValueMetaName(-1) : storageType;
    this.conversionMask = conversionMask;
    this.dateFormatLenient = dateFormatLenient;
    this.dateFormatLocale = dateFormatLocale;
    this.dateFormatTimeZone = dateFormatTimeZone;
    this.lenientStringToNumber = lenientStringToNumber;
    this.decimalSymbol = decimalSymbol;
    this.groupingSymbol = groupingSymbol;
    this.currencySymbol = currencySymbol;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return the rename
   */
  public String getRename() {
    return rename;
  }

  /**
   * @param rename the rename to set
   */
  public void setRename(String rename) {
    this.rename = rename;
  }

  /**
   * @return the type
   */
  public String getType() {
    return type;
  }

  /**
   * @param type the type to set
   */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * @return the length
   */
  public int getLength() {
    return length;
  }

  /**
   * @param length the length to set
   */
  public void setLength(int length) {
    this.length = length;
  }

  /**
   * @return the precision
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * @param precision the precision to set
   */
  public void setPrecision(int precision) {
    this.precision = precision;
  }

  /**
   * @return the storageType
   */
  public String getStorageType() {
    return storageType;
  }

  /**
   * @param storageType the storageType to set
   */
  public void setStorageType(String storageType) {
    this.storageType = storageType;
  }

  /**
   * @return the conversionMask
   */
  public String getConversionMask() {
    return conversionMask;
  }

  /**
   * @param conversionMask the conversionMask to set
   */
  public void setConversionMask(String conversionMask) {
    this.conversionMask = conversionMask;
  }

  /**
   * @return whether date conversion from string is lenient or not
   */
  public boolean isDateFormatLenient() {
    return dateFormatLenient;
  }

  /**
   * @param dateFormatLenient whether date conversion from string is lenient or not
   */
  public void setDateFormatLenient(boolean dateFormatLenient) {
    this.dateFormatLenient = dateFormatLenient;
  }

  /**
   * @return the decimalSymbol
   */
  public String getDecimalSymbol() {
    return decimalSymbol;
  }

  /**
   * @param decimalSymbol the decimalSymbol to set
   */
  public void setDecimalSymbol(String decimalSymbol) {
    this.decimalSymbol = decimalSymbol;
  }

  /**
   * @return the groupingSymbol
   */
  public String getGroupingSymbol() {
    return groupingSymbol;
  }

  /**
   * @param groupingSymbol the groupingSymbol to set
   */
  public void setGroupingSymbol(String groupingSymbol) {
    this.groupingSymbol = groupingSymbol;
  }

  /**
   * @return the currencySymbol
   */
  public String getCurrencySymbol() {
    return currencySymbol;
  }

  /**
   * @param currencySymbol the currencySymbol to set
   */
  public void setCurrencySymbol(String currencySymbol) {
    this.currencySymbol = currencySymbol;
  }

  /**
   * @return the encoding to use when decoding binary data to strings
   */
  public String getEncoding() {
    return encoding;
  }

  /**
   * @param encoding the encoding to use when decoding binary data to strings
   */
  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /**
   * @return the lenientStringToNumber
   */
  public boolean isLenientStringToNumber() {
    return lenientStringToNumber;
  }

  /**
   * @param lenientStringToNumber the lenientStringToNumber to set
   */
  public void setLenientStringToNumber(boolean lenientStringToNumber) {
    this.lenientStringToNumber = lenientStringToNumber;
  }

  /**
   * @return the dateFormatLocale
   */
  public String getDateFormatLocale() {
    return dateFormatLocale;
  }

  /**
   * @param dateFormatLocale the dateFormatLocale to set
   */
  public void setDateFormatLocale(String dateFormatLocale) {
    this.dateFormatLocale = dateFormatLocale;
  }

  /**
   * @return the dateFormatTimeZone
   */
  public String getDateFormatTimeZone() {
    return dateFormatTimeZone;
  }

  /**
   * @param dateFormatTimeZone the dateFormatTimeZone to set
   */
  public void setDateFormatTimeZone(String dateFormatTimeZone) {
    this.dateFormatTimeZone = dateFormatTimeZone;
  }
}
