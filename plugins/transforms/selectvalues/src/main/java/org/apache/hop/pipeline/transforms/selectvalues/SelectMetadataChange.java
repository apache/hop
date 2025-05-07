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

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class SelectMetadataChange {

  // META-DATA mode
  /** Fields of which we want to change the meta data */
  // @Injection(name = "META_NAME", group = "METAS")
  @HopMetadataProperty(
      key = "name",
      injectionKey = "META_NAME",
      injectionKeyDescription = "SelectValues.Injection.META_NAME")
  private String name;

  /** Meta: new name of field */
  // @Injection(name = "META_RENAME", group = "METAS")
  @HopMetadataProperty(
      key = "rename",
      injectionKey = "META_RENAME",
      injectionKeyDescription = "SelectValues.Injection.META_RENAME")
  private String rename;

  /** Meta: new Value type for this field or TYPE_NONE if no change needed! */
  @HopMetadataProperty(
      key = "type",
      injectionKey = "META_TYPE",
      injectionKeyDescription = "SelectValues.Injection.META_TYPE")
  private String type;

  /** Meta: new length of field */
  @HopMetadataProperty(
      key = "length",
      injectionKey = "META_LENGTH",
      injectionKeyDescription = "SelectValues.Injection.META_LENGTH")
  private int length = -1;

  /** Meta: new precision of field (for numbers) */
  @HopMetadataProperty(
      key = "precision",
      injectionKey = "META_PRECISION",
      injectionKeyDescription = "SelectValues.Injection.META_PRECISION")
  private int precision = -1;

  /** Meta: the storage type, NORMAL or BINARY_STRING */
  @HopMetadataProperty(
      key = "storage_type",
      injectionKey = "META_STORAGE_TYPE",
      injectionKeyDescription = "SelectValues.Injection.META_STORAGE_TYPE")
  private String storageType;

  /** The conversion metadata if any conversion needs to take place */
  @HopMetadataProperty(
      key = "conversion_mask",
      injectionKey = "META_CONVERSION_MASK",
      injectionKeyDescription = "SelectValues.Injection.META_CONVERSION_MASK")
  private String conversionMask;

  /** Treat the date format as lenient */
  @HopMetadataProperty(
      key = "date_format_lenient",
      injectionKey = "META_DATE_FORMAT_LENIENT",
      injectionKeyDescription = "SelectValues.Injection.META_DATE_FORMAT_LENIENT")
  private boolean dateFormatLenient;

  /** This is the locale to use for date parsing */
  @HopMetadataProperty(
      key = "date_format_locale",
      injectionKey = "META_DATE_FORMAT_LOCALE",
      injectionKeyDescription = "SelectValues.Injection.META_DATE_FORMAT_LOCALE")
  private String dateFormatLocale;

  /** This is the time zone to use for date parsing */
  @HopMetadataProperty(
      key = "date_format_timezone",
      injectionKey = "META_DATE_FORMAT_TIMEZONE",
      injectionKeyDescription = "SelectValues.Injection.META_DATE_FORMAT_TIMEZONE")
  private String dateFormatTimeZone;

  /** Treat string to number format as lenient */
  @HopMetadataProperty(
      key = "lenient_string_to_number",
      injectionKey = "META_LENIENT_STRING_TO_NUMBER",
      injectionKeyDescription = "SelectValues.Injection.META_LENIENT_STRING_TO_NUMBER")
  private boolean lenientStringToNumber;

  /** The decimal symbol for number conversions */
  @HopMetadataProperty(
      key = "decimal_symbol",
      injectionKey = "META_DECIMAL",
      injectionKeyDescription = "SelectValues.Injection.META_DECIMAL")
  private String decimalSymbol;

  /** The grouping symbol for number conversions */
  @HopMetadataProperty(
      key = "grouping_symbol",
      injectionKey = "META_GROUPING",
      injectionKeyDescription = "SelectValues.Injection.META_GROUPING")
  private String groupingSymbol;

  /** The currency symbol for number conversions */
  @HopMetadataProperty(
      key = "currency_symbol",
      injectionKey = "META_CURRENCY",
      injectionKeyDescription = "SelectValues.Injection.META_CURRENCY")
  private String currencySymbol;

  /** The encoding to use when decoding binary data to Strings */
  @HopMetadataProperty(
      key = "encoding",
      injectionKey = "META_ENCODING",
      injectionKeyDescription = "SelectValues.Injection.META_ENCODING")
  private String encoding;

  public SelectMetadataChange() {
    storageType = "";
    type = ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_NONE);
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
}
