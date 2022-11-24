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

package org.apache.hop.pipeline.transforms.concatfields;

import org.apache.hop.metadata.api.HopMetadataProperty;

/** Describes a single field in a text file */
public class ConcatField implements Cloneable {
  @HopMetadataProperty(key = "name", injectionKey = "OUTPUT_FIELDNAME")
  private String name;

  @HopMetadataProperty(key = "type", injectionKey = "OUTPUT_TYPE")
  private String type;

  @HopMetadataProperty(key = "format", injectionKey = "OUTPUT_FORMAT")
  private String format;

  @HopMetadataProperty(key = "length", injectionKey = "OUTPUT_LENGTH")
  private int length = -1;

  @HopMetadataProperty(key = "precision", injectionKey = "OUTPUT_PRECISION")
  private int precision = -1;

  @HopMetadataProperty(key = "currency", injectionKey = "OUTPUT_CURRENCY")
  private String currencySymbol;

  @HopMetadataProperty(key = "decimal", injectionKey = "OUTPUT_DECIMAL")
  private String decimalSymbol;

  @HopMetadataProperty(key = "group", injectionKey = "OUTPUT_GROUP")
  private String groupingSymbol;

  @HopMetadataProperty(key = "nullif", injectionKey = "OUTPUT_NULL")
  private String nullString;

  @HopMetadataProperty(key = "trim_type", injectionKey = "OUTPUT_TRIM")
  private String trimType;

  public ConcatField() {}

  public ConcatField(ConcatField f) {
    this.name = f.name;
    this.type = f.type;
    this.format = f.format;
    this.length = f.length;
    this.precision = f.precision;
    this.currencySymbol = f.currencySymbol;
    this.decimalSymbol = f.decimalSymbol;
    this.groupingSymbol = f.groupingSymbol;
    this.nullString = f.nullString;
    this.trimType = f.trimType;
  }

  public ConcatField clone() {
    return new ConcatField(this);
  }

  public int compare(Object obj) {
    ConcatField field = (ConcatField) obj;
    return name.compareTo(field.getName());
  }

  public boolean equal(Object obj) {
    ConcatField field = (ConcatField) obj;
    return name.equals(field.getName());
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
   * Gets type
   *
   * @return value of type
   */
  public String getType() {
    return type;
  }

  /**
   * Sets type
   *
   * @param type value of type
   */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * Gets format
   *
   * @return value of format
   */
  public String getFormat() {
    return format;
  }

  /**
   * Sets format
   *
   * @param format value of format
   */
  public void setFormat(String format) {
    this.format = format;
  }

  /**
   * Gets length
   *
   * @return value of length
   */
  public int getLength() {
    return length;
  }

  /**
   * Sets length
   *
   * @param length value of length
   */
  public void setLength(int length) {
    this.length = length;
  }

  /**
   * Gets precision
   *
   * @return value of precision
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * Sets precision
   *
   * @param precision value of precision
   */
  public void setPrecision(int precision) {
    this.precision = precision;
  }

  /**
   * Gets currencySymbol
   *
   * @return value of currencySymbol
   */
  public String getCurrencySymbol() {
    return currencySymbol;
  }

  /**
   * Sets currencySymbol
   *
   * @param currencySymbol value of currencySymbol
   */
  public void setCurrencySymbol(String currencySymbol) {
    this.currencySymbol = currencySymbol;
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
   * Gets nullString
   *
   * @return value of nullString
   */
  public String getNullString() {
    return nullString;
  }

  /**
   * Sets nullString
   *
   * @param nullString value of nullString
   */
  public void setNullString(String nullString) {
    this.nullString = nullString;
  }

  /**
   * Gets trimType
   *
   * @return value of trimType
   */
  public String getTrimType() {
    return trimType;
  }

  /**
   * Sets trimType
   *
   * @param trimType value of trimType
   */
  public void setTrimType(String trimType) {
    this.trimType = trimType;
  }
}
