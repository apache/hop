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

package org.apache.hop.pipeline.transforms.datagrid;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class DataGridFieldMeta implements Cloneable {

  @HopMetadataProperty(
      groupKey = "field",
      key = "currency",
      injectionKeyDescription = "DataGridDialog.Currency.Column")
  private String currency;

  @HopMetadataProperty(
      groupKey = "field",
      key = "decimal",
      injectionKeyDescription = "DataGridDialog.Decimal.Column")
  private String decimal;

  @HopMetadataProperty(
      groupKey = "field",
      key = "group",
      injectionKeyDescription = "DataGridDialog.Group.Column")
  private String group;

  @HopMetadataProperty(
      groupKey = "field",
      key = "name",
      injectionKeyDescription = "DataGridDialog.Name.Column")
  private String name;

  @HopMetadataProperty(
      groupKey = "field",
      key = "type",
      injectionKeyDescription = "DataGridDialog.Type.Column")
  private String type;

  @HopMetadataProperty(
      groupKey = "field",
      key = "format",
      injectionKeyDescription = "DataGridDialog.Format.Column")
  private String format;

  @HopMetadataProperty(
      groupKey = "field",
      key = "length",
      injectionKeyDescription = "DataGridDialog.Length.Column")
  private int lenght;

  @HopMetadataProperty(
      groupKey = "field",
      key = "precision",
      injectionKeyDescription = "DataGridDialog.Precision.Column")
  private int precision;

  /** Flag : set empty string */
  @HopMetadataProperty(
      groupKey = "field",
      key = "set_empty_string",
      injectionKeyDescription = "DataGridDialog.Value.SetEmptyString")
  private boolean emptyString;

  public DataGridFieldMeta() {}

  public DataGridFieldMeta(
      String currency,
      String decimal,
      String group,
      String name,
      String type,
      String format,
      int lenght,
      int precision,
      Boolean isEmptyString) {
    this.currency = currency;
    this.decimal = decimal;
    this.group = group;
    this.name = name;
    this.type = type;
    this.format = format;
    this.lenght = lenght;
    this.precision = precision;
    this.emptyString = isEmptyString;
  }

  public DataGridFieldMeta(DataGridFieldMeta m) {
    this.currency = m.currency;
    this.decimal = m.decimal;
    this.group = m.group;
    this.name = m.name;
    this.type = m.type;
    this.format = m.format;
    this.lenght = m.lenght;
    this.precision = m.precision;
    this.emptyString = m.emptyString;
  }

  @Override
  public DataGridFieldMeta clone() {
    return new DataGridFieldMeta(this);
  }

  public String getCurrency() {
    return currency;
  }

  public void setCurrency(String currency) {
    this.currency = currency;
  }

  public String getDecimal() {
    return decimal;
  }

  public void setDecimal(String decimal) {
    this.decimal = decimal;
  }

  public String getGroup() {
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public int getLenght() {
    return lenght;
  }

  public void setLenght(int lenght) {
    this.lenght = lenght;
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(int precision) {
    this.precision = precision;
  }

  public boolean isEmptyString() {
    return emptyString;
  }

  public void setEmptyString(boolean emptyString) {
    this.emptyString = emptyString;
  }
}
