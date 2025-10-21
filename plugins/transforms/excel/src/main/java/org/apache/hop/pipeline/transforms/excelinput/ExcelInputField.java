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

package org.apache.hop.pipeline.transforms.excelinput;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Describes a single field in an excel file */
@Getter
@Setter
public class ExcelInputField implements Cloneable {
  @HopMetadataProperty(
      key = "name",
      injectionKey = "NAME",
      injectionKeyDescription = "ExcelInput.Injection.NAME")
  private String name;

  @HopMetadataProperty(
      key = "type",
      injectionKey = "TYPE",
      injectionKeyDescription = "ExcelInput.Injection.TYPE")
  private String type;

  @HopMetadataProperty(
      key = "length",
      injectionKey = "LENGTH",
      injectionKeyDescription = "ExcelInput.Injection.LENGTH")
  private int length = -1;

  @HopMetadataProperty(
      key = "precision",
      injectionKey = "PRECISION",
      injectionKeyDescription = "ExcelInput.Injection.PRECISION")
  private int precision = -1;

  @HopMetadataProperty(
      key = "trim_type",
      storeWithCode = true,
      injectionKey = "TRIM_TYPE",
      injectionKeyDescription = "ExcelInput.Injection.TRIM_TYPE")
  private IValueMeta.TrimType trimType;

  @HopMetadataProperty(
      key = "format",
      injectionKey = "FORMAT",
      injectionKeyDescription = "ExcelInput.Injection.FORMAT")
  private String format;

  @HopMetadataProperty(
      key = "currency",
      injectionKey = "CURRENCY",
      injectionKeyDescription = "ExcelInput.Injection.CURRENCY")
  private String currencySymbol;

  @HopMetadataProperty(
      key = "decimal",
      injectionKey = "DECIMAL",
      injectionKeyDescription = "ExcelInput.Injection.DECIMAL")
  private String decimalSymbol;

  @HopMetadataProperty(
      key = "group",
      injectionKey = "GROUP",
      injectionKeyDescription = "ExcelInput.Injection.GROUP")
  private String groupSymbol;

  @HopMetadataProperty(
      key = "repeat",
      injectionKey = "REPEAT",
      injectionKeyDescription = "ExcelInput.Injection.REPEAT")
  private boolean repeat;

  public ExcelInputField() {}

  public ExcelInputField(ExcelInputField f) {
    this();
    this.name = f.name;
    this.type = f.type;
    this.length = f.length;
    this.precision = f.precision;
    this.trimType = f.trimType;
    this.format = f.format;
    this.currencySymbol = f.currencySymbol;
    this.decimalSymbol = f.decimalSymbol;
    this.groupSymbol = f.groupSymbol;
    this.repeat = f.repeat;
  }

  public ExcelInputField(String name, int length, int precision) {
    this();
    this.name = name;
    this.length = length;
    this.precision = precision;
  }

  @Override
  public ExcelInputField clone() {
    return new ExcelInputField(this);
  }

  @Override
  public String toString() {
    return name + ":" + type + "(" + length + "," + precision + ")";
  }

  public int getHopType() {
    return ValueMetaFactory.getIdForValueMeta(type);
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName(getHopType());
  }
}
