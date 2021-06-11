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

package org.apache.hop.pipeline.transforms.databaselookup;

import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class ReturnValue {
  @HopMetadataProperty(
      key = "name",
      injectionKey = "return_table_field",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.ReturnTableField")
  private String tableField;

  @HopMetadataProperty(
      key = "rename",
      injectionKey = "return_rename",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.ReturnRename")
  private String newName;

  @HopMetadataProperty(
      key = "default",
      injectionKey = "return_default_value",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.ReturnDefaultValue")
  private String defaultValue;

  @HopMetadataProperty(
      key = "type",
      injectionKey = "return_default_type",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.ReturnDefaultType")
  private String defaultType;

  /** Have the lookup eat the incoming row when nothing gets found */
  @HopMetadataProperty(
          key = "trim_type",
          injectionKey = "return_trim_type",
          injectionKeyDescription = "DatabaseLookupMeta.Injection.TrimType")
  private String trimType;

  public ReturnValue() {}

  public ReturnValue(ReturnValue r) {
    this.tableField = r.tableField;
    this.newName = r.newName;
    this.defaultValue = r.defaultValue;
    this.defaultType = r.defaultType;
    this.trimType = ValueMetaBase.trimTypeCode[0];
  }

  public ReturnValue(String tableField, String newName, String defaultValue, String defaultType) {
    this.tableField = tableField;
    this.newName = newName;
    this.defaultValue = defaultValue;
    this.defaultType = defaultType;
  }

  /**
   * Gets tableField
   *
   * @return value of tableField
   */
  public String getTableField() {
    return tableField;
  }

  /** @param tableField The tableField to set */
  public void setTableField(String tableField) {
    this.tableField = tableField;
  }

  /**
   * Gets newName
   *
   * @return value of newName
   */
  public String getNewName() {
    return newName;
  }

  /** @param newName The newName to set */
  public void setNewName(String newName) {
    this.newName = newName;
  }

  /**
   * Gets defaultValue
   *
   * @return value of defaultValue
   */
  public String getDefaultValue() {
    return defaultValue;
  }

  /** @param defaultValue The defaultValue to set */
  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
  }

  /**
   * Gets defaultType
   *
   * @return value of defaultType
   */
  public String getDefaultType() {
    return defaultType;
  }

  /** @param defaultType The defaultType to set */
  public void setDefaultType(String defaultType) {
    this.defaultType = defaultType;
  }

  /**
   * The type of trim applied to string fields if needed
   *
   * @return
   */
  public String getTrimType() {
    return trimType;
  }

  public void setTrimType(String trimType) {
    this.trimType = trimType;
  }



}
