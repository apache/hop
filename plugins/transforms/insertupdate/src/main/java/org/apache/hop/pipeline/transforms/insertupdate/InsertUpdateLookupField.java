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

package org.apache.hop.pipeline.transforms.insertupdate;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;

public class InsertUpdateLookupField {

  public static final Class<?> PKG = InsertUpdateLookupField.class; // i18n

  /** Lookup key fields * */
  @HopMetadataProperty(
      key = "key",
      injectionGroupKey = "KEYS",
      injectionGroupDescription = "InsertUpdateMeta.Injection.KEYS",
      injectionKeyDescription = "InsertUpdateMeta.Injection.KEY")
  private List<InsertUpdateKeyField> lookupKeys;

  /** Update fields * */
  @HopMetadataProperty(
      key = "value",
      injectionGroupKey = "UPDATES",
      injectionGroupDescription = "InsertUpdateMeta.Injection.UPDATES",
      injectionKeyDescription = "InsertUpdateMeta.Injection.UPDATE")
  private List<InsertUpdateValue> valueFields;

  /** The lookup table's schema name */
  @HopMetadataProperty(
      key = "schema",
      injectionKeyDescription = "InsertUpdateMeta.Injection.SCHEMA_NAME",
      injectionKey = "SCHEMA_NAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schemaName;

  /** The lookup table name */
  @HopMetadataProperty(
      key = "table",
      injectionKeyDescription = "InsertUpdateMeta.Injection.TABLE_NAME",
      injectionKey = "TABLE_NAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tableName;

  public InsertUpdateLookupField() {
    init();
  }

  public InsertUpdateLookupField(String schemaName, String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    init();
  }

  public InsertUpdateLookupField(
      String schemaName,
      String tableName,
      List<InsertUpdateKeyField> lookupKeys,
      List<InsertUpdateValue> valueFields) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.lookupKeys = lookupKeys;
    this.valueFields = valueFields;
    init();
  }

  protected void init() {
    lookupKeys = new ArrayList<>();
    valueFields = new ArrayList<>();
    schemaName = "";
    tableName = BaseMessages.getString(PKG, "InsertUpdateMeta.DefaultTableName");
  }

  /**
   * @return the schemaName
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * @param schemaName the schemaName to set
   */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  /**
   * @return Returns the tableName.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName The tableName to set.
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<InsertUpdateKeyField> getLookupKeys() {
    return lookupKeys;
  }

  public void setLookupKeys(List<InsertUpdateKeyField> lookupKeys) {
    this.lookupKeys = lookupKeys;
  }

  public List<InsertUpdateValue> getValueFields() {
    return valueFields;
  }

  public void setValueFields(List<InsertUpdateValue> valueFields) {
    this.valueFields = valueFields;
  }
}
