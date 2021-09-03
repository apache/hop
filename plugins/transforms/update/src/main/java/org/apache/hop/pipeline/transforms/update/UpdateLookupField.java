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

package org.apache.hop.pipeline.transforms.update;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.ArrayList;
import java.util.List;

public class UpdateLookupField {

    /** Lookup key fields **/
    @HopMetadataProperty(key = "key",
            injectionGroupKey = "KEYS",
            injectionGroupDescription = "UpdateMeta.Injection.LookupKeys",
            injectionKeyDescription = "UpdateMeta.Injection.LookupKey")
    private List<UpdateKeyField> lookupKeys;

    /** Update fields **/
    @HopMetadataProperty(key = "value",
            injectionGroupKey = "UPDATES",
            injectionGroupDescription = "UpdateMeta.Injection.UpdateKeys",
            injectionKeyDescription = "UpdateMeta.Injection.UpdateKey")
    private List<UpdateField> updateFields;

    /** The lookup table's schema name */
    @HopMetadataProperty(key = "schema",
            injectionKeyDescription = "UpdateMeta.Injection.SchemaName",
            injectionKey = "SCHEMA_NAME")
    private String schemaName;

    /** The lookup table name */
    @HopMetadataProperty(key = "table",
            injectionKeyDescription = "UpdateMeta.Injection.TableName",
            injectionKey = "TABLE_NAME")
    private String tableName;

    public UpdateLookupField() {
        init();
    }

    public UpdateLookupField(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        init();
    }

    public UpdateLookupField(String schemaName, String tableName, List<UpdateKeyField> lookupKeys, List<UpdateField> updateFields) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.lookupKeys = lookupKeys;
        this.updateFields = updateFields;
    }

    protected void init() {
        lookupKeys = new ArrayList<>();
        updateFields = new ArrayList<>();
    }

    /** @return the schemaName */
    public String getSchemaName() {
        return schemaName;
    }

    /** @param schemaName the schemaName to set */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /** @return Returns the tableName. */
    public String getTableName() {
        return tableName;
    }

    /** @param tableName The tableName to set. */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<UpdateKeyField> getLookupKeys() {
        return lookupKeys;
    }

    public void setLookupKeys(List<UpdateKeyField> lookupKeys) {
        this.lookupKeys = lookupKeys;
    }

    public List<UpdateField> getUpdateFields() {
        return updateFields;
    }

    public void setUpdateFields(List<UpdateField> updateFields) {
        this.updateFields = updateFields;
    }
}
