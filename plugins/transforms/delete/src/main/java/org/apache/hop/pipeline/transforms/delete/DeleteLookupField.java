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

package org.apache.hop.pipeline.transforms.delete;


import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DeleteLookupField {

    @HopMetadataProperty(
            key = "key",
            injectionGroupDescription = "Delete.Injection.Fields",
            injectionKeyDescription = "Delete.Injection.Field")
    private List<DeleteKeyField> fields;

    /** The target schema name */
    @HopMetadataProperty(
            key = "schema",
            injectionKeyDescription = "Delete.Injection.SchemaName.Field")
    private String schemaName;

    /** The lookup table name */
    @HopMetadataProperty(
            key = "table",
            injectionKeyDescription = "Delete.Injection.TableName.Field")
    private String tableName;

    public DeleteLookupField() {
        fields = new ArrayList<>();
    }

    public DeleteLookupField(DeleteLookupField obj) {
        this.schemaName = obj.schemaName;
        this.tableName = obj.tableName;

        fields = new ArrayList<>();
        for (DeleteKeyField field : obj.fields) {
            this.fields.add(new DeleteKeyField(field));
        }
    }

    public DeleteLookupField(String schemaName, String tableName, List<DeleteKeyField> fields) {
        this.fields = fields;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    /** @return Returns the tableName. */
    public String getTableName() {
        return tableName;
    }

    /** @param tableName The tableName to set. */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }


    /**
     * Gets fields
     *
     * @return value of fields
     */
    public List<DeleteKeyField> getFields() {
        return fields;
    }

    /** @param fields The fields to set */
    public void setFields(List<DeleteKeyField> fields) {
        this.fields = fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteLookupField that = (DeleteLookupField) o;
        return fields.equals(that.fields) && Objects.equals(schemaName, that.schemaName) && tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, schemaName, tableName);
    }
}
