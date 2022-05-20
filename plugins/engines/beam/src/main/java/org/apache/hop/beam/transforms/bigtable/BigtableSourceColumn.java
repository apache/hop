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

package org.apache.hop.beam.transforms.bigtable;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class BigtableSourceColumn {
    @HopMetadataProperty(key = "qualifier")
    private String qualifier;

    @HopMetadataProperty(key = "target_type")
    private String targetType;

    @HopMetadataProperty(key = "target_field_name")
    private String targetFieldName;

    public BigtableSourceColumn() {
    }

    public BigtableSourceColumn(String qualifier, String targetType, String targetFieldName) {
        this.qualifier = qualifier;
        this.targetType = targetType;
        this.targetFieldName = targetFieldName;
    }

    public IValueMeta getValueMeta() throws HopPluginException {
        int type = ValueMetaFactory.getIdForValueMeta(targetType);
        String name = Const.NVL(targetFieldName, qualifier);
        return ValueMetaFactory.createValueMeta(name, type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BigtableSourceColumn that = (BigtableSourceColumn) o;
        return qualifier.equals(that.qualifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(qualifier);
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public String getTargetType() {
        return targetType;
    }

    public void setTargetType(String targetType) {
        this.targetType = targetType;
    }

    public String getTargetFieldName() {
        return targetFieldName;
    }

    public void setTargetFieldName(String targetFieldName) {
        this.targetFieldName = targetFieldName;
    }
}
