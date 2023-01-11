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

package org.apache.hop.pipeline.transforms.nullif;

import org.apache.hop.core.Const;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class NullIfField implements Cloneable {

    @HopMetadataProperty(injectionKey = "FIELDNAME", injectionKeyDescription = "NullIf.Injection.FIELDNAME")
    private String name;

    @HopMetadataProperty(injectionKey = "FIELDVALUE", injectionKeyDescription = "NullIf.Injection.FIELDVALUE")
    private String value;
    
    public NullIfField() {
    }

    public NullIfField(String name, String value) {
        this.name = name;
        this.value = value;
    }
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NullIfField field = (NullIfField) o;
        return Objects.equals(name, field.name) && Objects.equals(value, field.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public NullIfField clone() {
        return new NullIfField(name, value);
    }
}
