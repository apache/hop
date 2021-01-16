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

package org.apache.hop.pipeline.transforms.jsonoutputenhanced;


import org.apache.hop.core.injection.Injection;

public class JsonOutputKeyField implements Cloneable {

    @Injection( name = "JSON_FIELDNAME", group = "KEY_FIELDS" )
    private String fieldName;

    @Injection( name = "JSON_ELEMENTNAME", group = "KEY_FIELDS" )
    private String elementName;

    public JsonOutputKeyField(String fieldName) {
        this.fieldName = fieldName;
    }

    public JsonOutputKeyField() {
    }

    public int compare(Object obj) {
        JsonOutputKeyField field = (JsonOutputKeyField) obj;

        return fieldName.compareTo(field.getFieldName());
    }

    public boolean equal(Object obj) {
        JsonOutputKeyField field = (JsonOutputKeyField) obj;

        return fieldName.equals(field.getFieldName());
    }

    public Object clone() {
        try {
            Object retval = super.clone();
            return retval;
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldname) {
        this.fieldName = fieldname;
    }

    public String getElementName() {
        return elementName;
    }

    public void setElementName(String elementName) {
        this.elementName = elementName;
    }
}
