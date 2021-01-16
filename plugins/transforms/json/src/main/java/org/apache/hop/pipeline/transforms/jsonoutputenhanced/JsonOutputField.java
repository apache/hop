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

public class JsonOutputField implements Cloneable {

    @Injection( name = "JSON_FIELDNAME", group = "FIELDS" )
    private String fieldName;
    @Injection( name = "JSON_ELEMENTNAME", group = "FIELDS" )
    private String elementName;
    @Injection( name = "JSON_ISJSONFRAGMENT", group = "FIELDS" )
    private boolean isJSONFragment;
    @Injection( name = "JSON_REMOVEIFBLANK", group = "FIELDS" )
    private boolean removeIfBlank;

    public boolean isJSONFragment() {
        return isJSONFragment;
    }

    public void setJSONFragment(boolean JSONFragment) {
        isJSONFragment = JSONFragment;
    }

    public boolean isRemoveIfBlank() {
        return removeIfBlank;
    }

    public void setRemoveIfBlank(boolean removeIfBlank) {
        this.removeIfBlank = removeIfBlank;
    }

    public JsonOutputField(String fieldName, String elementName, int type, String format, int length,
                           int precision, String currencySymbol, String decimalSymbol, String groupSymbol, String nullString,
                           boolean attribute, String attributeParentName) {
        this.fieldName = fieldName;
        this.elementName = elementName;
    }

    public JsonOutputField() {
    }

    public int compare(Object obj) {
        JsonOutputField field = (JsonOutputField) obj;

        return fieldName.compareTo(field.getFieldName());
    }

    public boolean equal(Object obj) {
        JsonOutputField field = (JsonOutputField) obj;

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

    /**
     * @return Returns the elementName.
     */
    public String getElementName() {
        return elementName;
    }

    /**
     * @param elementName The elementName to set.
     */
    public void setElementName(String elementName) {
        this.elementName = elementName;
    }
}
