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

package org.apache.hop.pipeline.transforms.setvaluefield;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class SetField implements Cloneable {

  @HopMetadataProperty(
      key = "name",
      injectionKey = "FIELD_NAME",
      injectionKeyDescription = "SetValueField.Injection.SetField.FieldName")
  private String fieldName;

  @HopMetadataProperty(
      key = "replaceby",
      injectionKey = "REPLACE_BY_FIELD_VALUE",
      injectionKeyDescription = "SetValueField.Injection.SetField.ReplaceByField")
  private String replaceByField;

  public SetField() {
  }

  public SetField(SetField cloned) {
    this.fieldName = cloned.fieldName;
    this.replaceByField = cloned.replaceByField;
  }

  public SetField(String name, String replaceBy) {
    this.fieldName = name;
    this.replaceByField = replaceBy;
  }
    
  @Override
  public Object clone() {
    return new SetField(this);
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(final String name) {
    this.fieldName = name;
  }

  public String getReplaceByField() {
    return this.replaceByField;
  }

  public void setReplaceByField(final String field) {
    this.replaceByField = field;
  }
}