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

package org.apache.hop.pipeline.transforms.ifnull;

import java.util.Objects;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class Field {

  @HopMetadataProperty(
      key = "name",
      injectionKey = "FIELD_NAME",
      injectionKeyDescription = "IfNull.Injection.FIELD_NAME")
  private String name;

  @HopMetadataProperty(
      key = "value",
      injectionKey = "REPLACE_VALUE",
      injectionKeyDescription = "IfNull.Injection.REPLACE_VALUE")
  private String value;

  @HopMetadataProperty(
      key = "mask",
      injectionKey = "REPLACE_MASK",
      injectionKeyDescription = "IfNull.Injection.REPLACE_MASK")
  private String mask;

  @HopMetadataProperty(
      key = "set_empty_string",
      injectionKey = "SET_EMPTY_STRING",
      injectionKeyDescription = "IfNull.Injection.SET_EMPTY_STRING")
  private boolean setEmptyString;

  public Field() {}

  public Field(Field field) {
    this.name = field.name;
    this.value = field.value;
    this.mask = field.mask;
    this.setEmptyString = field.setEmptyString;
  }

  public String getName() {
    return name;
  }

  public void setName(String fieldName) {
    this.name = fieldName;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String replaceValue) {
    this.value = replaceValue;
  }

  public String getMask() {
    return mask;
  }

  public void setMask(String replaceMask) {
    this.mask = replaceMask;
  }

  public boolean isSetEmptyString() {
    return setEmptyString;
  }

  public void setSetEmptyString(boolean setEmptyString) {
    this.setEmptyString = setEmptyString;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Field field = (Field) o;
    return Objects.equals(name, field.name)
        && Objects.equals(value, field.value)
        && Objects.equals(mask, field.mask)
        && this.setEmptyString == field.setEmptyString;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value, mask, setEmptyString);
  }

  @Override
  public Field clone() {
    return new Field(this);
  }
}
