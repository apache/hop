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

package org.apache.hop.pipeline.transforms.ifnull;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class ValueType {

  /** which types to display? */
  @HopMetadataProperty(
      key = "name",
      injectionKey = "TYPE_NAME",
      injectionKeyDescription = "IfNull.Injection.TYPE_NAME")
  private String name;

  /** by which value we replace */
  @HopMetadataProperty(
      key = "value",
      injectionKey = "TYPE_REPLACE_VALUE",
      injectionKeyDescription = "IfNull.Injection.TYPE_REPLACE_VALUE")
  private String value;

  @HopMetadataProperty(
      key = "mask",
      injectionKey = "TYPE_REPLACE_MASK",
      injectionKeyDescription = "IfNull.Injection.TYPE_REPLACE_MASK")
  private String mask;

  /** Flag : set empty string for type */
  @HopMetadataProperty(
      key = "set_type_empty_string",
      injectionKey = "SET_TYPE_EMPTY_STRING",
      injectionKeyDescription = "IfNull.Injection.SET_TYPE_EMPTY_STRING")
  private boolean setEmptyString;

  public ValueType() {}

  public ValueType(ValueType other) {
    this.name = other.name;
    this.value = other.value;
    this.mask = other.mask;
    this.setEmptyString = other.setEmptyString;
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

  public String getMask() {
    return mask;
  }

  public void setMask(String mask) {
    this.mask = mask;
  }

  public boolean isSetEmptyString() {
    return setEmptyString;
  }

  public void setSetEmptyString(boolean setEmptyString) {
    this.setEmptyString = setEmptyString;
  }
}
