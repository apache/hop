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

package org.apache.hop.pipeline.transforms.selectvalues;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class SelectField {

  public SelectField() {}

  /** Select: Name of the selected field */
  @HopMetadataProperty(key = "name", injectionKey = "FIELD_NAME", injectionGroupKey = "FIELDS")
  private String name;

  /** Select: Rename to ... */
  // @Injection(name = "FIELD_RENAME", group = "FIELDS")
  @HopMetadataProperty(key = "rename", injectionKey = "FIELD_RENAME", injectionGroupKey = "FIELDS")
  private String rename;

  /** Select: length of field */
  // @Injection(name = "FIELD_LENGTH", group = "FIELDS")
  @HopMetadataProperty(key = "length", injectionKey = "FIELD_LENGTH", injectionGroupKey = "FIELDS")
  private int length = SelectValuesMeta.UNDEFINED;

  /** Select: Precision of field (for numbers) */
  // @Injection(name = "FIELD_PRECISION", group = "FIELDS")
  @HopMetadataProperty(
      key = "precision",
      injectionKey = "FIELD_PRECISION",
      injectionGroupKey = "FIELDS")
  private int precision = SelectValuesMeta.UNDEFINED;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getRename() {
    return rename;
  }

  public void setRename(String rename) {
    this.rename = rename;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(int precision) {
    this.precision = precision;
  }
}
