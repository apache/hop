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

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class SelectField {

  public SelectField() {}

  public SelectField(SelectField f) {
    this.name = f.name;
    this.rename = f.rename;
    this.length = f.length;
    this.precision = f.precision;
  }

  /** Select: Name of the selected field */
  @HopMetadataProperty(
      key = "name",
      injectionKey = "FIELD_NAME",
      injectionKeyDescription = "SelectValues.Injection.FIELD_NAME")
  private String name;

  /** Select: Rename to ... */
  @HopMetadataProperty(
      key = "rename",
      injectionKey = "FIELD_RENAME",
      injectionKeyDescription = "SelectValues.Injection.FIELD_RENAME")
  private String rename;

  /** Select: length of field */
  @HopMetadataProperty(
      key = "length",
      injectionKey = "FIELD_LENGTH",
      injectionKeyDescription = "SelectValues.Injection.FIELD_LENGTH")
  private int length = SelectValuesMeta.UNDEFINED;

  /** Select: Precision of field (for numbers) */
  @HopMetadataProperty(
      key = "precision",
      injectionKey = "FIELD_PRECISION",
      injectionKeyDescription = "SelectValues.Injection.FIELD_PRECISION")
  private int precision = SelectValuesMeta.UNDEFINED;
}
