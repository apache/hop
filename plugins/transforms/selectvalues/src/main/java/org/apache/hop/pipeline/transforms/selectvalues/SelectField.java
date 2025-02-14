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

  /** Select: Name of the selected field */
  @HopMetadataProperty(key = "name", injectionKey = "FIELD_NAME", injectionGroupKey = "FIELD")
  private String name;

  /** Select: Rename to ... */
  // @Injection(name = "FIELD_RENAME", group = "FIELDS")
  @HopMetadataProperty(key = "rename", injectionKey = "FIELD_RENAME", injectionGroupKey = "FIELD")
  private String rename;

  /** Select: length of field */
  // @Injection(name = "FIELD_LENGTH", group = "FIELDS")
  @HopMetadataProperty(key = "length", injectionKey = "FIELD_LENGTH", injectionGroupKey = "FIELD")
  private int length = SelectValuesMeta.UNDEFINED;

  /** Select: Precision of field (for numbers) */
  // @Injection(name = "FIELD_PRECISION", group = "FIELDS")
  @HopMetadataProperty(
      key = "precision",
      injectionKey = "FIELD_PRECISION",
      injectionGroupKey = "FIELD")
  private int precision = SelectValuesMeta.UNDEFINED;
}
