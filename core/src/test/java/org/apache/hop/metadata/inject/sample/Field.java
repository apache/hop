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
 *
 */

package org.apache.hop.metadata.inject.sample;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class Field {
  @HopMetadataProperty(injectionKey = "FIELD_NAME")
  private String name;

  @HopMetadataProperty(
      injectionKey = "FIELD_TYPE",
      intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class)
  private int hopType;

  @HopMetadataProperty(injectionKey = "FIELD_LENGTH")
  private int length;

  @HopMetadataProperty(injectionKey = "FIELD_PRECISION")
  private int precision;

  @HopMetadataProperty(
      injectionKey = "FIELD_TRIM_TYPE",
      intCodeConverter = ValueMetaBase.TrimTypeCodeConverter.class)
  private int trimType;

  public Field() {}

  public Field(int hopType, int length, String name, int precision, int trimType) {
    this.hopType = hopType;
    this.length = length;
    this.name = name;
    this.precision = precision;
    this.trimType = trimType;
  }
}
