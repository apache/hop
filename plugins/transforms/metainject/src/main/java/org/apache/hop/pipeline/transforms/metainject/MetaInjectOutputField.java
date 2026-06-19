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

package org.apache.hop.pipeline.transforms.metainject;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class MetaInjectOutputField {
  @HopMetadataProperty(
      key = "source_output_field_name",
      injectionKey = "SOURCE_OUTPUT_NAME",
      injectionKeyDescription = "MetaInject.Injection.SOURCE_OUTPUT_NAME")
  private String name;

  @HopMetadataProperty(
      key = "source_output_field_type",
      intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class,
      injectionKey = "SOURCE_OUTPUT_TYPE",
      injectionKeyDescription = "MetaInject.Injection.SOURCE_OUTPUT_TYPE")
  private int type;

  @HopMetadataProperty(
      key = "source_output_field_length",
      injectionKey = "SOURCE_OUTPUT_LENGTH",
      injectionKeyDescription = "MetaInject.Injection.SOURCE_OUTPUT_LENGTH")
  private int length;

  @HopMetadataProperty(
      key = "source_output_field_precision",
      injectionKey = "SOURCE_OUTPUT_PRECISION",
      injectionKeyDescription = "MetaInject.Injection.SOURCE_OUTPUT_PRECISION")
  private int precision;

  public MetaInjectOutputField() {}

  public MetaInjectOutputField(MetaInjectOutputField f) {
    super();
    this.name = f.name;
    this.type = f.type;
    this.length = f.length;
    this.precision = f.precision;
  }

  public MetaInjectOutputField(String name, int type, int length, int precision) {
    super();
    this.name = name;
    this.type = type;
    this.length = length;
    this.precision = precision;
  }

  public String getTypeDescription() {
    return ValueMetaFactory.getValueMetaName(type);
  }

  public IValueMeta createValueMeta() throws HopPluginException {
    return ValueMetaFactory.createValueMeta(name, type, length, precision);
  }
}
