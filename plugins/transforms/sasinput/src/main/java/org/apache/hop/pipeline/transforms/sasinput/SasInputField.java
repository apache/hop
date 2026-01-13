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

package org.apache.hop.pipeline.transforms.sasinput;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** This defines a selected list of fields from the input files */
@Getter
@Setter
public class SasInputField implements Cloneable {
  @HopMetadataProperty private String name;
  @HopMetadataProperty private String rename;

  @HopMetadataProperty(intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class)
  private int type;

  @HopMetadataProperty private int length;
  @HopMetadataProperty private int precision;

  @HopMetadataProperty(key = "conversion_mask")
  private String conversionMask;

  @HopMetadataProperty(key = "decimal")
  private String decimalSymbol;

  @HopMetadataProperty(key = "grouping")
  private String groupingSymbol;

  @HopMetadataProperty(
      key = "trim_type",
      intCodeConverter = ValueMetaBase.TrimTypeCodeConverter.class)
  private int trimType;

  public SasInputField() {}

  public SasInputField(SasInputField field) {
    this.name = field.name;
    this.rename = field.rename;
    this.type = field.type;
    this.conversionMask = field.conversionMask;
    this.decimalSymbol = field.decimalSymbol;
    this.groupingSymbol = field.groupingSymbol;
    this.trimType = field.trimType;
  }

  @Override
  public SasInputField clone() throws CloneNotSupportedException {
    return new SasInputField(this);
  }
}
