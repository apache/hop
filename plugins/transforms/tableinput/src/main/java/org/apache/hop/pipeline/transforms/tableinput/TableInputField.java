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

package org.apache.hop.pipeline.transforms.tableinput;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * Output field definition used when Table Input specifies fields instead of querying the database.
 */
@Getter
@Setter
public class TableInputField {
  @HopMetadataProperty(
      key = "name",
      injectionKey = "FIELD_NAME",
      injectionKeyDescription = "TableInputMeta.Injection.FieldName")
  private String name;

  @HopMetadataProperty(
      key = "type",
      intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class,
      injectionKey = "FIELD_TYPE",
      injectionKeyDescription = "TableInputMeta.Injection.FieldType")
  private int type;

  @HopMetadataProperty(
      key = "format",
      injectionKey = "FIELD_FORMAT",
      injectionKeyDescription = "TableInputMeta.Injection.FieldFormat")
  private String format;

  @HopMetadataProperty(
      key = "length",
      injectionKey = "FIELD_LENGTH",
      injectionKeyDescription = "TableInputMeta.Injection.FieldLength")
  private int length = -1;

  @HopMetadataProperty(
      key = "precision",
      injectionKey = "FIELD_PRECISION",
      injectionKeyDescription = "TableInputMeta.Injection.FieldPrecision")
  private int precision = -1;

  public TableInputField() {}

  public TableInputField(TableInputField other) {
    this();
    if (other == null) {
      return;
    }
    this.name = other.name;
    this.type = other.type;
    this.format = other.format;
    this.length = other.length;
    this.precision = other.precision;
  }

  public IValueMeta toValueMeta(String origin, IVariables variables) throws HopPluginException {
    int hopType = type;
    if (hopType == IValueMeta.TYPE_NONE) {
      hopType = IValueMeta.TYPE_STRING;
    }
    String fieldName = name;
    if (variables != null && fieldName != null) {
      fieldName = variables.resolve(fieldName);
    }
    IValueMeta valueMeta = ValueMetaFactory.createValueMeta(fieldName, hopType);
    valueMeta.setLength(length);
    valueMeta.setPrecision(precision);
    valueMeta.setOrigin(origin);
    valueMeta.setConversionMask(format);
    return valueMeta;
  }
}
