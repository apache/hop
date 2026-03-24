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

package org.apache.hop.pipeline.transforms.jsoninput;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Describes a JsonPath field. */
@Getter
@Setter
public class JsonInputField {
  @HopMetadataProperty(
      key = "name",
      injectionKey = "FIELD_NAME",
      injectionKeyDescription = "JsonInput.Injection.FIELD_NAME")
  protected String name;

  @HopMetadataProperty(
      key = "length",
      injectionKey = "FIELD_LENGTH",
      injectionKeyDescription = "JsonInput.Injection.FIELD_LENGTH")
  protected int length = -1;

  @HopMetadataProperty(
      key = "type",
      intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class,
      injectionKey = "FIELD_TYPE",
      injectionKeyDescription = "JsonInput.Injection.FIELD_TYPE")
  protected int type;

  @HopMetadataProperty(
      key = "format",
      injectionKey = "FIELD_FORMAT",
      injectionKeyDescription = "JsonInput.Injection.FIELD_FORMAT")
  protected String format;

  @HopMetadataProperty(
      key = "trim_type",
      intCodeConverter = ValueMetaBase.TrimTypeCodeConverter.class,
      injectionKey = "FIELD_TRIM_TYPE",
      injectionKeyDescription = "JsonInput.Injection.FIELD_TRIM_TYPE")
  protected int trimType;

  @HopMetadataProperty(
      key = "precision",
      injectionKey = "FIELD_PRECISION",
      injectionKeyDescription = "JsonInput.Injection.FIELD_PRECISION")
  protected int precision;

  @HopMetadataProperty(
      key = "currency",
      injectionKey = "FIELD_CURRENCY",
      injectionKeyDescription = "JsonInput.Injection.FIELD_CURRENCY")
  protected String currencySymbol;

  @HopMetadataProperty(
      key = "decimal",
      injectionKey = "FIELD_DECIMAL",
      injectionKeyDescription = "JsonInput.Injection.FIELD_DECIMAL")
  protected String decimalSymbol;

  @HopMetadataProperty(
      key = "group",
      injectionKey = "FIELD_GROUP",
      injectionKeyDescription = "JsonInput.Injection.FIELD_GROUP")
  protected String groupSymbol;

  @HopMetadataProperty(
      key = "repeat",
      injectionKey = "FIELD_REPEAT",
      injectionKeyDescription = "JsonInput.Injection.FIELD_REPEAT")
  protected boolean repeated;

  @HopMetadataProperty(
      key = "path",
      injectionKey = "FIELD_PATH",
      injectionKeyDescription = "JsonInput.Injection.FIELD_PATH")
  private String path;

  public JsonInputField(String fieldName) {
    super();
    setName(fieldName);
  }

  public JsonInputField() {
    this("");
  }

  public JsonInputField(JsonInputField f) {
    this();
    this.name = f.name;
    this.path = f.path;
    this.type = f.type;
    this.length = f.length;
    this.precision = f.precision;
    this.format = f.format;
    this.currencySymbol = f.currencySymbol;
    this.decimalSymbol = f.decimalSymbol;
    this.groupSymbol = f.groupSymbol;
    this.repeated = f.repeated;
    this.trimType = f.trimType;
  }

  @Override
  public JsonInputField clone() {
    return new JsonInputField(this);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof JsonInputField that)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), name);
  }

  public IValueMeta toValueMeta(String fieldOriginTransformName, IVariables vspace)
      throws HopPluginException {
    int hopType = getType();
    if (hopType == IValueMeta.TYPE_NONE) {
      hopType = IValueMeta.TYPE_STRING;
    }
    IValueMeta v =
        ValueMetaFactory.createValueMeta(
            vspace != null ? vspace.resolve(getName()) : getName(), hopType);
    v.setLength(getLength());
    v.setPrecision(getPrecision());
    v.setOrigin(fieldOriginTransformName);
    v.setConversionMask(getFormat());
    v.setDecimalSymbol(getDecimalSymbol());
    v.setGroupingSymbol(getGroupSymbol());
    v.setCurrencySymbol(getCurrencySymbol());
    v.setTrimType(getTrimType());
    return v;
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName(type);
  }

  public void setTypeWithString(String value) {
    this.type = ValueMetaFactory.getIdForValueMeta(value);
  }

  public String getTrimTypeCode() {
    return ValueMetaBase.getTrimTypeCode(trimType);
  }

  public String getTrimTypeDesc() {
    return ValueMetaBase.getTrimTypeDesc(trimType);
  }
}
