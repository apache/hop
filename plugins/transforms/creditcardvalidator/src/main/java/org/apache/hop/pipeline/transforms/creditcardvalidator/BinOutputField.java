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

package org.apache.hop.pipeline.transforms.creditcardvalidator;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Describes an output field mapped from a CSV column of the BIN file. */
@Getter
@Setter
public class BinOutputField {

  @HopMetadataProperty(key = "name")
  private String name;

  @HopMetadataProperty(key = "column")
  private String column;

  @HopMetadataProperty(key = "type")
  private int type = IValueMeta.TYPE_STRING;

  @HopMetadataProperty(key = "format")
  private String format;

  @HopMetadataProperty(key = "length")
  private int length = -1;

  @HopMetadataProperty(key = "precision")
  private int precision = -1;

  @HopMetadataProperty(key = "currency")
  private String currency;

  @HopMetadataProperty(key = "decimal")
  private String decimal;

  @HopMetadataProperty(key = "group")
  private String group;

  @HopMetadataProperty(key = "trim_type")
  private int trimType = IValueMeta.TRIM_TYPE_NONE;

  public BinOutputField() {}

  public BinOutputField(String name, String column) {
    this.name = name;
    this.column = column;
  }

  public BinOutputField(BinOutputField other) {
    this.name = other.name;
    this.column = other.column;
    this.type = other.type;
    this.format = other.format;
    this.length = other.length;
    this.precision = other.precision;
    this.currency = other.currency;
    this.decimal = other.decimal;
    this.group = other.group;
    this.trimType = other.trimType;
  }

  @Override
  public BinOutputField clone() {
    return new BinOutputField(this);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BinOutputField that)) {
      return false;
    }
    return Objects.equals(name, that.name) && Objects.equals(column, that.column);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, column);
  }

  /**
   * Build the value metadata for this output field, copying type, length, precision and symbols.
   */
  public IValueMeta createValueMeta(String fieldName) throws HopPluginException {
    IValueMeta valueMeta = ValueMetaFactory.createValueMeta(fieldName, type, length, precision);
    valueMeta.setConversionMask(format);
    valueMeta.setDecimalSymbol(decimal);
    valueMeta.setGroupingSymbol(group);
    valueMeta.setCurrencySymbol(currency);
    valueMeta.setTrimType(trimType);
    return valueMeta;
  }
}
