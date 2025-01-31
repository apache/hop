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

package org.apache.hop.pipeline.transforms.concatfields;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Describes a single field in a text file */
@Getter
@Setter
public class ConcatField implements Cloneable {
  @HopMetadataProperty(key = "name", injectionKey = "OUTPUT_FIELDNAME")
  private String name;

  @HopMetadataProperty(key = "type", injectionKey = "OUTPUT_TYPE")
  private String type;

  @HopMetadataProperty(key = "format", injectionKey = "OUTPUT_FORMAT")
  private String format;

  @HopMetadataProperty(key = "length", injectionKey = "OUTPUT_LENGTH")
  private int length = -1;

  @HopMetadataProperty(key = "precision", injectionKey = "OUTPUT_PRECISION")
  private int precision = -1;

  @HopMetadataProperty(key = "currency", injectionKey = "OUTPUT_CURRENCY")
  private String currencySymbol;

  @HopMetadataProperty(key = "decimal", injectionKey = "OUTPUT_DECIMAL")
  private String decimalSymbol;

  @HopMetadataProperty(key = "group", injectionKey = "OUTPUT_GROUP")
  private String groupingSymbol;

  @HopMetadataProperty(key = "nullif", injectionKey = "OUTPUT_NULL")
  private String nullString;

  @HopMetadataProperty(key = "trim_type", injectionKey = "OUTPUT_TRIM")
  private String trimType;

  public ConcatField() {}

  public ConcatField(ConcatField f) {
    this.name = f.name;
    this.type = f.type;
    this.format = f.format;
    this.length = f.length;
    this.precision = f.precision;
    this.currencySymbol = f.currencySymbol;
    this.decimalSymbol = f.decimalSymbol;
    this.groupingSymbol = f.groupingSymbol;
    this.nullString = f.nullString;
    this.trimType = f.trimType;
  }

  public ConcatField clone() {
    return new ConcatField(this);
  }

  public int compare(Object obj) {
    ConcatField field = (ConcatField) obj;
    return name.compareTo(field.getName());
  }

  public boolean equal(Object obj) {
    ConcatField field = (ConcatField) obj;
    return name.equals(field.getName());
  }
}
