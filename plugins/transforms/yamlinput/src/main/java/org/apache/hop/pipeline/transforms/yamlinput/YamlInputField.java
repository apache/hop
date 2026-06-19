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

package org.apache.hop.pipeline.transforms.yamlinput;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * Read YAML files, parse them and convert them to rows and writes these to one or more output
 * streams.
 */
@Getter
@Setter
public class YamlInputField implements Cloneable {
  private static final Class<?> PKG = YamlInputMeta.class;

  public static final int TYPE_TRIM_NONE = 0;
  public static final int TYPE_TRIM_LEFT = 1;
  public static final int TYPE_TRIM_RIGHT = 2;
  public static final int TYPE_TRIM_BOTH = 3;

  protected static final String[] TRIM_TYPE_DESC = {
    BaseMessages.getString(PKG, "YamlInputField.TrimType.None"),
    BaseMessages.getString(PKG, "YamlInputField.TrimType.Left"),
    BaseMessages.getString(PKG, "YamlInputField.TrimType.Right"),
    BaseMessages.getString(PKG, "YamlInputField.TrimType.Both")
  };

  @HopMetadataProperty(key = "name")
  private String name;

  @HopMetadataProperty(key = "path")
  private String path;

  @HopMetadataProperty(key = "type", intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class)
  private int type;

  @HopMetadataProperty(key = "length")
  private int length;

  @HopMetadataProperty(key = "format")
  private String format;

  @HopMetadataProperty(
      key = "trim_type",
      intCodeConverter = ValueMetaBase.TrimTypeCodeConverter.class)
  private int trimType;

  @HopMetadataProperty(key = "precision")
  private int precision;

  @HopMetadataProperty(key = "currency")
  private String currencySymbol;

  @HopMetadataProperty(key = "decimal")
  private String decimalSymbol;

  @HopMetadataProperty(key = "group")
  private String groupSymbol;

  public YamlInputField(String fieldName) {
    this.name = fieldName;
    this.path = "";
    this.length = -1;
    this.type = IValueMeta.TYPE_STRING;
    this.format = "";
    this.trimType = TYPE_TRIM_NONE;
    this.groupSymbol = "";
    this.decimalSymbol = "";
    this.currencySymbol = "";
    this.precision = -1;
  }

  public YamlInputField() {
    this("");
  }

  public YamlInputField(YamlInputField f) {
    this();
    this.name = f.name;
    this.path = f.path;
    this.type = f.type;
    this.length = f.length;
    this.format = f.format;
    this.trimType = f.trimType;
    this.precision = f.precision;
    this.currencySymbol = f.currencySymbol;
    this.decimalSymbol = f.decimalSymbol;
    this.groupSymbol = f.groupSymbol;
  }

  public static int getTrimTypeByDesc(String trimTypeDescription) {
    return ValueMetaBase.getTrimTypeByDesc(trimTypeDescription);
  }

  public static String getTrimTypeDesc(int trimType) {
    return ValueMetaBase.getTrimTypeDesc(trimType);
  }

  @Override
  public Object clone() {
    return new YamlInputField(this);
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName(type);
  }

  public String getTrimTypeDesc() {
    return getTrimTypeDesc(trimType);
  }
}
