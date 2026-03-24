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

package org.apache.hop.pipeline.transforms.loadfileinput;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/** Describes a field */
@Getter
@Setter
public class LoadFileInputField implements Cloneable {
  private static final Class<?> PKG = LoadFileInputMeta.class;

  public static final int TYPE_TRIM_NONE = 0;
  public static final int TYPE_TRIM_LEFT = 1;
  public static final int TYPE_TRIM_RIGHT = 2;
  public static final int TYPE_TRIM_BOTH = 3;
  private static final String CONST_SPACE = "        ";

  public IValueMeta createValueMeta(IVariables variables) throws HopPluginException {
    int correctedType = getType();

    switch (getElementType()) {
      case LoadFileInputField.ElementType.FILE_CONTENT:
        if (correctedType == IValueMeta.TYPE_NONE) {
          correctedType = IValueMeta.TYPE_STRING;
        }
        break;
      case LoadFileInputField.ElementType.FILE_SIZE:
        if (correctedType == IValueMeta.TYPE_NONE) {
          correctedType = IValueMeta.TYPE_INTEGER;
        }
        break;
      default:
        break;
    }

    IValueMeta v = ValueMetaFactory.createValueMeta(variables.resolve(getName()), correctedType);
    v.setLength(getLength());
    v.setPrecision(getPrecision());
    v.setConversionMask(getFormat());
    v.setCurrencySymbol(getCurrencySymbol());
    v.setDecimalSymbol(getDecimalSymbol());
    v.setGroupingSymbol(getGroupSymbol());
    v.setTrimType(getTrimType());
    return v;
  }

  @Getter
  public enum ElementType implements IEnumHasCodeAndDescription {
    FILE_CONTENT(
        "content", BaseMessages.getString(PKG, "LoadFileInputField.ElementType.FileContent")),
    FILE_SIZE("size", BaseMessages.getString(PKG, "LoadFileInputField.ElementType.FileSize")),
    ;
    private final String code;
    private final String description;

    ElementType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static ElementType lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          ElementType.class, description, FILE_CONTENT);
    }

    public static ElementType lookupCode(String code) {
      return IEnumHasCode.lookupCode(ElementType.class, code, FILE_CONTENT);
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(ElementType.class);
    }
  }

  @HopMetadataProperty(
      key = "name",
      injectionKey = "FIELD_NAME",
      injectionKeyDescription = "LoadFileInputMeta.Injection.FIELD_NAME")
  private String name;

  @HopMetadataProperty(
      key = "type",
      intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class,
      injectionKey = "FIELD_TYPE",
      injectionKeyDescription = "LoadFileInputMeta.Injection.FIELD_TYPE")
  private int type;

  @HopMetadataProperty(
      key = "length",
      injectionKey = "FIELD_LENGTH",
      injectionKeyDescription = "LoadFileInputMeta.Injection.FIELD_LENGTH")
  private int length;

  @HopMetadataProperty(
      key = "format",
      injectionKey = "FIELD_FORMAT",
      injectionKeyDescription = "LoadFileInputMeta.Injection.FIELD_FORMAT")
  private String format;

  @HopMetadataProperty(
      key = "trim_type",
      intCodeConverter = ValueMetaBase.TrimTypeCodeConverter.class,
      injectionKey = "FIELD_TRIM_TYPE",
      injectionKeyDescription = "LoadFileInputMeta.Injection.FIELD_TRIM_TYPE")
  private int trimType;

  @HopMetadataProperty(
      key = "element_type",
      storeWithCode = true,
      injectionKey = "FIELD_ELEMENT_TYPE",
      injectionKeyDescription = "LoadFileInputMeta.Injection.FIELD_ELEMENT_TYPE")
  private ElementType elementType;

  @HopMetadataProperty(
      key = "precision",
      injectionKey = "FIELD_PRECISION",
      injectionKeyDescription = "LoadFileInputMeta.Injection.FIELD_PRECISION")
  private int precision;

  @HopMetadataProperty(
      key = "currency",
      injectionKey = "FIELD_CURRENCY",
      injectionKeyDescription = "LoadFileInputMeta.Injection.FIELD_CURRENCY")
  private String currencySymbol;

  @HopMetadataProperty(
      key = "decimal",
      injectionKey = "FIELD_DECIMAL",
      injectionKeyDescription = "LoadFileInputMeta.Injection.FIELD_DECIMAL")
  private String decimalSymbol;

  @HopMetadataProperty(
      key = "group",
      injectionKey = "FIELD_GROUP",
      injectionKeyDescription = "LoadFileInputMeta.Injection.FIELD_GROUP")
  private String groupSymbol;

  @HopMetadataProperty(
      key = "repeat",
      injectionKey = "FIELD_REPEAT",
      injectionKeyDescription = "LoadFileInputMeta.Injection.FIELD_REPEAT")
  private boolean repeated;

  public LoadFileInputField() {
    this("");
  }

  public LoadFileInputField(String name) {
    this.name = name;
    this.elementType = ElementType.FILE_CONTENT;
    this.length = -1;
    this.type = IValueMeta.TYPE_STRING;
    this.format = "";
    this.trimType = TYPE_TRIM_NONE;
    this.groupSymbol = "";
    this.decimalSymbol = "";
    this.currencySymbol = "";
    this.precision = -1;
    this.repeated = false;
  }

  public LoadFileInputField(LoadFileInputField f) {
    this();
    this.name = f.name;
    this.elementType = f.elementType;
    this.type = f.type;
    this.length = f.length;
    this.precision = f.precision;
    this.format = f.format;
    this.decimalSymbol = f.decimalSymbol;
    this.groupSymbol = f.groupSymbol;
    this.currencySymbol = f.currencySymbol;
    this.trimType = f.trimType;
    this.repeated = f.repeated;
  }

  public static int getTrimTypeByCode(String code) {
    return ValueMetaBase.getTrimTypeByCode(code);
  }

  public static ElementType getElementTypeByCode(String code) {
    return ElementType.lookupCode(code);
  }

  public static int getTrimTypeByDesc(String description) {
    return ValueMetaBase.getTrimTypeByDesc(description);
  }

  public static ElementType getElementTypeByDesc(String description) {
    return ElementType.lookupDescription(description);
  }

  public static String getTrimTypeCode(int trimType) {
    return ValueMetaBase.getTrimTypeCode(trimType);
  }

  public static String getElementTypeCode(ElementType elementType) {
    return elementType.getCode();
  }

  public static String getTrimTypeDesc(int trimType) {
    return ValueMetaBase.getTrimTypeDesc(trimType);
  }

  public static String getElementTypeDesc(ElementType elementType) {
    return elementType.getDescription();
  }

  @Override
  public Object clone() {
    return new LoadFileInputField(this);
  }

  public void guess() {
    // Do nothing
  }
}
