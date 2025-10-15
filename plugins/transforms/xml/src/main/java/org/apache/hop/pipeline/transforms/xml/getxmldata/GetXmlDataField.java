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

package org.apache.hop.pipeline.transforms.xml.getxmldata;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Describes an XML field and the position in an XML field. */
@Getter
@Setter
public class GetXmlDataField implements Cloneable {
  private static final Class<?> PKG = GetXmlDataMeta.class;

  public static final int RESULT_TYPE_VALUE_OF = 0;
  public static final int RESULT_TYPE_TYPE_SINGLE_NODE = 1;

  public static final String[] ResultTypeCode = {"valueof", "singlenode"};

  public static final String[] ResultTypeDesc = {
    BaseMessages.getString(PKG, "GetXMLDataField.ResultType.ValueOf"),
    BaseMessages.getString(PKG, "GetXMLDataField.ResultType.SingleNode")
  };

  public static final int TYPE_TRIM_NONE = 0;
  public static final int TYPE_TRIM_LEFT = 1;
  public static final int TYPE_TRIM_RIGHT = 2;
  public static final int TYPE_TRIM_BOTH = 3;

  public static final int ELEMENT_TYPE_NODE = 0;
  public static final int ELEMENT_TYPE_ATTRIBUTE = 1;

  public static final String[] trimTypeCode = {"none", "left", "right", "both"};

  public static final String[] trimTypeDesc = {
    BaseMessages.getString(PKG, "GetXMLDataField.TrimType.None"),
    BaseMessages.getString(PKG, "GetXMLDataField.TrimType.Left"),
    BaseMessages.getString(PKG, "GetXMLDataField.TrimType.Right"),
    BaseMessages.getString(PKG, "GetXMLDataField.TrimType.Both")
  };

  // //////////////////////////////////////////////////////////////
  //
  // Conversion to be done to go from "attribute" to "attribute"
  // - The output is written as "attribut" but both "attribut" and
  // "attribute" are accepted as input.
  // - When v3.1 is being deprecated all supported versions will
  // support "attribut" and "attribute". Then output "attribute"
  // as all version support it.
  // - In a distant future remove "attribut" all together in v5 or so.
  //
  // //////////////////////////////////////////////////////////////
  public static final String[] ElementTypeCode = {"node", "attribute"};

  public static final String[] ElementOldTypeCode = {"node", "attribut"};

  public static final String[] ElementTypeDesc = {
    BaseMessages.getString(PKG, "GetXMLDataField.ElementType.Node"),
    BaseMessages.getString(PKG, "GetXMLDataField.ElementType.Attribute")
  };

  @HopMetadataProperty(injectionKeyDescription = "GetXmlDataMeta.Injection.FieldName")
  private String name;

  @HopMetadataProperty(injectionKeyDescription = "GetXmlDataMeta.Injection.XPath")
  private String xPath;

  private String resolvedXpath;

  @HopMetadataProperty(injectionKeyDescription = "GetXmlDataMeta.Injection.Type")
  private String type;

  @HopMetadataProperty(injectionKeyDescription = "GetXmlDataMeta.Injection.Length")
  private int length;

  @HopMetadataProperty(injectionKeyDescription = "GetXmlDataMeta.Injection.Format")
  private String format;

  @HopMetadataProperty(
      key = "trim_type",
      injectionKeyDescription = "GetXmlDataMeta.Injection.TrimType")
  private String trimType;

  @HopMetadataProperty(
      key = "element_type",
      injectionKeyDescription = "GetXmlDataMeta.Injection.ElementType")
  private String elementType;

  @HopMetadataProperty(
      key = "result_type",
      injectionKeyDescription = "GetXmlDataMeta.Injection.ResultType")
  private String resultType;

  @HopMetadataProperty(injectionKeyDescription = "GetXmlDataMeta.Injection.Precision")
  private int precision;

  @HopMetadataProperty(
      key = "currency",
      injectionKeyDescription = "GetXmlDataMeta.Injection.CurrencySymbol")
  private String currencySymbol;

  @HopMetadataProperty(
      key = "decimal",
      injectionKeyDescription = "GetXmlDataMeta.Injection.DecimalSymbol")
  private String decimalSymbol;

  @HopMetadataProperty(
      key = "group",
      injectionKeyDescription = "GetXmlDataMeta.Injection.GroupSymbol")
  private String groupSymbol;

  @HopMetadataProperty(injectionKeyDescription = "GetXmlDataMeta.Injection.Repeat")
  private boolean repeat;

  public GetXmlDataField(String fieldname) {
    this.name = fieldname;
    this.xPath = "";
    this.length = -1;
    this.type = ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING);
    this.format = "";
    this.trimType = getTrimTypeCode(TYPE_TRIM_NONE);
    this.elementType = getElementTypeDesc(ELEMENT_TYPE_NODE);
    this.resultType = getResultTypeCode(RESULT_TYPE_VALUE_OF);
    this.groupSymbol = "";
    this.decimalSymbol = "";
    this.currencySymbol = "";
    this.precision = -1;
    this.repeat = false;
  }

  public GetXmlDataField() {
    this("");
  }

  public static int getTrimTypeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < trimTypeCode.length; i++) {
      if (trimTypeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public static int getElementTypeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    // / Code to be removed later on as explained in the top of
    // this file.
    // //////////////////////////////////////////////////////////////
    for (int i = 0; i < ElementOldTypeCode.length; i++) {
      if (ElementOldTypeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    // //////////////////////////////////////////////////////////////

    for (int i = 0; i < ElementTypeCode.length; i++) {
      if (ElementTypeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    return 0;
  }

  public static int getTrimTypeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < trimTypeDesc.length; i++) {
      if (trimTypeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public static int getElementTypeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < ElementTypeDesc.length; i++) {
      if (ElementTypeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public static String getTrimTypeCode(int i) {
    if (i < 0 || i >= trimTypeCode.length) {
      return trimTypeCode[0];
    }
    return trimTypeCode[i];
  }

  public static String getElementTypeCode(int i) {
    // To be changed to the new code once all are converted
    if (i < 0 || i >= ElementOldTypeCode.length) {
      return ElementOldTypeCode[0];
    }
    return ElementOldTypeCode[i];
  }

  public static String getTrimTypeDesc(int i) {
    if (i < 0 || i >= trimTypeDesc.length) {
      return trimTypeDesc[0];
    }
    return trimTypeDesc[i];
  }

  public static String getElementTypeDesc(int i) {
    if (i < 0 || i >= ElementTypeDesc.length) {
      return ElementTypeDesc[0];
    }
    return ElementTypeDesc[i];
  }

  protected String getResolvedXPath() {
    return resolvedXpath;
  }

  protected void setResolvedXPath(String resolvedXpath) {
    this.resolvedXpath = resolvedXpath;
  }

  public String getTypeDesc() {
    return type;
  }

  public String getTrimTypeCode() {
    return trimType;
  }

  public String getElementTypeCode() {
    return elementType;
  }

  public String getTrimTypeDesc() {
    return getTrimTypeDesc(getTrimTypeByCode(trimType));
  }

  public String getElementTypeDesc() {
    return getElementTypeDesc(getElementTypeByCode(elementType));
  }

  public void flipRepeated() {
    repeat = !repeat;
  }

  public static final int getResultTypeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < ResultTypeDesc.length; i++) {
      if (ResultTypeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public String getResultTypeDesc() {
    return getResultTypeDesc(getResultTypeByCode(resultType));
  }

  public static final String getResultTypeDesc(int i) {
    if (i < 0 || i >= ResultTypeDesc.length) {
      return ResultTypeDesc[0];
    }
    return ResultTypeDesc[i];
  }

  public static final int getResultTypeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < ResultTypeCode.length; i++) {
      if (ResultTypeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    return 0;
  }

  public static final String getResultTypeCode(int i) {
    if (i < 0 || i >= ResultTypeCode.length) {
      return ResultTypeCode[0];
    }
    return ResultTypeCode[i];
  }

  @Override
  public Object clone() {
    try {
      GetXmlDataField retval = (GetXmlDataField) super.clone();
      return retval;
    } catch (CloneNotSupportedException e) {
      return null;
    }
  }
}
