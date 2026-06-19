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

package org.apache.hop.pipeline.transforms.xml.addxml;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Describes a single field in an XML output file */
@Getter
@Setter
public class XmlField implements Cloneable {
  @HopMetadataProperty(
      key = "name",
      injectionKey = "OUTPUT_FIELD_NAME",
      injectionKeyDescription = "AddXMLMeta.Injection.OUTPUT_FIELD_NAME")
  private String fieldName;

  @HopMetadataProperty(
      key = "element",
      injectionKey = "OUTPUT_ELEMENT_NAME",
      injectionKeyDescription = "AddXMLMeta.Injection.OUTPUT_ELEMENT_NAME")
  private String elementName;

  @HopMetadataProperty(
      key = "type",
      intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class,
      injectionKey = "OUTPUT_TYPE",
      injectionKeyDescription = "AddXMLMeta.Injection.OUTPUT_TYPE")
  private int type;

  @HopMetadataProperty(
      key = "format",
      injectionKey = "OUTPUT_FORMAT",
      injectionKeyDescription = "AddXMLMeta.Injection.OUTPUT_FORMAT")
  private String format;

  @HopMetadataProperty(
      key = "length",
      injectionKey = "OUTPUT_LENGTH",
      injectionKeyDescription = "AddXMLMeta.Injection.OUTPUT_LENGTH")
  private int length;

  @HopMetadataProperty(
      key = "precision",
      injectionKey = "OUTPUT_PRECISION",
      injectionKeyDescription = "AddXMLMeta.Injection.OUTPUT_PRECISION")
  private int precision;

  @HopMetadataProperty(
      key = "currency",
      injectionKey = "OUTPUT_CURRENCY_SYMBOL",
      injectionKeyDescription = "AddXMLMeta.Injection.OUTPUT_CURRENCY_SYMBOL")
  private String currencySymbol;

  @HopMetadataProperty(
      key = "decimal",
      injectionKey = "OUTPUT_DECIMAL_SYMBOL",
      injectionKeyDescription = "AddXMLMeta.Injection.OUTPUT_DECIMAL_SYMBOL")
  private String decimalSymbol;

  @HopMetadataProperty(
      key = "group",
      injectionKey = "OUTPUT_GROUPING_SYMBOL",
      injectionKeyDescription = "AddXMLMeta.Injection.OUTPUT_GROUPING_SYMBOL")
  private String groupingSymbol;

  @HopMetadataProperty(
      key = "attribute",
      injectionKey = "OUTPUT_ATTRIBUTE",
      injectionKeyDescription = "AddXMLMeta.Injection.OUTPUT_ATTRIBUTE")
  private boolean attribute;

  @HopMetadataProperty(
      key = "attributeParentName",
      injectionKey = "OUTPUT_ATTRIBUTE_PARENT_NAME",
      injectionKeyDescription = "AddXMLMeta.Injection.OUTPUT_ATTRIBUTE_PARENT_NAME")
  private String attributeParentName;

  @HopMetadataProperty(
      key = "nullif",
      injectionKey = "OUTPUT_NULL_STRING",
      injectionKeyDescription = "AddXMLMeta.Injection.OUTPUT_NULL_STRING")
  private String nullString;

  public XmlField() {}

  public XmlField(XmlField f) {
    this();
    this.attribute = f.attribute;
    this.attributeParentName = f.attributeParentName;
    this.currencySymbol = f.currencySymbol;
    this.decimalSymbol = f.decimalSymbol;
    this.elementName = f.elementName;
    this.fieldName = f.fieldName;
    this.format = f.format;
    this.groupingSymbol = f.groupingSymbol;
    this.length = f.length;
    this.nullString = f.nullString;
    this.precision = f.precision;
    this.type = f.type;
  }

  public int compare(Object obj) {
    XmlField field = (XmlField) obj;

    return fieldName.compareTo(field.getFieldName());
  }

  public boolean equal(Object obj) {
    XmlField field = (XmlField) obj;

    return fieldName.equals(field.getFieldName());
  }

  @Override
  public XmlField clone() {
    return new XmlField(this);
  }

  public String getTypeDesc() {
    return ValueMetaBase.getTypeDesc(type);
  }

  @Injection(name = "", group = "OUTPUT_FIELDS")
  public void setTypeWithDescription(String typeDesc) {
    this.type = ValueMetaBase.getType(typeDesc);
  }

  public String toString() {
    return fieldName + ":" + getTypeDesc() + ":" + elementName;
  }
}
