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

package org.apache.hop.pipeline.transforms.xml.xmloutput;

import com.google.common.base.Enums;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Describes a single field in an XML output file */
@Getter
@Setter
public class XmlField implements Cloneable {
  @SuppressWarnings("java:S115")
  public enum ContentType {
    Element,
    Attribute;

    /**
     * Ensuring that this enum can return with some default value. Necessary for when being used on
     * the GUI side if a user leaves the field empty, it will enforce a default value. This allows
     * the object to be saved, loaded, and cloned as necessary.
     *
     * @param contentType The content type name
     * @return ContentType
     */
    public static ContentType getIfPresent(String contentType) {
      return Enums.getIfPresent(ContentType.class, contentType).or(Element);
    }
  }

  @HopMetadataProperty(
      key = "name",
      injectionKey = "OUTPUT_FIELDNAME",
      injectionKeyDescription = "XMLOutput.Injection.OUTPUT_FIELDNAME")
  private String fieldName;

  @HopMetadataProperty(
      key = "element",
      injectionKey = "OUTPUT_ELEMENTNAME",
      injectionKeyDescription = "XMLOutput.Injection.OUTPUT_ELEMENTNAME")
  private String elementName;

  @HopMetadataProperty(
      key = "type",
      intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class,
      injectionKey = "OUTPUT_TYPE",
      injectionKeyDescription = "XMLOutput.Injection.OUTPUT_TYPE")
  private int type;

  @HopMetadataProperty(
      key = "format",
      injectionKey = "OUTPUT_FORMAT",
      injectionKeyDescription = "XMLOutput.Injection.OUTPUT_FORMAT")
  private String format;

  @HopMetadataProperty(
      key = "length",
      injectionKey = "OUTPUT_LENGTH",
      injectionKeyDescription = "XMLOutput.Injection.OUTPUT_LENGTH")
  private int length;

  @HopMetadataProperty(
      key = "precision",
      injectionKey = "OUTPUT_PRECISION",
      injectionKeyDescription = "XMLOutput.Injection.OUTPUT_PRECISION")
  private int precision;

  @HopMetadataProperty(
      key = "currency",
      injectionKey = "OUTPUT_CURRENCY",
      injectionKeyDescription = "XMLOutput.Injection.OUTPUT_CURRENCY")
  private String currencySymbol;

  @HopMetadataProperty(
      key = "decimal",
      injectionKey = "OUTPUT_DECIMAL",
      injectionKeyDescription = "XMLOutput.Injection.OUTPUT_DECIMAL")
  private String decimalSymbol;

  @HopMetadataProperty(
      key = "group",
      injectionKey = "OUTPUT_GROUP",
      injectionKeyDescription = "XMLOutput.Injection.OUTPUT_GROUP")
  private String groupingSymbol;

  @HopMetadataProperty(
      key = "nullif",
      injectionKey = "OUTPUT_NULL",
      injectionKeyDescription = "XMLOutput.Injection.OUTPUT_NULL")
  private String nullString;

  @HopMetadataProperty(
      key = "content_type",
      injectionKey = "OUTPUT_CONTENT_TYPE",
      injectionKeyDescription = "XMLOutput.Injection.OUTPUT_CONTENT_TYPE")
  private ContentType contentType;

  public XmlField(
      ContentType contentType,
      String fieldName,
      String elementName,
      int type,
      int length,
      int precision) {
    this();
    this.contentType = contentType;
    this.fieldName = fieldName;
    this.elementName = elementName;
    this.type = type;
    this.length = length;
    this.precision = precision;
  }

  public XmlField() {
    contentType = ContentType.Element;
  }

  public XmlField(XmlField f) {
    this.contentType = f.contentType;
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

  @Override
  public Object clone() {
    return new XmlField(this);
  }

  public int compare(Object obj) {
    XmlField field = (XmlField) obj;
    return fieldName.compareTo(field.getFieldName());
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof XmlField xmlField)) {
      return false;
    }
    return Objects.equals(fieldName, xmlField.fieldName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fieldName);
  }

  public String getTypeDesc() {
    return ValueMetaBase.getTypeDesc(type);
  }

  public void setTypeWithDescription(String typeDesc) {
    this.type = ValueMetaBase.getType(typeDesc);
  }

  public String toString() {
    return fieldName + ":" + getTypeDesc() + ":" + elementName;
  }
}
