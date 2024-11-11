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

package org.apache.hop.staticschema.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
    key = "schema-definition",
    name = "i18n::SchemaDefinition.Name",
    description = "i18n::SchemaDefinition.Description",
    image = "ui/images/folder.svg",
    documentationUrl = "/metadata-types/static-schema-definition.html",
    hopMetadataPropertyType = HopMetadataPropertyType.STATIC_SCHEMA_DEFINITION)
public class SchemaDefinition extends HopMetadataBase implements Serializable, IHopMetadata {

  private static final Class<?> PKG = SchemaDefinition.class;
  @HopMetadataProperty private String description;

  @HopMetadataProperty private List<SchemaFieldDefinition> fieldDefinitions;

  @HopMetadataProperty private String separator;

  @HopMetadataProperty private String enclosure;

  public SchemaDefinition() {
    fieldDefinitions = new ArrayList<>();
  }

  public SchemaDefinition(
      String name,
      String description,
      List<SchemaFieldDefinition> fieldDefinitions,
      String separator,
      String enclosure) {
    this.name = name;
    this.description = description;
    this.fieldDefinitions = fieldDefinitions;
    this.separator = separator;
    this.enclosure = enclosure;
  }

  public IRowMeta getRowMeta() throws HopPluginException {
    IRowMeta rowMeta = new RowMeta();
    for (SchemaFieldDefinition fieldDefinition : fieldDefinitions) {
      rowMeta.addValueMeta(fieldDefinition.getValueMeta());
    }
    return rowMeta;
  }

  public void validate() throws HopException {
    //    if (StringUtils.isEmpty(separator)) {
    //      throw new HopException("Please specify a separator in file definition "+name);
    //    }
    for (IValueMeta valueMeta : getRowMeta().getValueMetaList()) {
      if (StringUtils.isEmpty(valueMeta.getName())) {
        throw new HopException(
            "Schema definition " + name + " should not contain fields without a name");
      }
      if (valueMeta.getType() == IValueMeta.TYPE_NONE) {
        throw new HopException(
            "Schema definition " + name + " should not contain fields without a type");
      }
    }
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Gets fieldDefinitions
   *
   * @return value of fieldDefinitions
   */
  public List<SchemaFieldDefinition> getFieldDefinitions() {
    return fieldDefinitions;
  }

  /**
   * @param fieldDefinitions The fieldDefinitions to set
   */
  public void setFieldDefinitions(List<SchemaFieldDefinition> fieldDefinitions) {
    this.fieldDefinitions = fieldDefinitions;
  }

  /**
   * Gets separator
   *
   * @return value of separator
   */
  public String getSeparator() {
    return separator;
  }

  /**
   * @param separator The separator to set
   */
  public void setSeparator(String separator) {
    this.separator = separator;
  }

  /**
   * Gets enclosure
   *
   * @return value of enclosure
   */
  public String getEnclosure() {
    return enclosure;
  }

  /**
   * @param enclosure The enclosure to set
   */
  public void setEnclosure(String enclosure) {
    this.enclosure = enclosure;
  }
}
