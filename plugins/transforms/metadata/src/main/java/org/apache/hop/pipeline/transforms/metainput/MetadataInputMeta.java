/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.metainput;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Transform(
    id = "MetadataInput",
    name = "i18n::MetadataInput.Transform.Name",
    description = "i18n::MetadataInput.Transform.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    image = "metadata.svg",
    documentationUrl = "/pipeline/transforms/metadata-input.html")
public class MetadataInputMeta extends BaseTransformMeta<MetadataInput, MetadataInputData> {

  @HopMetadataProperty private String providerFieldName;
  @HopMetadataProperty private String typeKeyFieldName;
  @HopMetadataProperty private String typeNameFieldName;
  @HopMetadataProperty private String typeDescriptionFieldName;
  @HopMetadataProperty private String typeClassFieldName;
  @HopMetadataProperty private String nameFieldName;
  @HopMetadataProperty private String jsonFieldName;
  @HopMetadataProperty private List<String> typeKeyFilters;

  public MetadataInputMeta() {
    this.providerFieldName = "provider";
    this.typeKeyFieldName = "typeKey";
    this.typeNameFieldName = "typeName";
    this.typeDescriptionFieldName = "typeDescription";
    this.typeClassFieldName = "typeClass";
    this.nameFieldName = "name";
    this.jsonFieldName = "json";
    this.typeKeyFilters = new ArrayList<>();
  }

  public MetadataInputMeta(MetadataInputMeta meta) {
    this.providerFieldName = meta.providerFieldName;
    this.typeKeyFieldName = meta.typeKeyFieldName;
    this.typeNameFieldName = meta.typeNameFieldName;
    this.typeDescriptionFieldName = meta.typeDescriptionFieldName;
    this.typeClassFieldName = meta.typeClassFieldName;
    this.nameFieldName = meta.nameFieldName;
    this.jsonFieldName = meta.jsonFieldName;
    this.typeKeyFilters = new ArrayList<>(meta.typeKeyFilters);
  }

  @Override
  public MetadataInputMeta clone() {
    return new MetadataInputMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    // The output fields (all type String) to consider...
    //
    List<String> fieldNames =
        Arrays.asList(
            providerFieldName,
            typeKeyFieldName,
            typeNameFieldName,
            typeDescriptionFieldName,
            typeClassFieldName,
            nameFieldName,
            jsonFieldName);
    RowMetaBuilder builder = new RowMetaBuilder();
    for (String fieldName : fieldNames) {
      if (StringUtils.isNotEmpty(fieldName)) {
        builder.addString(fieldName);
      }
    }

    inputRowMeta.addRowMeta(builder.build());
  }

  /**
   * Gets providerFieldName
   *
   * @return value of providerFieldName
   */
  public String getProviderFieldName() {
    return providerFieldName;
  }

  /**
   * @param providerFieldName The providerFieldName to set
   */
  public void setProviderFieldName(String providerFieldName) {
    this.providerFieldName = providerFieldName;
  }

  /**
   * Gets typeKeyFieldName
   *
   * @return value of typeKeyFieldName
   */
  public String getTypeKeyFieldName() {
    return typeKeyFieldName;
  }

  /**
   * @param typeKeyFieldName The typeKeyFieldName to set
   */
  public void setTypeKeyFieldName(String typeKeyFieldName) {
    this.typeKeyFieldName = typeKeyFieldName;
  }

  /**
   * Gets typeNameFieldName
   *
   * @return value of typeNameFieldName
   */
  public String getTypeNameFieldName() {
    return typeNameFieldName;
  }

  /**
   * @param typeNameFieldName The typeNameFieldName to set
   */
  public void setTypeNameFieldName(String typeNameFieldName) {
    this.typeNameFieldName = typeNameFieldName;
  }

  /**
   * Gets typeDescriptionFieldName
   *
   * @return value of typeDescriptionFieldName
   */
  public String getTypeDescriptionFieldName() {
    return typeDescriptionFieldName;
  }

  /**
   * @param typeDescriptionFieldName The typeDescriptionFieldName to set
   */
  public void setTypeDescriptionFieldName(String typeDescriptionFieldName) {
    this.typeDescriptionFieldName = typeDescriptionFieldName;
  }

  /**
   * Gets typeClassFieldName
   *
   * @return value of typeClassFieldName
   */
  public String getTypeClassFieldName() {
    return typeClassFieldName;
  }

  /**
   * @param typeClassFieldName The typeClassFieldName to set
   */
  public void setTypeClassFieldName(String typeClassFieldName) {
    this.typeClassFieldName = typeClassFieldName;
  }

  /**
   * Gets nameFieldName
   *
   * @return value of nameFieldName
   */
  public String getNameFieldName() {
    return nameFieldName;
  }

  /**
   * @param nameFieldName The nameFieldName to set
   */
  public void setNameFieldName(String nameFieldName) {
    this.nameFieldName = nameFieldName;
  }

  /**
   * Gets jsonFieldName
   *
   * @return value of jsonFieldName
   */
  public String getJsonFieldName() {
    return jsonFieldName;
  }

  /**
   * @param jsonFieldName The jsonFieldName to set
   */
  public void setJsonFieldName(String jsonFieldName) {
    this.jsonFieldName = jsonFieldName;
  }

  /**
   * Gets typeKeyFilters
   *
   * @return value of typeKeyFilters
   */
  public List<String> getTypeKeyFilters() {
    return typeKeyFilters;
  }

  /**
   * @param typeKeyFilters The typeKeyFilters to set
   */
  public void setTypeKeyFilters(List<String> typeKeyFilters) {
    this.typeKeyFilters = typeKeyFilters;
  }
}
