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
 *
 */

package org.apache.hop.www.service;

import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
    key = "web-service",
    name = "i18n::WebService.name",
    description = "i18n::WebService.description",
    image = "ui/images/webservice.svg",
    documentationUrl = "/metadata-types/web-service.html",
    hopMetadataPropertyType = HopMetadataPropertyType.SERVER_WEB_SERVICE)
public class WebService extends HopMetadataBase implements IHopMetadata {

  @HopMetadataProperty private boolean enabled;
  @HopMetadataProperty private String filename;
  @HopMetadataProperty private String transformName;
  @HopMetadataProperty private String fieldName;
  @HopMetadataProperty private String contentType;
  @HopMetadataProperty private boolean listingStatus;
  @HopMetadataProperty private String bodyContentVariable;
  @HopMetadataProperty private String runConfigurationName;

  public WebService() {}

  public WebService(
      String name,
      boolean enabled,
      String filename,
      String transformName,
      String fieldName,
      String contentType,
      boolean listingStatus,
      String bodyContentVariable,
      String runConfigurationName) {
    super(name);
    this.enabled = enabled;
    this.filename = filename;
    this.transformName = transformName;
    this.fieldName = fieldName;
    this.contentType = contentType;
    this.listingStatus = listingStatus;
    this.bodyContentVariable = bodyContentVariable;
    this.runConfigurationName = runConfigurationName;
  }

  /**
   * Gets enabled
   *
   * @return value of enabled
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * @param enabled The enabled to set
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename The filename to set
   */
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /**
   * Gets transformName
   *
   * @return value of transformName
   */
  public String getTransformName() {
    return transformName;
  }

  /**
   * @param transformName The transformName to set
   */
  public void setTransformName(String transformName) {
    this.transformName = transformName;
  }

  /**
   * Gets fieldName
   *
   * @return value of fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName The fieldName to set
   */
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  /**
   * Gets contentType
   *
   * @return value of contentType
   */
  public String getContentType() {
    return contentType;
  }

  /**
   * @param contentType The contentType to set
   */
  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  /**
   * Gets listingStatus
   *
   * @return value of listingStatus
   */
  public boolean isListingStatus() {
    return listingStatus;
  }

  /**
   * @param listingStatus The listingStatus to set
   */
  public void setListingStatus(boolean listingStatus) {
    this.listingStatus = listingStatus;
  }

  /**
   * Gets bodyContentVariable
   *
   * @return value of bodyContentVariable
   */
  public String getBodyContentVariable() {
    return bodyContentVariable;
  }

  /**
   * Sets bodyContentVariable
   *
   * @param bodyContentVariable value of bodyContentVariable
   */
  public void setBodyContentVariable(String bodyContentVariable) {
    this.bodyContentVariable = bodyContentVariable;
  }

  /**
   * Gets runConfigurationName
   *
   * @return value of runConfigurationName
   */
  public String getRunConfigurationName() {
    return runConfigurationName;
  }

  /**
   * Sets runConfigurationName
   *
   * @param runConfigurationName value of runConfigurationName
   */
  public void setRunConfigurationName(String runConfigurationName) {
    this.runConfigurationName = runConfigurationName;
  }
}
