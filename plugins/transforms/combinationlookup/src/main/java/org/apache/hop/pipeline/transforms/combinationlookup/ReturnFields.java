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
 *
 */

package org.apache.hop.pipeline.transforms.combinationlookup;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class ReturnFields {
  /** Technical Key field to return */
  @HopMetadataProperty(
      key = "name",
      injectionKey = "TECHNICAL_KEY_FIELD",
      injectionKeyDescription = "CombinationLookup.Injection.TECHNICAL_KEY_FIELD")
  private String technicalKeyField;

  /** Use the auto-increment feature of the database to generate keys. */
  @HopMetadataProperty(
      key = "use_autoinc",
      injectionKey = "AUTO_INC",
      injectionKeyDescription = "CombinationLookup.Injection.AUTO_INC")
  private boolean useAutoIncrement;

  @HopMetadataProperty(
      key = "last_update_field",
      injectionKey = "LAST_UPDATE_FIELD",
      injectionKeyDescription = "CombinationLookup.Injection.LAST_UPDATE_FIELD")
  private String lastUpdateField;

  /** Which method to use for the creation of the tech key */
  @HopMetadataProperty(
      key = "creation_method",
      injectionKey = "TECHNICAL_KEY_CREATION",
      injectionKeyDescription = "CombinationLookup.Injection.TECHNICAL_KEY_CREATION")
  private String techKeyCreation = null;

  public ReturnFields() {}

  public ReturnFields(ReturnFields f) {
    this.technicalKeyField = f.technicalKeyField;
    this.useAutoIncrement = f.useAutoIncrement;
    this.lastUpdateField = f.lastUpdateField;
    this.techKeyCreation = f.techKeyCreation;
  }

  /**
   * Gets technicalKeyField
   *
   * @return value of technicalKeyField
   */
  public String getTechnicalKeyField() {
    return technicalKeyField;
  }

  /**
   * Sets technicalKeyField
   *
   * @param technicalKeyField value of technicalKeyField
   */
  public void setTechnicalKeyField(String technicalKeyField) {
    this.technicalKeyField = technicalKeyField;
  }

  /**
   * Gets useAutoinc
   *
   * @return value of useAutoinc
   */
  public boolean isUseAutoIncrement() {
    return useAutoIncrement;
  }

  /**
   * Sets useAutoinc
   *
   * @param useAutoIncrement value of useAutoinc
   */
  public void setUseAutoIncrement(boolean useAutoIncrement) {
    this.useAutoIncrement = useAutoIncrement;
  }

  /**
   * Gets lastUpdateField
   *
   * @return value of lastUpdateField
   */
  public String getLastUpdateField() {
    return lastUpdateField;
  }

  /**
   * Sets lastUpdateField
   *
   * @param lastUpdateField value of lastUpdateField
   */
  public void setLastUpdateField(String lastUpdateField) {
    this.lastUpdateField = lastUpdateField;
  }

  /**
   * Gets techKeyCreation
   *
   * @return value of techKeyCreation
   */
  public String getTechKeyCreation() {
    return techKeyCreation;
  }

  /**
   * Sets techKeyCreation
   *
   * @param techKeyCreation value of techKeyCreation
   */
  public void setTechKeyCreation(String techKeyCreation) {
    this.techKeyCreation = techKeyCreation;
  }
}
