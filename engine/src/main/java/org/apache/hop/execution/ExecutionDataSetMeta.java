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

package org.apache.hop.execution;

import org.apache.hop.core.Const;

public class ExecutionDataSetMeta {
  private String setKey;
  private String logChannelId;
  private String name;
  private String fieldName;
  private String sampleDescription;
  private String copyNr;
  private String description;

  public ExecutionDataSetMeta() {}

  public ExecutionDataSetMeta(
      String setKey, String logChannelId, String name, String copyNr, String description) {
    this.setKey = setKey;
    this.logChannelId = logChannelId;
    this.name = name;
    this.copyNr = copyNr;
    this.description = description;
  }

  public ExecutionDataSetMeta(
      String setKey,
      String logChannelId,
      String name,
      String copyNr,
      String fieldName,
      String sampleDescription,
      String description) {
    this.setKey = setKey;
    this.logChannelId = logChannelId;
    this.name = name;
    this.copyNr = copyNr;
    this.fieldName = fieldName;
    this.sampleDescription = sampleDescription;
    this.description = description;
  }

  @Override
  public String toString() {
    return "ExecutionDataSetMeta{" + Const.CR +
            "  setKey='" + setKey + '\'' + Const.CR +
            ", logChannelId='" + logChannelId + '\'' + Const.CR +
            ", name='" + name + '\'' + Const.CR +
            ", fieldName='" + fieldName + '\'' + Const.CR +
            ", sampleDescription='" + sampleDescription + '\'' + Const.CR +
            ", copyNr='" + copyNr + '\'' + Const.CR +
            ", description='" + description + '\'' + Const.CR +
            '}';
  }

  /**
   * Gets setKey
   *
   * @return value of setKey
   */
  public String getSetKey() {
    return setKey;
  }

  /**
   * Sets setKey
   *
   * @param setKey value of setKey
   */
  public void setSetKey(String setKey) {
    this.setKey = setKey;
  }

  /**
   * Gets logChannelId
   *
   * @return value of logChannelId
   */
  public String getLogChannelId() {
    return logChannelId;
  }

  /**
   * Sets logChannelId
   *
   * @param logChannelId value of logChannelId
   */
  public void setLogChannelId(String logChannelId) {
    this.logChannelId = logChannelId;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * Sets name
   *
   * @param name value of name
   */
  public void setName(String name) {
    this.name = name;
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
   * Sets fieldName
   *
   * @param fieldName value of fieldName
   */
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  /**
   * Gets sampleDescription
   *
   * @return value of sampleDescription
   */
  public String getSampleDescription() {
    return sampleDescription;
  }

  /**
   * Sets sampleDescription
   *
   * @param sampleDescription value of sampleDescription
   */
  public void setSampleDescription(String sampleDescription) {
    this.sampleDescription = sampleDescription;
  }

  /**
   * Gets copyNr
   *
   * @return value of copyNr
   */
  public String getCopyNr() {
    return copyNr;
  }

  /**
   * Sets copyNr
   *
   * @param copyNr value of copyNr
   */
  public void setCopyNr(String copyNr) {
    this.copyNr = copyNr;
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
   * Sets description
   *
   * @param description value of description
   */
  public void setDescription(String description) {
    this.description = description;
  }
}
