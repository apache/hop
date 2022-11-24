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

package org.apache.hop.pipeline.transforms.concatfields;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class ExtraFields {
  /** The target field name */
  @HopMetadataProperty(key = "targetFieldName")
  private String targetFieldName;

  /** The length of the string field */
  @HopMetadataProperty(key = "targetFieldLength")
  private int targetFieldLength;

  /** Remove the selected fields in the output stream */
  @HopMetadataProperty(key = "removeSelectedFields")
  private boolean removeSelectedFields;

  public ExtraFields() {}

  public ExtraFields(ExtraFields f) {
    this.targetFieldName = f.targetFieldName;
    this.targetFieldLength = f.targetFieldLength;
    this.removeSelectedFields = f.removeSelectedFields;
  }

  /**
   * Gets targetFieldName
   *
   * @return value of targetFieldName
   */
  public String getTargetFieldName() {
    return targetFieldName;
  }

  /**
   * Sets targetFieldName
   *
   * @param targetFieldName value of targetFieldName
   */
  public void setTargetFieldName(String targetFieldName) {
    this.targetFieldName = targetFieldName;
  }

  /**
   * Gets targetFieldLength
   *
   * @return value of targetFieldLength
   */
  public int getTargetFieldLength() {
    return targetFieldLength;
  }

  /**
   * Sets targetFieldLength
   *
   * @param targetFieldLength value of targetFieldLength
   */
  public void setTargetFieldLength(int targetFieldLength) {
    this.targetFieldLength = targetFieldLength;
  }

  /**
   * Gets removeSelectedFields
   *
   * @return value of removeSelectedFields
   */
  public boolean isRemoveSelectedFields() {
    return removeSelectedFields;
  }

  /**
   * Sets removeSelectedFields
   *
   * @param removeSelectedFields value of removeSelectedFields
   */
  public void setRemoveSelectedFields(boolean removeSelectedFields) {
    this.removeSelectedFields = removeSelectedFields;
  }
}
