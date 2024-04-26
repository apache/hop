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

package org.apache.hop.parquet.transforms.output;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class ParquetField {
  @HopMetadataProperty(key = "source_field")
  private String sourceFieldName;

  @HopMetadataProperty(key = "target_field")
  private String targetFieldName;

  public ParquetField() {}

  public ParquetField(String sourceFieldName, String targetFieldName) {
    this.sourceFieldName = sourceFieldName;
    this.targetFieldName = targetFieldName;
  }

  public ParquetField(ParquetField f) {
    this(f.sourceFieldName, f.targetFieldName);
  }

  /**
   * Gets sourceFieldName
   *
   * @return value of sourceFieldName
   */
  public String getSourceFieldName() {
    return sourceFieldName;
  }

  /**
   * @param sourceFieldName The sourceFieldName to set
   */
  public void setSourceFieldName(String sourceFieldName) {
    this.sourceFieldName = sourceFieldName;
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
   * @param targetFieldName The targetFieldName to set
   */
  public void setTargetFieldName(String targetFieldName) {
    this.targetFieldName = targetFieldName;
  }
}
