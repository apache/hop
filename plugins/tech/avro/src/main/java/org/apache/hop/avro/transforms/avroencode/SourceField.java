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

package org.apache.hop.avro.transforms.avroencode;

import org.apache.hop.core.Const;
import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class SourceField {
  @HopMetadataProperty(key = "source_field")
  private String sourceFieldName;

  @HopMetadataProperty(key = "target_field_name")
  private String targetFieldName;

  public SourceField() {}

  public SourceField(
      String sourceFieldName,
      String targetFieldName) {
    this.sourceFieldName = sourceFieldName;
    this.targetFieldName = targetFieldName;
  }

  public SourceField(SourceField f) {
    this.sourceFieldName = f.sourceFieldName;
    this.targetFieldName = f.targetFieldName;
  }

  @Override
  public SourceField clone() {
    return new SourceField(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SourceField that = (SourceField) o;
    return Objects.equals(sourceFieldName, that.sourceFieldName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceFieldName);
  }

  /**
   * If the target field name is not known we take the source field name
   *
   * @return The target field name in the Avro record.
   */
  public String calculateTargetFieldName() {
    return Const.NVL(targetFieldName, sourceFieldName);
  }

  /**
   * Gets sourceField
   *
   * @return value of sourceField
   */
  public String getSourceFieldName() {
    return sourceFieldName;
  }

  /** @param sourceFieldName The sourcePath to set */
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

  /** @param targetFieldName The targetFieldName to set */
  public void setTargetFieldName(String targetFieldName) {
    this.targetFieldName = targetFieldName;
  }

}
