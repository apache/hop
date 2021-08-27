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

package org.apache.hop.avro.transforms.avrodecode;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class TargetField {
  @HopMetadataProperty(key = "source_field")
  private String sourceField;

  @HopMetadataProperty(key = "source_avro_type")
  private String sourceAvroType;

  @HopMetadataProperty(key = "target_field_name")
  private String targetFieldName;

  @HopMetadataProperty(key = "target_type")
  private String targetType;

  @HopMetadataProperty(key = "target_format")
  private String targetFormat;

  @HopMetadataProperty(key = "target_length")
  private String targetLength;

  @HopMetadataProperty(key = "target_precision")
  private String targetPrecision;

  public TargetField() {}

  public TargetField(
      String sourceField,
      String sourceAvroType,
      String targetFieldName,
      String targetType,
      String targetFormat,
      String targetLength,
      String targetPrecision) {
    this.sourceField = sourceField;
    this.sourceAvroType = sourceAvroType;
    this.targetFieldName = targetFieldName;
    this.targetType = targetType;
    this.targetFormat = targetFormat;
    this.targetLength = targetLength;
    this.targetPrecision = targetPrecision;
  }

  public TargetField(TargetField f) {
    this.sourceField = f.sourceField;
    this.sourceAvroType = f.sourceAvroType;
    this.targetFieldName = f.targetFieldName;
    this.targetType = f.targetType;
    this.targetFormat = f.targetFormat;
    this.targetLength = f.targetLength;
    this.targetPrecision = f.targetPrecision;
  }

  @Override
  public TargetField clone() {
    return new TargetField(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TargetField that = (TargetField) o;
    return Objects.equals(sourceField, that.sourceField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceField);
  }

  public IValueMeta createTargetValueMeta(IVariables variables) throws HopException {
    String name = variables.resolve(targetFieldName);
    int type = ValueMetaFactory.getIdForValueMeta(variables.resolve(targetType));
    int length = Const.toInt(variables.resolve(targetLength), -1);
    int precision = Const.toInt(variables.resolve(targetPrecision), -1);
    IValueMeta valueMeta = ValueMetaFactory.createValueMeta(name, type, length, precision);
    valueMeta.setConversionMask(variables.resolve(targetFormat));
    return valueMeta;
  }

  /**
   * Gets sourceField
   *
   * @return value of sourceField
   */
  public String getSourceField() {
    return sourceField;
  }

  /** @param sourceField The sourcePath to set */
  public void setSourceField(String sourceField) {
    this.sourceField = sourceField;
  }

  /**
   * Gets sourceAvroType
   *
   * @return value of sourceAvroType
   */
  public String getSourceAvroType() {
    return sourceAvroType;
  }

  /** @param sourceAvroType The sourceAvroType to set */
  public void setSourceAvroType(String sourceAvroType) {
    this.sourceAvroType = sourceAvroType;
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

  /**
   * Gets targetType
   *
   * @return value of targetType
   */
  public String getTargetType() {
    return targetType;
  }

  /** @param targetType The targetType to set */
  public void setTargetType(String targetType) {
    this.targetType = targetType;
  }

  /**
   * Gets targetFormat
   *
   * @return value of targetFormat
   */
  public String getTargetFormat() {
    return targetFormat;
  }

  /** @param targetFormat The targetFormat to set */
  public void setTargetFormat(String targetFormat) {
    this.targetFormat = targetFormat;
  }

  /**
   * Gets targetLength
   *
   * @return value of targetLength
   */
  public String getTargetLength() {
    return targetLength;
  }

  /** @param targetLength The targetLength to set */
  public void setTargetLength(String targetLength) {
    this.targetLength = targetLength;
  }

  /**
   * Gets targetPrecision
   *
   * @return value of targetPrecision
   */
  public String getTargetPrecision() {
    return targetPrecision;
  }

  /** @param targetPrecision The targetPrecision to set */
  public void setTargetPrecision(String targetPrecision) {
    this.targetPrecision = targetPrecision;
  }
}
