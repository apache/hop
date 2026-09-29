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

import java.util.Arrays;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class ParquetField {
  @HopMetadataProperty(key = "source_field")
  private String sourceFieldName;

  @HopMetadataProperty(key = "target_field")
  private String targetFieldName;

  /**
   * Parquet type code ({@link ParquetFieldType#getCode()}). Empty keeps the type chosen from the
   * Hop type of the source field.
   */
  @HopMetadataProperty(key = "parquet_type")
  private String parquetType;

  /** Decimal precision. Used only when the Parquet type is Decimal. */
  @HopMetadataProperty(key = "precision")
  private String precision;

  /** Decimal scale. Used only when the Parquet type is Decimal. */
  @HopMetadataProperty(key = "scale")
  private String scale;

  public ParquetField() {}

  public ParquetField(String sourceFieldName, String targetFieldName) {
    this.sourceFieldName = sourceFieldName;
    this.targetFieldName = targetFieldName;
  }

  public ParquetField(ParquetField f) {
    this.sourceFieldName = f.sourceFieldName;
    this.targetFieldName = f.targetFieldName;
    this.parquetType = f.parquetType;
    this.precision = f.precision;
    this.scale = f.scale;
  }

  /**
   * @return the selected Parquet type, or null when none is selected
   * @throws HopException when a type is set but is not one of the supported types
   */
  public ParquetFieldType parquetFieldType() throws HopException {
    if (Utils.isEmpty(parquetType) || parquetType.trim().isEmpty()) {
      return null;
    }
    ParquetFieldType type = ParquetFieldType.fromCode(parquetType);
    if (type == null) {
      throw new HopException(
          "Parquet type '"
              + parquetType
              + "' of field '"
              + (sourceFieldName == null ? "" : sourceFieldName)
              + "' is not supported. Supported types: "
              + String.join(", ", Arrays.asList(ParquetFieldType.codes())));
    }
    return type;
  }
}
