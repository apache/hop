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
package org.apache.hop.pipeline.transforms.databasevaluevalidation;

import java.util.Objects;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;

@Getter
@Setter
@NoArgsConstructor
public class DatabaseValueValidationField {

  @HopMetadataProperty(
      key = "stream_name",
      injectionKey = "STREAM_FIELDNAME",
      injectionKeyDescription = "DatabaseValueValidationMeta.Injection.FieldStream",
      hopMetadataPropertyType = HopMetadataPropertyType.STREAM_FIELD)
  private String fieldStream;

  @HopMetadataProperty(
      key = "column_name",
      injectionKey = "DATABASE_FIELDNAME",
      injectionKeyDescription = "DatabaseValueValidationMeta.Injection.FieldDatabase",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_COLUMN)
  private String fieldDatabase;

  public DatabaseValueValidationField(String fieldDatabase, String fieldStream) {
    this.fieldDatabase = fieldDatabase;
    this.fieldStream = fieldStream;
  }

  public DatabaseValueValidationField(DatabaseValueValidationField other) {
    this.fieldStream = other.fieldStream;
    this.fieldDatabase = other.fieldDatabase;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatabaseValueValidationField that = (DatabaseValueValidationField) o;
    return Objects.equals(fieldStream, that.fieldStream)
        && Objects.equals(fieldDatabase, that.fieldDatabase);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldStream, fieldDatabase);
  }
}
