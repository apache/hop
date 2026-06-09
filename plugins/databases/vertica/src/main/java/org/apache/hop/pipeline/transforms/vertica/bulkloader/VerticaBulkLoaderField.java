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

package org.apache.hop.pipeline.transforms.vertica.bulkloader;

import java.util.Objects;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;

public class VerticaBulkLoaderField {

  public VerticaBulkLoaderField() {}

  public VerticaBulkLoaderField(String fieldDatabase, String fieldStream) {
    this.fieldDatabase = fieldDatabase;
    this.fieldStream = fieldStream;
  }

  @HopMetadataProperty(
      key = "stream_name",
      injectionKey = "STREAM_FIELDNAME",
      injectionKeyDescription = "VerticaBulkLoader.Inject.FIELDSTREAM")
  private String fieldStream;

  @HopMetadataProperty(
      key = "column_name",
      injectionKey = "DATABASE_FIELDNAME",
      injectionKeyDescription = "VerticaBulkLoader.Inject.FIELDDATABASE",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_COLUMN)
  private String fieldDatabase;

  public String getFieldStream() {
    return fieldStream;
  }

  public void setFieldStream(String fieldStream) {
    this.fieldStream = fieldStream;
  }

  public String getFieldDatabase() {
    return fieldDatabase;
  }

  public void setFieldDatabase(String fieldDatabase) {
    this.fieldDatabase = fieldDatabase;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VerticaBulkLoaderField that = (VerticaBulkLoaderField) o;
    return fieldStream.equals(that.fieldStream) && fieldDatabase.equals(that.fieldDatabase);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldStream, fieldDatabase);
  }
}
