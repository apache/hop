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

package org.apache.hop.pipeline.transforms.cratedbbulkloader;

import java.util.Objects;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;

public class CrateDBBulkLoaderField {

  public CrateDBBulkLoaderField() {}

  public CrateDBBulkLoaderField(String fieldDatabase, String fieldStream) {
    this.databaseField = fieldDatabase;
    this.streamField = fieldStream;
  }

  @HopMetadataProperty(
      key = "stream_name",
      injectionKey = "STREAM_FIELDNAME",
      injectionKeyDescription = "CrateDBBulkLoader.Inject.FIELDSTREAM")
  private String streamField;

  @HopMetadataProperty(
      key = "column_name",
      injectionKey = "DATABASE_FIELDNAME",
      injectionKeyDescription = "CrateDBBulkLoader.Inject.FIELDDATABASE",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_COLUMN)
  private String databaseField;

  public String getStreamField() {
    return streamField;
  }

  public void setStreamField(String streamField) {
    this.streamField = streamField;
  }

  public String getDatabaseField() {
    return databaseField;
  }

  public void setDatabaseField(String databaseField) {
    this.databaseField = databaseField;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CrateDBBulkLoaderField that = (CrateDBBulkLoaderField) o;
    return streamField.equals(that.streamField) && databaseField.equals(that.databaseField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamField, databaseField);
  }
}
