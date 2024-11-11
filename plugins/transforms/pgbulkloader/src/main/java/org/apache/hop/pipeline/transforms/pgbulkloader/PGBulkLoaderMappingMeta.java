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

package org.apache.hop.pipeline.transforms.pgbulkloader;

import java.util.Objects;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;

public class PGBulkLoaderMappingMeta {
  private static final Class<?> PKG = PGBulkLoaderMeta.class;

  /** Field value to dateMask after lookup */
  @HopMetadataProperty(
      key = "stream_name",
      injectionKeyDescription = "PGBulkLoader.Injection.StreamName.Label",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_COLUMN)
  private String fieldTable;

  /** Field name in the stream */
  @HopMetadataProperty(
      key = "field_name",
      injectionKeyDescription = "PGBulkLoader.Injection.FieldName.Label")
  private String fieldStream;

  /** boolean indicating if field needs to be updated */
  @HopMetadataProperty(
      key = "date_mask",
      injectionKeyDescription = "PGBulkLoader.Injection.DateMask.Label")
  private String dateMask;

  public PGBulkLoaderMappingMeta() {
    fieldTable = "";
    fieldStream = "";
    dateMask = "";
  }

  public PGBulkLoaderMappingMeta(PGBulkLoaderMappingMeta m) {
    this.fieldTable = m.fieldTable;
    this.fieldStream = m.fieldStream;
    this.dateMask = m.dateMask;
  }

  public PGBulkLoaderMappingMeta(String fieldTable, String fieldStream, String dateMask) {
    this.fieldTable = fieldTable;
    this.fieldStream = fieldStream;
    this.dateMask = dateMask;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PGBulkLoaderMappingMeta that = (PGBulkLoaderMappingMeta) o;
    return Objects.equals(fieldTable, that.fieldTable)
        && Objects.equals(fieldStream, that.fieldStream)
        && Objects.equals(dateMask, that.dateMask);
  }

  public String getFieldTable() {
    return fieldTable;
  }

  public void setFieldTable(String fieldTable) {
    this.fieldTable = fieldTable;
  }

  public String getFieldStream() {
    return fieldStream;
  }

  public void setFieldStream(String fieldStream) {
    this.fieldStream = fieldStream;
  }

  public String getDateMask() {
    return dateMask;
  }

  public void setDateMask(String dateMask) {
    this.dateMask = dateMask;
  }
}
