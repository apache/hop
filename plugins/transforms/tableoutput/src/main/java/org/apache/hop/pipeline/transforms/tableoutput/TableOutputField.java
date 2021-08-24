/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hop.pipeline.transforms.tableoutput;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class TableOutputField {

  public TableOutputField() {}

  public TableOutputField(String fieldDatabase, String fieldStream) {
    this.fieldDatabase = fieldDatabase;
    this.fieldStream = fieldStream;
  }

  public TableOutputField(TableOutputField obj) {
    this.fieldStream = obj.fieldStream;
    this.fieldDatabase = obj.fieldDatabase;
  }

  /** Fields containing the values in the input stream to insert */
  @HopMetadataProperty(
      key = "stream_name",
      injectionKey = "STREAM_FIELDNAME",
      injectionKeyDescription = "TableOutputMeta.Injection.FieldStream.Field")
  private String fieldStream;

  /** Fields in the table to insert */
  @HopMetadataProperty(
      key = "column_name",
      injectionKey = "DATABASE_FIELDNAME",
      injectionKeyDescription = "TableOutputMeta.Injection.FieldDatabase.Field")
  private String fieldDatabase;

  /** @return Fields containing the values in the input stream to insert. */
  public String getFieldStream() {
    return fieldStream;
  }

  /**
   * @param fieldStream The fields containing the values in the input stream to insert in the table.
   */
  public void setFieldStream(String fieldStream) {
    this.fieldStream = fieldStream;
  }

  /** @return Fields containing the fieldnames in the database insert. */
  public String getFieldDatabase() {
    return fieldDatabase;
  }

  /** @param fieldDatabase The fields containing the names of the fields to insert. */
  public void setFieldDatabase(String fieldDatabase) {
    this.fieldDatabase = fieldDatabase;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TableOutputField that = (TableOutputField) o;
    return fieldStream.equals(that.fieldStream) && fieldDatabase.equals(that.fieldDatabase);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldStream, fieldDatabase);
  }
}
