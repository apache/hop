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

package org.apache.hop.pipeline.transforms.schemamapping;

import java.util.Objects;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class SchemaMappingField {

  public SchemaMappingField() {}

  public SchemaMappingField(String fieldSchemaDefinition, String fieldStream) {
    this.fieldSchemaDefinition = fieldSchemaDefinition;
    this.fieldStream = fieldStream;
  }

  public SchemaMappingField(SchemaMappingField obj) {
    this.fieldStream = obj.fieldStream;
    this.fieldSchemaDefinition = obj.fieldSchemaDefinition;
  }

  /** Fields containing the values in the input stream to insert */
  @HopMetadataProperty(
      injectionKey = "STREAM_FIELDNAME",
      injectionKeyDescription = "SchemaMappingMeta.Injection.FieldStream.Field")
  private String fieldStream;

  /** Fields in the table to insert */
  @HopMetadataProperty(
      injectionKey = "SCHEMADEF_FIELDNAME",
      injectionKeyDescription = "SchemaMappingMeta.Injection.FieldSchemaDef.Field")
  private String fieldSchemaDefinition;

  /**
   * @return Fields containing the values in the input stream to insert.
   */
  public String getFieldStream() {
    return fieldStream;
  }

  /**
   * @param fieldStream The fields containing the values in the input stream to insert in the table.
   */
  public void setFieldStream(String fieldStream) {
    this.fieldStream = fieldStream;
  }

  /**
   * @return Fields containing the fieldnames in the database insert.
   */
  public String getFieldSchemaDefinition() {
    return fieldSchemaDefinition;
  }

  /**
   * @param fieldSchemaDefinition The fields containing the names of the fields to insert.
   */
  public void setFieldSchemaDefinition(String fieldSchemaDefinition) {
    this.fieldSchemaDefinition = fieldSchemaDefinition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SchemaMappingField that = (SchemaMappingField) o;
    return fieldStream.equals(that.fieldStream)
        && fieldSchemaDefinition.equals(that.fieldSchemaDefinition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldStream, fieldSchemaDefinition);
  }
}
