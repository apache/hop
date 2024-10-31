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

package org.apache.hop.neo4j.transforms.loginfo;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class GetLoggingInfoField {

  public GetLoggingInfoField() {}

  public GetLoggingInfoField(String fieldName, String fieldType, String fieldArgument) {
    this.fieldName = fieldName;
    this.fieldType = fieldType;
    this.fieldArgument = fieldArgument;
  }

  @HopMetadataProperty(key = "name", injectionKey = "FIELD_NAME")
  private String fieldName;

  @HopMetadataProperty(key = "type", injectionKey = "FIELD_TYPE")
  private String fieldType;

  @HopMetadataProperty(key = "argument", injectionKey = "FIELD_ARGUMENT")
  private String fieldArgument;

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    GetLoggingInfoField that = (GetLoggingInfoField) obj;
    return fieldType.equals(that.fieldType)
        && fieldName.equals(that.fieldName)
        && fieldArgument.equals(that.fieldArgument);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, fieldType, fieldArgument);
  }
}
