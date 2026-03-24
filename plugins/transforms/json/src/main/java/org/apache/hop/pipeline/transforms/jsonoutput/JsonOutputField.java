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

package org.apache.hop.pipeline.transforms.jsonoutput;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Describes a single field in a JSON output file */
@Getter
@Setter
public class JsonOutputField implements Cloneable {
  @HopMetadataProperty(
      key = "name",
      injectionKeyDescription = "JsonOutput.Injection.JSON_FIELDNAME")
  private String fieldName;

  @HopMetadataProperty(
      key = "element",
      injectionKeyDescription = "JsonOutput.Injection.JSON_ELEMENTNAME")
  private String elementName;

  public JsonOutputField() {}

  public JsonOutputField(JsonOutputField f) {
    this();
    this.elementName = f.elementName;
    this.fieldName = f.fieldName;
  }

  public int compare(Object obj) {
    JsonOutputField field = (JsonOutputField) obj;

    return fieldName.compareTo(field.getFieldName());
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof JsonOutputField that)) {
      return false;
    }
    return Objects.equals(fieldName, that.fieldName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fieldName);
  }

  @Override
  public Object clone() {
    return new JsonOutputField(this);
  }
}
