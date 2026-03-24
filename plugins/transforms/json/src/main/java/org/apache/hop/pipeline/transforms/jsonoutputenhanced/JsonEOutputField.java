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

package org.apache.hop.pipeline.transforms.jsonoutputenhanced;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class JsonEOutputField implements Cloneable {
  @HopMetadataProperty(
      key = "name",
      injectionKey = "JSON_FIELDNAME",
      injectionKeyDescription = "JsonEOutput.Injection.JSON_FIELDNAME")
  private String fieldName;

  @HopMetadataProperty(
      key = "element",
      injectionKey = "JSON_ELEMENTNAME",
      injectionKeyDescription = "JsonEOutput.Injection.JSON_ELEMENTNAME")
  private String elementName;

  @HopMetadataProperty(
      key = "json_fragment",
      injectionKey = "JSON_ISJSONFRAGMENT",
      injectionKeyDescription = "JsonEOutput.Injection.JSON_ISJSONFRAGMENT")
  private boolean jsonFragment;

  @HopMetadataProperty(
      key = "is_without_enclosing",
      injectionKey = "JSON_NOENCLOSURE",
      injectionKeyDescription = "JsonEOutput.Injection.JSON_NOENCLOSURE")
  private boolean withoutEnclosing;

  @HopMetadataProperty(
      key = "remove_if_blank",
      injectionKey = "JSON_REMOVEIFBLANK",
      injectionKeyDescription = "JsonEOutput.Injection.JSON_REMOVEIFBLANK")
  private boolean removeIfBlank;

  public JsonEOutputField() {}

  public JsonEOutputField(JsonEOutputField f) {
    this();
    this.elementName = f.elementName;
    this.fieldName = f.fieldName;
    this.jsonFragment = f.jsonFragment;
    this.withoutEnclosing = f.withoutEnclosing;
    this.removeIfBlank = f.removeIfBlank;
  }

  public int compare(Object obj) {
    JsonEOutputField field = (JsonEOutputField) obj;
    return fieldName.compareTo(field.getFieldName());
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof JsonEOutputField that)) {
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
    return new JsonEOutputField(this);
  }
}
