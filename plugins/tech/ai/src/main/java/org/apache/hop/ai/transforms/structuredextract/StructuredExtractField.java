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
package org.apache.hop.ai.transforms.structuredextract;

import java.util.Objects;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * One field to pull out of the text.
 *
 * <p>This is the whole user-facing contract of the transform: the rows of this grid become the JSON
 * schema sent to the model, the columns {@code getFields} adds to the stream, and the types the
 * answer is coerced into.
 */
public class StructuredExtractField {

  @HopMetadataProperty(
      key = "name",
      injectionKey = "FIELD_NAME",
      injectionKeyDescription = "StructuredExtractMeta.Injection.FIELD_NAME")
  private String name;

  /** A Hop value type name, as {@code ValueMetaFactory} spells it: String, Integer, Number, ... */
  @HopMetadataProperty(
      key = "type",
      injectionKey = "FIELD_TYPE",
      injectionKeyDescription = "StructuredExtractMeta.Injection.FIELD_TYPE")
  private String type = "String";

  /**
   * What this field means, in the user's own words. It is sent to the model as the schema
   * property's description and is the single most effective thing a user can write here: "the date
   * the contract renews" extracts far better than a field called {@code renewal_date} alone.
   */
  @HopMetadataProperty(
      key = "description",
      injectionKey = "FIELD_DESCRIPTION",
      injectionKeyDescription = "StructuredExtractMeta.Injection.FIELD_DESCRIPTION")
  private String description;

  /**
   * Whether the model must return this field. Optional fields let a model say "not present" rather
   * than inventing a value, which is usually what you want for anything that may be absent.
   */
  @HopMetadataProperty(
      key = "required",
      injectionKey = "FIELD_REQUIRED",
      injectionKeyDescription = "StructuredExtractMeta.Injection.FIELD_REQUIRED")
  private boolean required = true;

  /**
   * Optional comma separated list of the only values allowed. It becomes a JSON enum, which
   * constrains the model rather than asking it politely, so it is the reliable way to classify.
   */
  @HopMetadataProperty(
      key = "allowed_values",
      injectionKey = "FIELD_ALLOWED_VALUES",
      injectionKeyDescription = "StructuredExtractMeta.Injection.FIELD_ALLOWED_VALUES")
  private String allowedValues;

  public StructuredExtractField() {}

  public StructuredExtractField(String name, String type, String description, boolean required) {
    this.name = name;
    this.type = type;
    this.description = description;
    this.required = required;
  }

  public StructuredExtractField(StructuredExtractField other) {
    this.name = other.name;
    this.type = other.type;
    this.description = other.description;
    this.required = other.required;
    this.allowedValues = other.allowedValues;
  }

  public String getName() {
    return name;
  }

  /**
   * The name with surrounding space removed, or empty when there is nothing but space.
   *
   * <p>{@code Utils.isEmpty} is false for a string of spaces, so testing the raw name would let a
   * blank row through and add a column with no name to the stream.
   */
  public String trimmedName() {
    return name == null ? "" : name.trim();
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public boolean isRequired() {
    return required;
  }

  public void setRequired(boolean required) {
    this.required = required;
  }

  public String getAllowedValues() {
    return allowedValues;
  }

  public void setAllowedValues(String allowedValues) {
    this.allowedValues = allowedValues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StructuredExtractField other)) {
      return false;
    }
    return required == other.required
        && Objects.equals(name, other.name)
        && Objects.equals(type, other.type)
        && Objects.equals(description, other.description)
        && Objects.equals(allowedValues, other.allowedValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, description, required, allowedValues);
  }
}
