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
 *
 */

package org.apache.hop.pipeline.transforms.input;

import java.util.Objects;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class InputField {
  @HopMetadataProperty private String name;
  @HopMetadataProperty private String type;
  @HopMetadataProperty private String length;
  @HopMetadataProperty private String precision;

  public InputField() {}

  public InputField(String name, String type, String length, String precision) {
    this.name = name;
    this.type = type;
    this.length = length;
    this.precision = precision;
  }

  public InputField(InputField f) {
    this.name = f.name;
    this.type = f.type;
    this.length = f.length;
    this.precision = f.precision;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InputField that = (InputField) o;
    return Objects.equals(name, that.name)
        && Objects.equals(type, that.type)
        && Objects.equals(length, that.length)
        && Objects.equals(precision, that.precision);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, length, precision);
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * Sets name
   *
   * @param name value of name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public String getType() {
    return type;
  }

  /**
   * Sets type
   *
   * @param type value of type
   */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * Gets length
   *
   * @return value of length
   */
  public String getLength() {
    return length;
  }

  /**
   * Sets length
   *
   * @param length value of length
   */
  public void setLength(String length) {
    this.length = length;
  }

  /**
   * Gets precision
   *
   * @return value of precision
   */
  public String getPrecision() {
    return precision;
  }

  /**
   * Sets precision
   *
   * @param precision value of precision
   */
  public void setPrecision(String precision) {
    this.precision = precision;
  }
}
