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

package org.apache.hop.beam.transforms.bigtable;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class BigtableColumn {

  @HopMetadataProperty(key = "qualifier")
  private String name;

  @HopMetadataProperty(key = "family")
  private String family;

  @HopMetadataProperty(key = "source_field")
  private String sourceField;

  public BigtableColumn() {}

  public BigtableColumn(BigtableColumn c) {
    this.name = c.name;
    this.family = c.family;
    this.sourceField = c.sourceField;
  }

  public BigtableColumn(String name, String family, String sourceField) {
    this.name = name;
    this.family = family;
    this.sourceField = sourceField;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BigtableColumn that = (BigtableColumn) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /** @param name The name to set */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets family
   *
   * @return value of family
   */
  public String getFamily() {
    return family;
  }

  /** @param family The family to set */
  public void setFamily(String family) {
    this.family = family;
  }

  /**
   * Gets sourceField
   *
   * @return value of sourceField
   */
  public String getSourceField() {
    return sourceField;
  }

  /** @param sourceField The sourceField to set */
  public void setSourceField(String sourceField) {
    this.sourceField = sourceField;
  }
}
