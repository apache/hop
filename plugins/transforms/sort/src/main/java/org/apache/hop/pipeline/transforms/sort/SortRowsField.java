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

package org.apache.hop.pipeline.transforms.sort;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class SortRowsField {

  public SortRowsField() {}

  public SortRowsField(
      String fieldName,
      boolean ascending,
      boolean caseSensitive,
      boolean collatorEnabled,
      int collatorStrength,
      boolean preSortedField) {
    this.fieldName = fieldName;
    this.ascending = ascending;
    this.caseSensitive = caseSensitive;
    this.collatorEnabled = collatorEnabled;
    this.collatorStrength = collatorStrength;
    this.preSortedField = preSortedField;
  }

  /** order by which fields? */
  @HopMetadataProperty(key = "name", injectionKey = "NAME")
  private String fieldName;

  /** false : descending, true=ascending */
  @HopMetadataProperty(key = "ascending", injectionKey = "SORT_ASCENDING")
  private boolean ascending = true;

  /** false : case insensitive, true=case sensitive */
  @HopMetadataProperty(key = "case_sensitive", injectionKey = "IGNORE_CASE")
  private boolean caseSensitive = true;

  /** false : collator disabeld, true=collator enabled */
  @HopMetadataProperty(key = "collator_enabled", injectionKey = "COLLATOR_ENABLED")
  private boolean collatorEnabled = false;

  // collator strength, 0,1,2,3
  @HopMetadataProperty(key = "collator_strength", injectionKey = "COLLATOR_STRENGTH")
  private int collatorStrength = 0;

  /** false : not a presorted field, true=presorted field */
  @HopMetadataProperty(key = "presorted", injectionKey = "PRESORTED")
  private boolean preSortedField = false;

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public boolean isAscending() {
    return ascending;
  }

  public void setAscending(boolean ascending) {
    this.ascending = ascending;
  }

  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  public void setCaseSensitive(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
  }

  public boolean isCollatorEnabled() {
    return collatorEnabled;
  }

  public void setCollatorEnabled(boolean collatorEnabled) {
    this.collatorEnabled = collatorEnabled;
  }

  public int getCollatorStrength() {
    return collatorStrength;
  }

  public void setCollatorStrength(int collatorStrength) {
    this.collatorStrength = collatorStrength;
  }

  public boolean isPreSortedField() {
    return preSortedField;
  }

  public void setPreSortedField(boolean preSortedField) {
    this.preSortedField = preSortedField;
  }
}
