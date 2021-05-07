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

package org.apache.hop.metadata.serializer.xml.classes;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.ArrayList;
import java.util.List;

public class MetaData {

  @HopMetadataProperty private String filename;

  @HopMetadataProperty(key = "field_separator")
  private String separator;

  @HopMetadataProperty(key = "grouping_symbol")
  private String group;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<Field> fields;

  @HopMetadataProperty(groupKey = "values", key = "value")
  private List<String> values;

  @HopMetadataProperty(key = "test_enum")
  private TestEnum testEnum;

  public MetaData() {
    fields = new ArrayList<>();
    values = new ArrayList<>();
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  public String getFilename() {
    return filename;
  }

  /** @param filename The filename to set */
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /**
   * Gets separator
   *
   * @return value of separator
   */
  public String getSeparator() {
    return separator;
  }

  /** @param separator The separator to set */
  public void setSeparator(String separator) {
    this.separator = separator;
  }

  /**
   * Gets group
   *
   * @return value of group
   */
  public String getGroup() {
    return group;
  }

  /** @param group The group to set */
  public void setGroup(String group) {
    this.group = group;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<Field> getFields() {
    return fields;
  }

  /** @param fields The fields to set */
  public void setFields(List<Field> fields) {
    this.fields = fields;
  }

  /**
   * Gets values
   *
   * @return value of values
   */
  public List<String> getValues() {
    return values;
  }

  /** @param values The values to set */
  public void setValues(List<String> values) {
    this.values = values;
  }

  /**
   * Gets testEnum
   *
   * @return value of testEnum
   */
  public TestEnum getTestEnum() {
    return testEnum;
  }

  /** @param testEnum The testEnum to set */
  public void setTestEnum(TestEnum testEnum) {
    this.testEnum = testEnum;
  }
}
