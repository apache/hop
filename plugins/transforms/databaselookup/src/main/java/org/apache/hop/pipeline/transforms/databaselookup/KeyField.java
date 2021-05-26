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

package org.apache.hop.pipeline.transforms.databaselookup;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class KeyField {
  @HopMetadataProperty(
      key = "name",
      injectionKey = "key_input_field1",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.KeyInputField1")
  private String streamField1;

  @HopMetadataProperty(
      key = "name2",
      injectionKey = "key_input_field2",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.KeyInputField2")
  private String streamField2;

  @HopMetadataProperty(
      key = "condition",
      injectionKey = "key_condition",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.KeyCondition")
  private String condition;

  @HopMetadataProperty(
      key = "field",
      injectionKey = "key_table_field",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.KeyTableField")
  private String tableField;

  public KeyField() {}

  public KeyField(String streamField1, String streamField2, String condition, String tableField) {
    this.streamField1 = streamField1;
    this.streamField2 = streamField2;
    this.condition = condition;
    this.tableField = tableField;
  }

  public KeyField(KeyField k) {
    this.streamField1 = k.streamField1;
    this.streamField2 = k.streamField2;
    this.condition = k.condition;
    this.tableField = k.tableField;
  }

  /**
   * Gets streamField1
   *
   * @return value of streamField1
   */
  public String getStreamField1() {
    return streamField1;
  }

  /** @param streamField1 The streamField1 to set */
  public void setStreamField1(String streamField1) {
    this.streamField1 = streamField1;
  }

  /**
   * Gets streamField2
   *
   * @return value of streamField2
   */
  public String getStreamField2() {
    return streamField2;
  }

  /** @param streamField2 The streamField2 to set */
  public void setStreamField2(String streamField2) {
    this.streamField2 = streamField2;
  }

  /**
   * Gets condition
   *
   * @return value of condition
   */
  public String getCondition() {
    return condition;
  }

  /** @param condition The condition to set */
  public void setCondition(String condition) {
    this.condition = condition;
  }

  /**
   * Gets tableField
   *
   * @return value of tableField
   */
  public String getTableField() {
    return tableField;
  }

  /** @param tableField The tableField to set */
  public void setTableField(String tableField) {
    this.tableField = tableField;
  }
}
