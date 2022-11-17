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

package org.apache.hop.pipeline.transforms.memgroupby;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class GAggregate {
  @HopMetadataProperty(
      key = "aggregate",
      injectionGroupKey = "AGGREGATES",
      injectionGroupDescription = "MemoryGroupBy.Injection.AGGREGATES",
      injectionKey = "AGGREGATEFIELD",
      injectionKeyDescription = "MemoryGroupBy.Injection.AGGREGATEFIELD")
  private String field;

  @HopMetadataProperty(
      key = "subject",
      injectionGroupKey = "AGGREGATES",
      injectionGroupDescription = "MemoryGroupBy.Injection.AGGREGATES",
      injectionKey = "SUBJECTFIELD",
      injectionKeyDescription = "MemoryGroupBy.Injection.SUBJECTFIELD")
  private String subject;

  @HopMetadataProperty(
      key = "type",
      storeWithCode = true,
      injectionGroupKey = "AGGREGATES",
      injectionGroupDescription = "MemoryGroupBy.Injection.AGGREGATES",
      injectionKey = "AGGREGATETYPE",
      injectionKeyDescription = "MemoryGroupBy.Injection.AGGREGATETYPE",
      injectionConverter = GroupTypeConverter.class)
  private MemoryGroupByMeta.GroupType type;

  @HopMetadataProperty(
      key = "valuefield",
      injectionGroupKey = "AGGREGATES",
      injectionGroupDescription = "MemoryGroupBy.Injection.AGGREGATES",
      injectionKey = "VALUEFIELD",
      injectionKeyDescription = "MemoryGroupBy.Injection.VALUEFIELD")
  private String valueField;

  public GAggregate() {
    type = MemoryGroupByMeta.GroupType.None;
  }

  public GAggregate(GAggregate a) {
    this.field = a.field;
    this.subject = a.subject;
    this.type = a.type;
    this.valueField = a.valueField;
  }

  public GAggregate(
      String field, String subject, MemoryGroupByMeta.GroupType type, String valueField) {
    this.field = field;
    this.subject = subject;
    this.type = type;
    this.valueField = valueField;
  }

  /**
   * Gets field
   *
   * @return value of field
   */
  public String getField() {
    return field;
  }

  /**
   * Sets field
   *
   * @param field value of field
   */
  public void setField(String field) {
    this.field = field;
  }

  /**
   * Gets subject
   *
   * @return value of subject
   */
  public String getSubject() {
    return subject;
  }

  /**
   * Sets subject
   *
   * @param subject value of subject
   */
  public void setSubject(String subject) {
    this.subject = subject;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public MemoryGroupByMeta.GroupType getType() {
    return type;
  }

  /**
   * Sets type
   *
   * @param type value of type
   */
  public void setType(MemoryGroupByMeta.GroupType type) {
    this.type = type;
  }

  /**
   * Gets valueField
   *
   * @return value of valueField
   */
  public String getValueField() {
    return valueField;
  }

  /**
   * Sets valueField
   *
   * @param valueField value of valueField
   */
  public void setValueField(String valueField) {
    this.valueField = valueField;
  }
}
