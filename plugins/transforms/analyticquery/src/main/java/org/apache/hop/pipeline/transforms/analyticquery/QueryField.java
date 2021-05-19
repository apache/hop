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

package org.apache.hop.pipeline.transforms.analyticquery;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class QueryField {
  private static final Class<?> PKG = AnalyticQuery.class; // For Translator

  public enum AggregateType {
    NONE(""),
    LEAD(BaseMessages.getString(PKG, "AnalyticQueryMeta.TypeGroupLongDesc.LEAD")),
    LAG(BaseMessages.getString(PKG, "AnalyticQueryMeta.TypeGroupLongDesc.LAG"));

    private String description;

    AggregateType(String description) {
      this.description = description;
    }

    public static final String[] getDescriptions() {
      return new String[] {LEAD.description, LAG.description};
    }

    public static final AggregateType findTypeWithName(String name) {
      for (AggregateType value : values()) {
        if (value.name().equalsIgnoreCase(name)) {
          return value;
        }
      }
      return NONE;
    }

    public static final AggregateType findTypeWithDescription(String description) {
      for (AggregateType value : values()) {
        if (value.description.equalsIgnoreCase(description)) {
          return value;
        }
      }
      return NONE;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    public String getDescription() {
      return description;
    }
  }

  /** Name of OUTPUT fieldname "MYNEWLEADFUNCTION" */
  // @Injection(group = "fields", name = "OUTPUT.AGGREGATE_FIELD")
  @HopMetadataProperty(key = "aggregate", injectionKey = "OUTPUT.AGGREGATE_FIELD")
  private String aggregateField;

  /** Name of the input fieldname it operates on "ORDERTOTAL" */
  // @Injection(group = "fields", name = "OUTPUT.SUBJECT_FIELD")
  @HopMetadataProperty(key = "subject", injectionKey = "OUTPUT.SUBJECT_FIELD")
  private String subjectField;

  /** Aggregate type (LEAD/LAG, etc) */
  // @Injection(group = "fields", name = "OUTPUT.AGGREGATE_TYPE")
  @HopMetadataProperty(key = "type", injectionKey = "OUTPUT.AGGREGATE_TYPE")
  private AggregateType aggregateType;

  /** Offset "N" of how many rows to go forward/back */
  // @Injection(group = "fields", name = "OUTPUT.VALUE_FIELD")
  @HopMetadataProperty(key = "valuefield", injectionKey = "OUTPUT.VALUE_FIELD")
  private int valueField;

  public QueryField() {
    aggregateType = AggregateType.NONE;
    valueField = 1;
  }

  public QueryField(
      String aggregateField, String subjectField, AggregateType aggregateType, int valueField) {
    this.aggregateField = aggregateField;
    this.subjectField = subjectField;
    this.aggregateType = aggregateType;
    this.valueField = valueField;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryField that = (QueryField) o;
    return valueField == that.valueField
        && Objects.equals(aggregateField, that.aggregateField)
        && Objects.equals(subjectField, that.subjectField)
        && aggregateType == that.aggregateType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(aggregateField, subjectField, aggregateType, valueField);
  }

  /**
   * Gets aggregateField
   *
   * @return value of aggregateField
   */
  public String getAggregateField() {
    return aggregateField;
  }

  /** @param aggregateField The aggregateField to set */
  public void setAggregateField(String aggregateField) {
    this.aggregateField = aggregateField;
  }

  /**
   * Gets subjectField
   *
   * @return value of subjectField
   */
  public String getSubjectField() {
    return subjectField;
  }

  /** @param subjectField The subjectField to set */
  public void setSubjectField(String subjectField) {
    this.subjectField = subjectField;
  }

  /**
   * Gets aggregateType
   *
   * @return value of aggregateType
   */
  public AggregateType getAggregateType() {
    return aggregateType;
  }

  /** @param aggregateType The aggregateType to set */
  public void setAggregateType(AggregateType aggregateType) {
    this.aggregateType = aggregateType;
  }

  /**
   * Gets valueField
   *
   * @return value of valueField
   */
  public int getValueField() {
    return valueField;
  }

  /** @param valueField The valueField to set */
  public void setValueField(int valueField) {
    this.valueField = valueField;
  }
}
