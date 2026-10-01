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

package org.apache.hop.lineage.model;

import java.util.Objects;
import lombok.Getter;

/**
 * Provenance of one column written by a relational transform (e.g. Table Output), used to build
 * column-level lineage for the pipeline stream path. Carries the mapping from the input stream
 * field to the target column, plus the {@code originTransform} — the name of the transform that
 * produced the stream field ({@code IValueMeta.getOrigin()}). A sink correlates {@code
 * originTransform} with the source-table read from the same transform to resolve {@code
 * targetColumn} back to a source column, even through intervening pass-through transforms. Left
 * {@code null}/blank when the origin is unknown, in which case no column lineage is inferred for
 * the column.
 */
@Getter
public final class RelationalWriteColumn {

  /** Target table column name. */
  private final String targetColumn;

  /** Input stream field feeding the column. */
  private final String streamField;

  /** Name of the transform that produced the stream field, or null/blank when unknown. */
  private final String originTransform;

  public RelationalWriteColumn(String targetColumn, String streamField, String originTransform) {
    this.targetColumn = targetColumn;
    this.streamField = streamField;
    this.originTransform = originTransform;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RelationalWriteColumn that = (RelationalWriteColumn) o;
    return Objects.equals(targetColumn, that.targetColumn)
        && Objects.equals(streamField, that.streamField)
        && Objects.equals(originTransform, that.originTransform);
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetColumn, streamField, originTransform);
  }

  @Override
  public String toString() {
    return "RelationalWriteColumn{"
        + "targetColumn="
        + targetColumn
        + ", streamField="
        + streamField
        + ", originTransform="
        + originTransform
        + '}';
  }
}
