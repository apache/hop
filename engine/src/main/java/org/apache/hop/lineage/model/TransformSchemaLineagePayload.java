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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import lombok.Getter;

/**
 * Observed field names and Hop types at a transform boundary (input or output). Emitted after the
 * transform has run so {@link org.apache.hop.pipeline.transform.BaseTransform#getInputRowMeta()}
 * and output rowsets are populated when data flowed.
 */
@Getter
public final class TransformSchemaLineagePayload implements LineagePayload {

  private final TransformSchemaDirection direction;
  private final List<LineageFieldSchema> fields;

  public TransformSchemaLineagePayload(
      TransformSchemaDirection direction, List<LineageFieldSchema> fields) {
    this.direction = Objects.requireNonNull(direction, "direction");
    this.fields =
        fields == null || fields.isEmpty()
            ? List.of()
            : Collections.unmodifiableList(List.copyOf(fields));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TransformSchemaLineagePayload that = (TransformSchemaLineagePayload) o;
    return direction == that.direction && fields.equals(that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(direction, fields);
  }
}
