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

package org.apache.hop.lineage.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.hop.lineage.model.RelationalIoOperation;

/**
 * Declares that a transform's metadata describes relational (JDBC) table access, so the engine can
 * derive lineage from the {@code RDBMS_*} property annotations on that metadata instead of the
 * transform emitting events itself.
 *
 * <p>Placed on the {@code ITransformMeta} class. The annotated metadata is expected to carry, via
 * {@link org.apache.hop.metadata.api.HopMetadataPropertyType}:
 *
 * <ul>
 *   <li>{@code RDBMS_CONNECTION} — the connection name (required; without it there is no dataset
 *       namespace and nothing is emitted)
 *   <li>{@code RDBMS_TABLE}, optionally {@code RDBMS_SCHEMA} — the target/source table
 *   <li>{@code RDBMS_COLUMN} paired with {@code STREAM_FIELD} on the same mapping object —
 *       per-column provenance. Columns declaring no source (a generated technical key, for example)
 *       are simply left out of the column lineage.
 *   <li>{@code RDBMS_TRUNCATE} — a boolean, or a string whose value is matched against {@link
 *       #overwriteWhen()}, marking the write as replacing the target's contents
 * </ul>
 *
 * <p>Opting in is deliberately explicit rather than inferred from the presence of {@code
 * RDBMS_TABLE}: plenty of metadata names a table without reading or writing its rows (checking that
 * it exists, comparing two of them, describing a column to look for), and emitting dataset lineage
 * for those would be wrong.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RelationalLineage {

  /** What the transform does to the declared table. */
  RelationalIoOperation operation();

  /**
   * Property values of a string-typed {@code RDBMS_TRUNCATE} that mean "the target now holds only
   * this run's rows" — e.g. a bulk loader whose load action is {@code TRUNCATE} or {@code REPLACE}.
   * Ignored for a boolean {@code RDBMS_TRUNCATE}, where {@code true} carries the same meaning.
   */
  String[] overwriteWhen() default {};

  /**
   * Name of the metadata property holding a <i>field name</i> whose row value supplies the table at
   * runtime, rather than a literal table name. When set and populated, the declared {@code
   * RDBMS_TABLE} is not a real table and no lineage is derived from metadata alone — the transform
   * reports its actual targets itself.
   */
  String tableNameFromFieldProperty() default "";
}
