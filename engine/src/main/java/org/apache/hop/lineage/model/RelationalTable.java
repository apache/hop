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
 * One relational table referenced by a {@link RelationalIoLineagePayload}, decomposed into the
 * catalog/schema/table segments of the OpenLineage dataset-name contract (the "OpenLineage dataset
 * identity" page of the user manual, {@code {database}.{schema}.{table}}).
 *
 * <p>Segments are stored verbatim as the database resolves them (no re-casing): the emitter is
 * responsible for supplying them exactly as the database stores them in its catalog so both halves
 * of a cross-tool stitch agree. {@code database} and {@code schema} are optional (a database
 * without a catalog level supplies only schema+table); {@code table} is required. The qualified
 * name is derived centrally by {@code LineageRelationalIdentity#datasetName} so the join rule lives
 * in one place.
 */
@Getter
public final class RelationalTable {

  /** Catalog/database segment, or null/blank when the database has no catalog level. */
  private final String database;

  /** Schema segment, or null/blank when the database has no schema level. */
  private final String schema;

  /** Table (or view) name; required. */
  private final String table;

  public RelationalTable(String database, String schema, String table) {
    this.database = database;
    this.schema = schema;
    this.table = Objects.requireNonNull(table, "table");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RelationalTable that = (RelationalTable) o;
    return Objects.equals(database, that.database)
        && Objects.equals(schema, that.schema)
        && table.equals(that.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, schema, table);
  }

  @Override
  public String toString() {
    return "RelationalTable{"
        + "database="
        + database
        + ", schema="
        + schema
        + ", table="
        + table
        + '}';
  }
}
