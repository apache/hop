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

package org.apache.hop.lineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.lineage.LineageMetadataWalker.ColumnMapping;
import org.apache.hop.lineage.LineageMetadataWalker.Declaration;
import org.apache.hop.lineage.api.RelationalLineage;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.junit.jupiter.api.Test;

/**
 * Exercises the rules the walker applies to the {@code RDBMS_*} annotations. Uses purpose-built
 * metadata shapes rather than real transform metadata so each rule is isolated; that the real
 * transforms are annotated correctly is covered by their own module tests and the integration
 * suite.
 */
class LineageMetadataWalkerTest {

  /** A plain write: connection, schema and table on the meta, columns on a nested list. */
  @RelationalLineage(operation = RelationalIoOperation.WRITE)
  static class SimpleWriteMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection = "warehouse";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
    String schema = "staging";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
    String table = "orders";

    @HopMetadataProperty String notes = "ignored";

    @HopMetadataProperty List<Mapping> mappings = List.of(new Mapping("order_id", "id"));
  }

  static class Mapping {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.STREAM_FIELD)
    String stream;

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_COLUMN)
    String column;

    Mapping(String column, String stream) {
      this.column = column;
      this.stream = stream;
    }
  }

  @Test
  void readsConnectionSchemaTableAndColumnPairs() {
    Declaration d = LineageMetadataWalker.read(new SimpleWriteMeta());

    assertEquals(RelationalIoOperation.WRITE, d.operation());
    assertEquals("warehouse", d.connectionName());
    assertEquals("staging", d.schemaName());
    assertEquals("orders", d.tableName());
    assertTrue(d.isUsable());
    assertEquals(List.of(new ColumnMapping("order_id", "id")), d.columns());
  }

  /** Metadata that does not opt in produces nothing, however many RDBMS properties it carries. */
  static class UnannotatedMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection = "warehouse";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
    String table = "orders";
  }

  @Test
  void metadataWithoutTheClassAnnotationIsNotLineageBearing() {
    assertNull(LineageMetadataWalker.read(new UnannotatedMeta()));
    assertNull(LineageMetadataWalker.read(null));
  }

  /** The table and columns can live on a nested object, as they do for Insert/Update and Delete. */
  @RelationalLineage(operation = RelationalIoOperation.DELETE)
  static class NestedMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection = "warehouse";

    @HopMetadataProperty Lookup lookup = new Lookup();
  }

  static class Lookup {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
    String schema = "staging";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
    String table = "orders";

    @HopMetadataProperty List<Mapping> keys = List.of(new Mapping("order_id", "id"));
  }

  @Test
  void descendsIntoNestedMetadataObjects() {
    Declaration d = LineageMetadataWalker.read(new NestedMeta());

    assertEquals(RelationalIoOperation.DELETE, d.operation());
    assertEquals("staging", d.schemaName());
    assertEquals("orders", d.tableName());
    assertEquals(1, d.columns().size());
  }

  /**
   * A column with no declared source is generated by the transform — a dimension's technical key,
   * for instance. It must not appear in the column lineage at all rather than appear sourceless.
   */
  @RelationalLineage(operation = RelationalIoOperation.WRITE)
  static class GeneratedColumnMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection = "warehouse";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
    String table = "dim_customer";

    @HopMetadataProperty List<Generated> generated = List.of(new Generated());

    @HopMetadataProperty List<Mapping> mapped = List.of(new Mapping("name", "customer_name"));
  }

  static class Generated {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_COLUMN)
    String technicalKey = "customer_tk";
  }

  @Test
  void aColumnWithoutADeclaredSourceIsLeftOutOfColumnLineage() {
    Declaration d = LineageMetadataWalker.read(new GeneratedColumnMeta());

    assertEquals(List.of(new ColumnMapping("name", "customer_name")), d.columns());
  }

  /**
   * A mapping object declaring two stream fields — the shape of a key mapping that also holds the
   * second bound of a BETWEEN condition — cannot say which one feeds the column.
   */
  @RelationalLineage(operation = RelationalIoOperation.WRITE)
  static class AmbiguousMappingMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection = "warehouse";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
    String table = "orders";

    @HopMetadataProperty List<AmbiguousMapping> mapped = List.of(new AmbiguousMapping());
  }

  static class AmbiguousMapping {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_COLUMN)
    String column = "order_id";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.STREAM_FIELD)
    String from = "id_from";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.STREAM_FIELD)
    String to = "id_to";
  }

  @Test
  void anAmbiguousMappingYieldsNoColumnLineageRatherThanAGuess() {
    Declaration d = LineageMetadataWalker.read(new AmbiguousMappingMeta());

    assertTrue(d.isUsable(), "the table identity is still declared");
    assertTrue(
        d.columns().isEmpty(),
        "two stream fields on one mapping object must produce no column rather than the last one");
  }

  /** A boolean truncate marks the write as replacing the target. */
  @RelationalLineage(operation = RelationalIoOperation.WRITE)
  static class BooleanTruncateMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection = "warehouse";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
    String table = "orders";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TRUNCATE)
    boolean truncate = true;
  }

  @Test
  void booleanTruncateIsAnOverwrite() {
    assertTrue(LineageMetadataWalker.read(new BooleanTruncateMeta()).overwrite());
  }

  /** Loaders express the same thing as a load action; only the declared values count. */
  @RelationalLineage(
      operation = RelationalIoOperation.WRITE,
      overwriteWhen = {"TRUNCATE", "REPLACE"})
  static class LoadActionMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection = "warehouse";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
    String table = "orders";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TRUNCATE)
    String loadAction;

    LoadActionMeta(String loadAction) {
      this.loadAction = loadAction;
    }
  }

  @Test
  void loadActionIsAnOverwriteOnlyForTheDeclaredValues() {
    assertTrue(LineageMetadataWalker.read(new LoadActionMeta("truncate")).overwrite());
    assertTrue(LineageMetadataWalker.read(new LoadActionMeta("REPLACE")).overwrite());
    assertFalse(LineageMetadataWalker.read(new LoadActionMeta("APPEND")).overwrite());
    assertFalse(LineageMetadataWalker.read(new LoadActionMeta(null)).overwrite());
  }

  /**
   * When the target table comes from a row field there is no table to name at design time, so the
   * declaration is not usable and the transform reports its own targets instead.
   */
  @RelationalLineage(
      operation = RelationalIoOperation.WRITE,
      tableNameFromFieldProperty = "tableNameField")
  static class DynamicTableMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection = "warehouse";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
    String table = "unused";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
    String tableNameField;

    DynamicTableMeta(String tableNameField) {
      this.tableNameField = tableNameField;
    }
  }

  @Test
  void aTableNameTakenFromARowFieldMakesTheDeclarationUnusable() {
    assertFalse(LineageMetadataWalker.read(new DynamicTableMeta("target_table")).isUsable());
    // With the field left blank the transform writes to its fixed table after all.
    assertTrue(LineageMetadataWalker.read(new DynamicTableMeta("")).isUsable());
  }

  /** Without a connection there is no dataset namespace, so nothing can be emitted. */
  @RelationalLineage(operation = RelationalIoOperation.WRITE)
  static class NoConnectionMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
    String table = "orders";
  }

  @Test
  void aDeclarationWithoutAConnectionIsNotUsable() {
    assertFalse(LineageMetadataWalker.read(new NoConnectionMeta()).isUsable());
  }

  /** A cyclic metadata graph must terminate rather than hang or overflow. */
  @RelationalLineage(operation = RelationalIoOperation.WRITE)
  static class CyclicMeta {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
    String connection = "warehouse";

    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
    String table = "orders";

    @HopMetadataProperty CyclicMeta self;
  }

  @Test
  void aCyclicGraphTerminates() {
    CyclicMeta meta = new CyclicMeta();
    meta.self = meta;

    Declaration d = LineageMetadataWalker.read(meta);
    assertEquals("orders", d.tableName());
  }
}
