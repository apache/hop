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

package org.apache.hop.lakehouse.transforms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.lakehouse.LakeFormats;
import org.apache.hop.lakehouse.iceberg.CommitCoordinator;
import org.apache.hop.lakehouse.iceberg.IcebergRowReader;
import org.apache.hop.lakehouse.iceberg.IcebergTables;
import org.apache.hop.lakehouse.iceberg.io.HopVfsFileIO;
import org.apache.hop.lakehouse.metadata.LakeCatalog;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Runs Lake table output on the local engine and checks the Iceberg table it leaves behind. */
class LakeTableOutputLocalTest {

  private static final String[] REGIONS = {"EU", "US", "APAC"};

  @TempDir Path tempDir;

  private IRowMeta rowMeta;
  private MemoryMetadataProvider metadataProvider;
  private String tablePath;

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("region"));
    rowMeta.addValueMeta(new ValueMetaBigNumber("amount", 12, 2));
    metadataProvider = new MemoryMetadataProvider();
    tablePath = tempDir.resolve("orders").toUri().toString();
  }

  private static List<Object[]> rows(long from, long to) {
    List<Object[]> rows = new ArrayList<>();
    for (long id = from; id < to; id++) {
      rows.add(new Object[] {id, REGIONS[(int) (id % REGIONS.length)], BigDecimal.valueOf(id, 2)});
    }
    return rows;
  }

  private LakeTableOutputMeta output(String saveMode) {
    LakeTableOutputMeta meta = new LakeTableOutputMeta();
    meta.setFormat(LakeFormats.FORMAT_ICEBERG);
    meta.setIdentifierMode(LakeTableInputMeta.MODE_PATH);
    meta.setTablePath(tablePath);
    meta.setSaveMode(saveMode);
    return meta;
  }

  private static class Run {
    int errors;
    final AtomicInteger passedThrough = new AtomicInteger();
  }

  /** Injector -> output (copies) -> dummy. Optionally the dummy fails on the last row. */
  private Run run(LakeTableOutputMeta output, int copies, List<Object[]> rows, boolean failAfter)
      throws HopException {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("write-orders");
    TransformMeta injector = new TransformMeta("injector", new InjectorMeta());
    TransformMeta writer = new TransformMeta("output", output);
    writer.setCopies(copies);
    TransformMeta dummy = new TransformMeta("dummy", new DummyMeta());
    pipelineMeta.addTransform(injector);
    pipelineMeta.addTransform(writer);
    pipelineMeta.addTransform(dummy);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(injector, writer));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(writer, dummy));

    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.setMetadataProvider(metadataProvider);
    pipeline.prepareExecution();

    Run run = new Run();
    pipeline
        .getTransform("dummy", 0)
        .addRowListener(
            new RowAdapter() {
              @Override
              public void rowReadEvent(IRowMeta meta, Object[] row) throws HopTransformException {
                if (run.passedThrough.incrementAndGet() == rows.size() && failAfter) {
                  throw new HopTransformException("Failing after the output finished writing");
                }
              }
            });
    RowProducer producer = pipeline.addRowProducer("injector", 0);
    pipeline.startThreads();
    for (Object[] row : rows) {
      producer.putRow(rowMeta, row.clone());
    }
    producer.finished();
    pipeline.waitUntilFinished();
    run.errors = pipeline.getErrors();
    return run;
  }

  private Table table() throws HopException {
    return IcebergTables.loadFromPath(tablePath);
  }

  private static Set<Long> ids(Table table) {
    Set<Long> ids = new HashSet<>();
    List<Long> all = new ArrayList<>();
    try (IcebergRowReader reader = new IcebergRowReader(table, List.of("id"), null, null, 0, 1)) {
      reader.forEachRemaining(row -> all.add((Long) row[0]));
    }
    ids.addAll(all);
    assertEquals(all.size(), ids.size(), "no row is stored twice");
    return ids;
  }

  private long dataFileCount() throws IOException {
    Path data = tempDir.resolve("orders/data");
    if (!Files.exists(data)) {
      return 0;
    }
    try (Stream<Path> files = Files.walk(data)) {
      return files.filter(p -> p.toString().endsWith(".parquet")).count();
    }
  }

  @Test
  void createsTableInOneSnapshotFromAllCopies() throws Exception {
    Run run = run(output(LakeTableOutputMeta.MODE_ERROR), 3, rows(0, 1000), false);

    assertEquals(0, run.errors);
    assertEquals(1000, run.passedThrough.get(), "rows are passed on");
    Table table = table();
    assertEquals(1, table.history().size(), "one snapshot for the whole run");
    assertEquals(1000, ids(table).size());
    assertEquals("write-orders", table.currentSnapshot().summary().get("hop.pipeline"));
    assertTrue(table.currentSnapshot().summary().containsKey(CommitCoordinator.SUMMARY_RUN_ID));
    assertTrue(dataFileCount() >= 3, "each copy writes its own files");

    // The Spark engine's path tables use the same layout, so both engines see the same table.
    assertEquals(
        "1", Files.readString(tempDir.resolve("orders/metadata/version-hint.text")).trim());
    assertTrue(Files.exists(tempDir.resolve("orders/metadata/v1.metadata.json")));
  }

  @Test
  void typesOfCreatedTable() throws Exception {
    run(output(LakeTableOutputMeta.MODE_ERROR), 1, rows(0, 10), false);

    Schema schema = table().schema();
    assertEquals(Types.LongType.get(), schema.findType("id"));
    assertEquals(Types.StringType.get(), schema.findType("region"));
    assertEquals(Types.DecimalType.of(12, 2), schema.findType("amount"));
  }

  @Test
  void partitionsByColumns() throws Exception {
    LakeTableOutputMeta meta = output(LakeTableOutputMeta.MODE_ERROR);
    meta.setPartitionByColumns("region");

    assertEquals(0, run(meta, 2, rows(0, 300), false).errors);

    Table table = table();
    assertEquals("region", table.spec().fields().get(0).name());
    assertEquals(300, ids(table).size());
  }

  @Test
  void unknownPartitionColumnFails() throws Exception {
    LakeTableOutputMeta meta = output(LakeTableOutputMeta.MODE_ERROR);
    meta.setPartitionByColumns("country");

    assertTrue(run(meta, 1, rows(0, 10), false).errors > 0);
    assertFalse(Files.exists(tempDir.resolve("orders/metadata")));
  }

  @Test
  void appendAddsASnapshot() throws Exception {
    run(output(LakeTableOutputMeta.MODE_ERROR), 1, rows(0, 100), false);
    Run run = run(output(LakeTableOutputMeta.MODE_APPEND), 2, rows(100, 150), false);

    assertEquals(0, run.errors);
    Table table = table();
    assertEquals(2, table.history().size());
    assertEquals(150, ids(table).size());
  }

  @Test
  void appendCreatesAMissingTable() throws Exception {
    assertEquals(0, run(output(LakeTableOutputMeta.MODE_APPEND), 1, rows(0, 20), false).errors);
    assertEquals(20, ids(table()).size());
  }

  @Test
  void overwriteReplacesTheRows() throws Exception {
    run(output(LakeTableOutputMeta.MODE_ERROR), 1, rows(0, 100), false);
    Run run = run(output(LakeTableOutputMeta.MODE_OVERWRITE), 2, rows(500, 530), false);

    assertEquals(0, run.errors);
    Set<Long> ids = ids(table());
    assertEquals(30, ids.size());
    assertTrue(ids.contains(500L) && !ids.contains(0L));
  }

  @Test
  void errorIfExistsLeavesAnExistingTableAlone() throws Exception {
    run(output(LakeTableOutputMeta.MODE_ERROR), 1, rows(0, 100), false);
    Run run = run(output(LakeTableOutputMeta.MODE_ERROR), 1, rows(100, 200), false);

    assertTrue(run.errors > 0);
    assertEquals(100, ids(table()).size());
  }

  @Test
  void ignorePassesRowsWithoutWriting() throws Exception {
    run(output(LakeTableOutputMeta.MODE_ERROR), 1, rows(0, 100), false);
    Run run = run(output(LakeTableOutputMeta.MODE_IGNORE), 2, rows(100, 200), false);

    assertEquals(0, run.errors);
    assertEquals(100, run.passedThrough.get());
    assertEquals(1, table().history().size());
    assertEquals(100, ids(table()).size());
  }

  @Test
  void failureLaterInThePipelineCreatesNoTable() throws Exception {
    Run run = run(output(LakeTableOutputMeta.MODE_ERROR), 3, rows(0, 500), true);

    assertTrue(run.errors > 0);
    assertFalse(Files.exists(tempDir.resolve("orders/metadata")), "no table was created");
    assertEquals(0, dataFileCount(), "the data files written by the run are removed");
  }

  @Test
  void failureLaterInThePipelineLeavesTheTableUnchanged() throws Exception {
    run(output(LakeTableOutputMeta.MODE_ERROR), 1, rows(0, 100), false);
    long filesBefore = dataFileCount();

    Run run = run(output(LakeTableOutputMeta.MODE_APPEND), 2, rows(100, 400), true);

    assertTrue(run.errors > 0);
    Table table = table();
    assertEquals(1, table.history().size());
    assertEquals(100, ids(table).size());
    assertEquals(filesBefore, dataFileCount());
  }

  @Test
  void writesThroughAHadoopCatalog() throws Exception {
    LakeCatalog lake = new LakeCatalog();
    lake.setName("lake");
    lake.setCatalogName("lake");
    lake.setCatalogType(LakeCatalog.TYPE_HADOOP);
    lake.setWarehouse(tempDir.resolve("warehouse").toUri().toString());
    metadataProvider.getSerializer(LakeCatalog.class).save(lake);

    LakeTableOutputMeta meta = new LakeTableOutputMeta();
    meta.setFormat(LakeFormats.FORMAT_ICEBERG);
    meta.setIdentifierMode(LakeTableInputMeta.MODE_TABLE);
    meta.setCatalogMetadataName("lake");
    meta.setTableIdentifier("lake.sales.orders");
    meta.setSaveMode(LakeTableOutputMeta.MODE_APPEND);

    assertEquals(0, run(meta, 2, rows(0, 120), false).errors);
    Table table =
        IcebergTables.loadFromPath(tempDir.resolve("warehouse/sales/orders").toUri().toString());
    assertEquals(120, ids(table).size());
  }

  @Test
  void refusesToWriteToACatalogTableByPath() throws Exception {
    try (JdbcCatalog catalog = new JdbcCatalog()) {
      catalog.initialize(
          "lake",
          Map.of(
              CatalogProperties.URI,
              "jdbc:sqlite:" + tempDir.resolve("catalog.db"),
              CatalogProperties.WAREHOUSE_LOCATION,
              tempDir.resolve("warehouse").toUri().toString(),
              CatalogProperties.FILE_IO_IMPL,
              HopVfsFileIO.class.getName(),
              "jdbc.schema-version",
              "V1"));
      catalog.createNamespace(Namespace.of("sales"));
      Table managed =
          catalog.createTable(
              TableIdentifier.of("sales", "orders"),
              new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())));
      tablePath = managed.location();

      Run run = run(output(LakeTableOutputMeta.MODE_APPEND), 1, rows(0, 10), false);

      assertTrue(run.errors > 0);
      managed.refresh();
      assertEquals(null, managed.currentSnapshot());
    }
  }

  @Test
  void deltaIsNotSupportedOnTheLocalEngine() throws Exception {
    LakeTableOutputMeta meta = output(LakeTableOutputMeta.MODE_ERROR);
    meta.setFormat(LakeFormats.FORMAT_DELTA);

    assertTrue(run(meta, 1, rows(0, 10), false).errors > 0);
  }

  /**
   * A Hop timestamp is a local date and time, so it is stored as an Iceberg timestamp without zone
   * holding the same wall-clock time, whatever the JVM time zone is, and reads back unchanged.
   */
  @Test
  void timestampsKeepTheirWallClockTimeOutsideUtc() throws Exception {
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
      rowMeta.addValueMeta(new ValueMetaTimestamp("ts"));
      LocalDateTime noon = LocalDateTime.of(2026, 10, 1, 12, 0);
      List<Object[]> rows = new ArrayList<>();
      for (Object[] row : rows(0, 3)) {
        rows.add(new Object[] {row[0], row[1], row[2], Timestamp.valueOf(noon)});
      }

      Run run = run(output(LakeTableOutputMeta.MODE_ERROR), 1, rows, false);

      assertEquals(0, run.errors);
      Table table = table();
      assertEquals(Types.TimestampType.withoutZone(), table.schema().findType("ts"));
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          assertEquals(noon, record.getField("ts"), "stored as the same wall-clock time");
        }
      }
      try (IcebergRowReader reader = new IcebergRowReader(table, List.of("ts"), null, null, 0, 1)) {
        assertEquals(noon, ((Timestamp) reader.next()[0]).toLocalDateTime());
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  void unknownSaveModeIsRefused() throws Exception {
    run(output(LakeTableOutputMeta.MODE_ERROR), 1, rows(0, 10), false);

    Run run = run(output("Upsert"), 1, rows(10, 20), false);

    assertTrue(run.errors > 0, "an unsupported save mode fails instead of appending");
    assertEquals(10, ids(table()).size(), "the existing table is unchanged");
  }

  @Test
  void unknownSaveModeCreatesNoTable() throws Exception {
    Run run = run(output("Upsert"), 1, rows(0, 10), false);

    assertTrue(run.errors > 0);
    assertFalse(Files.exists(tempDir.resolve("orders/metadata")), "no table is created");
  }
}
