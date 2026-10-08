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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.lakehouse.LakeField;
import org.apache.hop.lakehouse.LakeFormats;
import org.apache.hop.lakehouse.iceberg.IcebergRowReader;
import org.apache.hop.lakehouse.iceberg.io.HopVfsFileIO;
import org.apache.hop.lakehouse.metadata.LakeCatalog;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs Lake table input on the local engine against a real Iceberg table. The table is created
 * through a JDBC catalog on SQLite and written with Iceberg's own writers, with every file going
 * through Hop VFS.
 */
class LakeTableInputLocalTest {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "region", Types.StringType.get()),
          Types.NestedField.optional(3, "amount", Types.DecimalType.of(12, 2)),
          Types.NestedField.optional(4, "ts", Types.TimestampType.withoutZone()),
          Types.NestedField.optional(5, "note", Types.StringType.get()));

  private static final String[] REGIONS = {"EU", "US", "APAC"};

  @TempDir Path tempDir;

  private JdbcCatalog catalog;
  private Table table;
  private long firstSnapshotId;
  private long firstSnapshotMillis;
  private MemoryMetadataProvider metadataProvider;

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    catalog = new JdbcCatalog();
    catalog.initialize(
        "lake",
        Map.of(
            CatalogProperties.URI,
            "jdbc:sqlite:" + tempDir.resolve("catalog.db"),
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouse(),
            CatalogProperties.FILE_IO_IMPL,
            HopVfsFileIO.class.getName(),
            "jdbc.schema-version",
            "V1"));
    catalog.createNamespace(Namespace.of("sales"));
    table =
        catalog.createTable(
            TableIdentifier.of("sales", "orders"),
            SCHEMA,
            PartitionSpec.builderFor(SCHEMA).identity("region").build(),
            Map.of("format-version", "2"));

    append(0, 1000);
    table.refresh();
    firstSnapshotId = table.currentSnapshot().snapshotId();
    firstSnapshotMillis = table.currentSnapshot().timestampMillis();
    Thread.sleep(5);
    append(1000, 1500);
    table.refresh();

    metadataProvider = new MemoryMetadataProvider();
  }

  @AfterEach
  void tearDown() throws Exception {
    catalog.close();
  }

  private String warehouse() {
    return tempDir.resolve("warehouse").toUri().toString();
  }

  private void append(long from, long to) throws IOException {
    GenericFileWriterFactory writerFactory =
        new GenericFileWriterFactory.Builder(table).dataFileFormat(FileFormat.PARQUET).build();
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 0, from).format(FileFormat.PARQUET).build();
    PartitionKey key = new PartitionKey(table.spec(), table.schema());
    InternalRecordWrapper wrapper = new InternalRecordWrapper(table.schema().asStruct());
    PartitionedFanoutWriter<Record> writer =
        new PartitionedFanoutWriter<>(
            table.spec(),
            FileFormat.PARQUET,
            writerFactory,
            fileFactory,
            table.io(),
            128L * 1024 * 1024) {
          @Override
          protected PartitionKey partition(Record row) {
            key.partition(wrapper.wrap(row));
            return key;
          }
        };
    for (long id = from; id < to; id++) {
      GenericRecord record = GenericRecord.create(table.schema());
      record.setField("id", id);
      record.setField("region", REGIONS[(int) (id % REGIONS.length)]);
      record.setField("amount", BigDecimal.valueOf(id, 2));
      record.setField("ts", LocalDateTime.of(2026, 10, 1, 0, 0).plusHours(id));
      record.setField("note", id % 10 == 0 ? null : "order " + id);
      writer.write(record);
    }
    var append = table.newAppend();
    for (DataFile file : writer.dataFiles()) {
      append.appendFile(file);
    }
    append.commit();
  }

  private LakeTableInputMeta pathInput() {
    LakeTableInputMeta meta = new LakeTableInputMeta();
    meta.setFormat(LakeFormats.FORMAT_ICEBERG);
    meta.setIdentifierMode(LakeTableInputMeta.MODE_PATH);
    meta.setTablePath(table.location());
    return meta;
  }

  /** The rows a pipeline with only this input transform produces, from all its copies. */
  private Result run(LakeTableInputMeta meta, int copies) throws HopException {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("read");
    TransformMeta transformMeta = new TransformMeta("input", meta);
    transformMeta.setCopies(copies);
    pipelineMeta.addTransform(transformMeta);

    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.setMetadataProvider(metadataProvider);
    pipeline.prepareExecution();

    Result result = new Result();
    for (int copy = 0; copy < copies; copy++) {
      pipeline
          .getTransform("input", copy)
          .addRowListener(
              new RowAdapter() {
                @Override
                public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
                  result.rowMeta = rowMeta;
                  result.rows.add(row);
                }
              });
    }
    pipeline.startThreads();
    pipeline.waitUntilFinished();
    result.errors = pipeline.getErrors();
    return result;
  }

  private static class Result {
    final List<Object[]> rows = Collections.synchronizedList(new ArrayList<>());
    volatile IRowMeta rowMeta;
    int errors;

    Set<Long> ids() {
      Set<Long> ids = new HashSet<>();
      synchronized (rows) {
        for (Object[] row : rows) {
          ids.add((Long) row[0]);
        }
      }
      return ids;
    }
  }

  @Test
  void readsAllColumnsAndRows() throws Exception {
    Result result = run(pathInput(), 1);

    assertEquals(0, result.errors);
    assertEquals(1500, result.rows.size());
    assertEquals(1500, result.ids().size());
    assertEquals(
        List.of("id", "region", "amount", "ts", "note"), List.of(result.rowMeta.getFieldNames()));

    Object[] row = result.rows.stream().filter(r -> (Long) r[0] == 7L).findFirst().orElseThrow();
    assertEquals("US", row[1]);
    assertEquals(0, new BigDecimal("0.07").compareTo((BigDecimal) row[2]));
    assertInstanceOf(Timestamp.class, row[3]);
    assertEquals(LocalDateTime.of(2026, 10, 1, 7, 0), ((Timestamp) row[3]).toLocalDateTime());
    assertEquals("order 7", row[4]);

    Object[] nullNote =
        result.rows.stream().filter(r -> (Long) r[0] == 10L).findFirst().orElseThrow();
    assertNull(nullNote[4]);
  }

  @Test
  void copiesReadEveryRowExactlyOnce() throws Exception {
    Result result = run(pathInput(), 3);

    assertEquals(0, result.errors);
    assertEquals(1500, result.rows.size());
    assertEquals(1500, result.ids().size());
  }

  @Test
  void selectedFieldsKeepTheirOrderAndType() throws Exception {
    LakeTableInputMeta meta = pathInput();
    meta.getFields().add(new LakeField("note", "String"));
    meta.getFields().add(new LakeField("id", "String"));

    Result result = run(meta, 1);

    assertEquals(0, result.errors);
    assertEquals(List.of("note", "id"), List.of(result.rowMeta.getFieldNames()));
    assertTrue(
        result.rows.stream().anyMatch(r -> "order 7".equals(r[0]) && "7".equals(r[1])),
        "the id column is converted to the String type of its field");
  }

  @Test
  void unknownSelectedFieldFails() throws Exception {
    LakeTableInputMeta meta = pathInput();
    meta.getFields().add(new LakeField("missing", "String"));

    assertTrue(run(meta, 1).errors > 0);
  }

  @Test
  void timeTravelToSnapshotId() throws Exception {
    LakeTableInputMeta meta = pathInput();
    meta.setTimeTravelType(LakeTableInputMeta.TIME_TRAVEL_VERSION);
    meta.setTimeTravelVersion(Long.toString(firstSnapshotId));

    Result result = run(meta, 2);

    assertEquals(0, result.errors);
    assertEquals(1000, result.ids().size());
  }

  @Test
  void timeTravelToTimestamp() throws Exception {
    LakeTableInputMeta meta = pathInput();
    meta.setTimeTravelType(LakeTableInputMeta.TIME_TRAVEL_TIMESTAMP);
    meta.setTimeTravelTimestamp(Long.toString(firstSnapshotMillis + 1));

    Result result = run(meta, 1);

    assertEquals(0, result.errors);
    assertEquals(1000, result.ids().size());
  }

  @Test
  void unknownSnapshotFails() throws Exception {
    LakeTableInputMeta meta = pathInput();
    meta.setTimeTravelType(LakeTableInputMeta.TIME_TRAVEL_VERSION);
    meta.setTimeTravelVersion("42");

    assertTrue(run(meta, 1).errors > 0);
  }

  @Test
  void versionHintOfHadoopTablesIsFollowed() throws Exception {
    // Hadoop catalogs and Spark path tables point at the current metadata file with
    // version-hint.text. Point it at the metadata written by the first append to show it is used.
    List<TableMetadata.MetadataLogEntry> previous =
        ((HasTableOperations) table).operations().current().previousFiles();
    Path firstAppend = Paths.get(URI.create(previous.get(previous.size() - 1).file()));
    Path metadata = firstAppend.getParent();
    Files.copy(firstAppend, metadata.resolve("v7.metadata.json"));
    Files.writeString(metadata.resolve("version-hint.text"), "7", StandardCharsets.UTF_8);

    Result result = run(pathInput(), 1);

    assertEquals(0, result.errors);
    assertEquals(1000, result.ids().size());
  }

  @Test
  void equalityDeletesAreApplied() throws Exception {
    // Engines like Flink and Spark delete rows by writing delete files next to the data.
    // Delete ids 0, 3, 6 and 9, which are all in the EU partition.
    Schema idOnly = table.schema().select("id");
    PartitionKey eu = new PartitionKey(table.spec(), table.schema());
    GenericRecord euRow = GenericRecord.create(table.schema());
    euRow.setField("region", "EU");
    eu.partition(new InternalRecordWrapper(table.schema().asStruct()).wrap(euRow));

    GenericFileWriterFactory factory =
        new GenericFileWriterFactory.Builder(table)
            .deleteFileFormat(FileFormat.PARQUET)
            .equalityFieldIds(new int[] {table.schema().findField("id").fieldId()})
            .equalityDeleteRowSchema(idOnly)
            .build();
    EqualityDeleteWriter<Record> deletes =
        factory.newEqualityDeleteWriter(
            OutputFileFactory.builderFor(table, 0, 99)
                .format(FileFormat.PARQUET)
                .build()
                .newOutputFile(table.spec(), eu),
            table.spec(),
            eu);
    try (deletes) {
      for (long id : new long[] {0, 3, 6, 9}) {
        GenericRecord record = GenericRecord.create(idOnly);
        record.setField("id", id);
        deletes.write(record);
      }
    }
    table.newRowDelta().addDeletes(deletes.toDeleteFile()).commit();

    Result result = run(pathInput(), 2);

    assertEquals(0, result.errors);
    assertEquals(1496, result.rows.size());
    Set<Long> ids = result.ids();
    assertEquals(1496, ids.size());
    for (long id : new long[] {0, 3, 6, 9}) {
      assertTrue(!ids.contains(id), "row " + id + " was deleted");
    }
    assertTrue(ids.contains(1L) && ids.contains(12L));
  }

  @Test
  void tableModeWithHadoopCatalog() throws Exception {
    LakeCatalog lake = new LakeCatalog();
    lake.setName("lake");
    lake.setCatalogName("lake");
    lake.setCatalogType(LakeCatalog.TYPE_HADOOP);
    lake.setWarehouse(warehouse());
    metadataProvider.getSerializer(LakeCatalog.class).save(lake);

    LakeTableInputMeta meta = new LakeTableInputMeta();
    meta.setFormat(LakeFormats.FORMAT_ICEBERG);
    meta.setIdentifierMode(LakeTableInputMeta.MODE_TABLE);
    meta.setCatalogMetadataName("lake");
    meta.setTableIdentifier("lake.sales.orders");

    Result result = run(meta, 2);

    assertEquals(0, result.errors);
    assertEquals(1500, result.ids().size());
  }

  @Test
  void deltaIsNotSupportedOnTheLocalEngine() throws Exception {
    LakeTableInputMeta meta = pathInput();
    meta.setFormat(LakeFormats.FORMAT_DELTA);

    assertTrue(run(meta, 1).errors > 0);
  }

  @Test
  void timestampFormats() throws Exception {
    assertEquals(1234L, LakeTableInput.parseTimestamp("1234"));
    assertEquals(
        LocalDateTime.of(2026, 10, 1, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
        LakeTableInput.parseTimestamp("2026-10-01T12:00:00Z"));
    assertEquals(
        LocalDateTime.of(2026, 10, 1, 12, 0, 0, 500_000_000)
            .atZone(ZoneId.systemDefault())
            .toInstant()
            .toEpochMilli(),
        LakeTableInput.parseTimestamp("2026-10-01 12:00:00.5"));
    assertThrows(HopException.class, () -> LakeTableInput.parseTimestamp("yesterday"));
    assertThrows(HopException.class, () -> LakeTableInput.parseTimestamp(""));
  }

  @Test
  void timestampsWithoutZoneKeepTheirWallClockTimeInEveryZone() throws Exception {
    TimeZone original = TimeZone.getDefault();
    try {
      for (String zone : List.of("America/Los_Angeles", "Europe/Berlin", "Asia/Kolkata")) {
        TimeZone.setDefault(TimeZone.getTimeZone(zone));

        Result result = run(pathInput(), 1);

        assertEquals(0, result.errors, zone);
        Object[] row =
            result.rows.stream().filter(r -> (Long) r[0] == 7L).findFirst().orElseThrow();
        assertEquals(
            LocalDateTime.of(2026, 10, 1, 7, 0), ((Timestamp) row[3]).toLocalDateTime(), zone);
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  void nonParquetDataFilesGiveAClearError() throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    Table avro =
        catalog.createTable(
            TableIdentifier.of("sales", "avro_orders"),
            schema,
            PartitionSpec.unpartitioned(),
            Map.of("format-version", "2", "write.format.default", "avro"));
    var writer =
        new GenericFileWriterFactory.Builder(avro)
            .dataFileFormat(FileFormat.AVRO)
            .build()
            .newDataWriter(
                OutputFileFactory.builderFor(avro, 0, 0)
                    .format(FileFormat.AVRO)
                    .build()
                    .newOutputFile(),
                avro.spec(),
                null);
    try (writer) {
      GenericRecord record = GenericRecord.create(schema);
      record.setField("id", 1L);
      writer.write(record);
    }
    avro.newAppend().appendFile(writer.toDataFile()).commit();

    try (IcebergRowReader reader = new IcebergRowReader(avro, null, null, null, 0, 1)) {
      UnsupportedOperationException e =
          assertThrows(UnsupportedOperationException.class, reader::hasNext);
      assertTrue(e.getMessage().contains("AVRO format"), e.getMessage());
      assertTrue(e.getMessage().contains("Only Parquet data files"), e.getMessage());
    }
  }

  @Test
  void copiesShareTheCurrentSnapshot() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("read");
    TransformMeta transformMeta = new TransformMeta("input", pathInput());
    transformMeta.setCopies(2);
    pipelineMeta.addTransform(transformMeta);
    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.setMetadataProvider(metadataProvider);
    pipeline.prepareExecution();
    LakeTableInput first = (LakeTableInput) pipeline.getTransform("input", 0);
    LakeTableInput second = (LakeTableInput) pipeline.getTransform("input", 1);

    long before = table.currentSnapshot().snapshotId();
    assertEquals(before, first.sharedCurrentSnapshotId(table));

    // A commit lands after the first copy resolved the snapshot, before the second one starts.
    append(1500, 1600);
    table.refresh();
    assertTrue(table.currentSnapshot().snapshotId() != before);

    assertEquals(before, second.sharedCurrentSnapshotId(table));
  }
}
