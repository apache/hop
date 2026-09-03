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
package org.apache.hop.parquet.transforms.output;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/** Tests for the Hive-style partitioned output of {@link ParquetOutput}. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ParquetOutputPartitionTest {

  @TempDir private Path tempDir;

  private TransformMockHelper<ParquetOutputMeta, ParquetOutputData> mockHelper;

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>(
            "Parquet Output", ParquetOutputMeta.class, ParquetOutputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void partitionPathUsesHiveStyleNameEqualsValue() throws Exception {
    ParquetOutputMeta meta = partitionedMeta("region", "year");
    ParquetOutput output = createTransform(meta, new ParquetOutputData());
    output.setInputRowMeta(salesRowMeta());
    output.resolveOutputFields();

    assertEquals(
        "region=EU/year=2026",
        output.partitionPath(salesRowMeta(), new Object[] {1L, "EU", "2026"}));
  }

  @Test
  void partitionPathUsesTheHiveDefaultNameForNullAndEmptyValues() throws Exception {
    ParquetOutputMeta meta = partitionedMeta("region");
    ParquetOutput output = createTransform(meta, new ParquetOutputData());
    output.setInputRowMeta(salesRowMeta());
    output.resolveOutputFields();

    assertEquals(
        "region=" + ParquetOutput.DEFAULT_PARTITION_NAME,
        output.partitionPath(salesRowMeta(), new Object[] {1L, null, "2026"}));
    assertEquals(
        "region=" + ParquetOutput.DEFAULT_PARTITION_NAME,
        output.partitionPath(salesRowMeta(), new Object[] {1L, "", "2026"}));
  }

  @Test
  void pathHostileCharactersAreEscaped() {
    // A value containing a separator must not create extra folder levels.
    assertEquals("a%2Fb", ParquetOutput.escapePathValue("a/b"));
    assertEquals("a%5Cb", ParquetOutput.escapePathValue("a\\b"));
    assertEquals("a%3Db", ParquetOutput.escapePathValue("a=b"));
    assertEquals("a%3Ab", ParquetOutput.escapePathValue("a:b"));
    assertEquals("a%09b", ParquetOutput.escapePathValue("a\tb"));
    assertEquals("100%25", ParquetOutput.escapePathValue("100%"));
    assertEquals("plain-value_1", ParquetOutput.escapePathValue("plain-value_1"));
    // VFS percent-decodes the path it is handed, so our own escapes are doubled on the way out
    assertEquals("region=EU%252FWest", ParquetOutput.toVfsPath("region=EU%2FWest"));
  }

  @Test
  void partitionFieldsAreNotWrittenIntoTheFile() throws Exception {
    ParquetOutputMeta meta = partitionedMeta("region");
    ParquetOutputData data = new ParquetOutputData();
    ParquetOutput output = createTransform(meta, data);
    output.setInputRowMeta(salesRowMeta());

    output.resolveOutputFields();

    assertEquals(2, data.outputFields.size());
    assertEquals("id", data.outputFields.get(0).getSourceFieldName());
    assertEquals("year", data.outputFields.get(1).getSourceFieldName());
    assertEquals(List.of(1), data.partitionFieldIndexes);
  }

  @Test
  void partitioningOnEveryFieldIsRejected() throws Exception {
    ParquetOutputMeta meta = partitionedMeta("id", "region", "year");
    ParquetOutput output = createTransform(meta, new ParquetOutputData());
    output.setInputRowMeta(salesRowMeta());

    HopException e = assertThrows(HopException.class, output::resolveOutputFields);
    assertTrue(e.getMessage().contains("nothing to write"));
  }

  @Test
  void unknownPartitionFieldIsRejected() throws Exception {
    ParquetOutputMeta meta = partitionedMeta("nope");
    ParquetOutput output = createTransform(meta, new ParquetOutputData());
    output.setInputRowMeta(salesRowMeta());

    HopException e = assertThrows(HopException.class, output::resolveOutputFields);
    assertTrue(e.getMessage().contains("nope"));
  }

  @Test
  void duplicatePartitionFieldIsRejected() throws Exception {
    ParquetOutputMeta meta = partitionedMeta("region", "region");
    ParquetOutput output = createTransform(meta, new ParquetOutputData());
    output.setInputRowMeta(salesRowMeta());

    HopException e = assertThrows(HopException.class, output::resolveOutputFields);
    assertTrue(e.getMessage().contains("more than once"));
  }

  @Test
  void writesOneFolderPerPartitionAndOmitsThePartitionColumn() throws Exception {
    ParquetOutputMeta meta = partitionedMeta("region");
    meta.setFilenameBase(tempDir.resolve("sales").toString());

    runRows(
        meta,
        new Object[] {1L, "EU", "2026"},
        new Object[] {2L, "US", "2026"},
        new Object[] {3L, "EU", "2026"});

    // Hive-style layout, one folder per distinct value.
    assertEquals(List.of("region=EU", "region=US"), names(childFolders(tempDir.resolve("sales"))));

    // The written file no longer carries the partition column ...
    Path euFile = onlyFile(tempDir.resolve("sales/region=EU"));
    IRowMeta schema = ParquetTestUtil.readSchema(euFile.toString());
    assertEquals(2, schema.size());
    assertEquals("id", schema.getValueMeta(0).getName());
    assertEquals("year", schema.getValueMeta(1).getName());
    assertEquals(-1, schema.indexOfValue("region"));

    // ... and both EU rows are in that one file.
    List<RowMetaAndData> rows = readRows(euFile, "id", "year");
    assertEquals(2, rows.size());
    assertEquals(1L, rows.get(0).getInteger("id", -1L));
    assertEquals(3L, rows.get(1).getInteger("id", -1L));
  }

  @Test
  void aPartitionValueWithASeparatorStaysOneFolderLevel() throws Exception {
    ParquetOutputMeta meta = partitionedMeta("region");
    meta.setFilenameBase(tempDir.resolve("sales").toString());

    runRows(meta, new Object[] {1L, "EU/West", "2026"});

    assertEquals(List.of("region=EU%2FWest"), names(childFolders(tempDir.resolve("sales"))));
  }

  @Test
  void appendModeLeavesEarlierFilesInPlace() throws Exception {
    ParquetOutputMeta meta = partitionedMeta("region");
    meta.setFilenameBase(tempDir.resolve("sales").toString());
    meta.setWriteMode(ParquetWriteMode.Append);

    runRows(meta, new Object[] {1L, "EU", "2026"});
    runRows(meta, new Object[] {2L, "EU", "2026"});

    assertEquals(2, listFiles(tempDir.resolve("sales/region=EU")).size());
  }

  @Test
  void overwritePartitionsModeEmptiesOnlyTheTouchedPartition() throws Exception {
    ParquetOutputMeta meta = partitionedMeta("region");
    meta.setFilenameBase(tempDir.resolve("sales").toString());

    meta.setWriteMode(ParquetWriteMode.Append);
    runRows(meta, new Object[] {1L, "EU", "2026"}, new Object[] {2L, "US", "2026"});
    assertEquals(1, listFiles(tempDir.resolve("sales/region=EU")).size());
    assertEquals(1, listFiles(tempDir.resolve("sales/region=US")).size());

    meta.setWriteMode(ParquetWriteMode.OverwritePartitions);
    runRows(meta, new Object[] {3L, "EU", "2026"});

    // EU was rewritten, US was not touched.
    assertEquals(1, listFiles(tempDir.resolve("sales/region=EU")).size());
    assertEquals(1, listFiles(tempDir.resolve("sales/region=US")).size());
    List<RowMetaAndData> euRows = readRows(onlyFile(tempDir.resolve("sales/region=EU")), "id");
    assertEquals(1, euRows.size());
    assertEquals(3L, euRows.get(0).getInteger("id", -1L));
  }

  @Test
  void overwritePartitionsDoesNotDeleteWhatTheSameRunJustWrote() throws Exception {
    ParquetOutputMeta meta = partitionedMeta("region");
    meta.setFilenameBase(tempDir.resolve("sales").toString());
    meta.setWriteMode(ParquetWriteMode.OverwritePartitions);
    // One open partition at a time forces EU to be closed and reopened between the two EU rows.
    meta.setMaxOpenPartitions("1");

    runRows(
        meta,
        new Object[] {1L, "EU", "2026"},
        new Object[] {2L, "US", "2026"},
        new Object[] {3L, "EU", "2026"});

    // Two files for EU because it was reopened, and the first was not wiped by the second.
    assertEquals(2, listFiles(tempDir.resolve("sales/region=EU")).size());
  }

  @Test
  void failIfExistsModeRefusesAnExistingPartition() throws Exception {
    ParquetOutputMeta meta = partitionedMeta("region");
    meta.setFilenameBase(tempDir.resolve("sales").toString());

    meta.setWriteMode(ParquetWriteMode.Append);
    runRows(meta, new Object[] {1L, "EU", "2026"});

    meta.setWriteMode(ParquetWriteMode.FailIfExists);
    HopException e =
        assertThrows(HopException.class, () -> runRows(meta, new Object[] {2L, "EU", "2026"}));
    assertTrue(e.getMessage().contains("already exists"));
  }

  @Test
  void overwriteAllModeEmptiesEveryPartition() throws Exception {
    ParquetOutputMeta meta = partitionedMeta("region");
    meta.setFilenameBase(tempDir.resolve("sales").toString());

    meta.setWriteMode(ParquetWriteMode.Append);
    runRows(meta, new Object[] {1L, "EU", "2026"}, new Object[] {2L, "US", "2026"});

    meta.setWriteMode(ParquetWriteMode.OverwriteAll);
    runRows(meta, new Object[] {3L, "APAC", "2026"});

    assertEquals(List.of("region=APAC"), names(childFolders(tempDir.resolve("sales"))));
  }

  @Test
  void moreDistinctValuesThanOpenFilesStillWritesEveryRow() throws Exception {
    ParquetOutputMeta meta = partitionedMeta("region");
    meta.setFilenameBase(tempDir.resolve("sales").toString());
    meta.setMaxOpenPartitions("2");

    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      rows.add(new Object[] {(long) i, "r" + i, "2026"});
    }
    runRows(meta, rows.toArray(new Object[0][]));

    assertEquals(6, childFolders(tempDir.resolve("sales")).size());
    for (int i = 0; i < 6; i++) {
      Path folder = tempDir.resolve("sales/region=r" + i);
      assertEquals(1, listFiles(folder).size(), "expected one file in " + folder);
    }
  }

  @Test
  void withoutPartitionFieldsNothingChanges() throws Exception {
    ParquetOutputMeta meta = new ParquetOutputMeta();
    meta.setFilenameBase(tempDir.resolve("flat").toString());
    meta.setCompressionCodec(CompressionCodecName.UNCOMPRESSED);
    meta.setFilenameIncludingCopyNr(false);
    meta.setFilenameIncludingSplitNr(false);
    meta.setRowGroupSize("4096");
    meta.setDataPageSize("1024");
    meta.setDictionaryPageSize("512");

    assertFalse(meta.isPartitioning());
    runRows(meta, new Object[] {1L, "EU", "2026"});

    // A single file at the base name, no folders, and the region column is still written.
    Path file = tempDir.resolve("flat.parquet");
    assertTrue(Files.exists(file), "expected " + file);
    assertEquals(3, ParquetTestUtil.readSchema(file.toString()).size());
  }

  // ------------------------------------------------------------------------------------------

  private ParquetOutputMeta partitionedMeta(String... partitionFields) {
    ParquetOutputMeta meta = new ParquetOutputMeta();
    meta.setCompressionCodec(CompressionCodecName.UNCOMPRESSED);
    meta.setFilenameIncludingSplitNr(false);
    // The default row group size is 256MB and every open writer buffers up to one row group, so
    // the tests that open several partitions at once would otherwise reserve gigabytes.
    meta.setRowGroupSize("4096");
    meta.setDataPageSize("1024");
    meta.setDictionaryPageSize("512");
    for (String field : partitionFields) {
      meta.getPartitionFields().add(new ParquetPartitionField(field));
    }
    return meta;
  }

  private static IRowMeta salesRowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("region"));
    rowMeta.addValueMeta(new ValueMetaString("year"));
    return rowMeta;
  }

  /** Feeds the given rows through a fresh transform instance and closes its files. */
  private void runRows(ParquetOutputMeta meta, Object[]... rows) throws Exception {
    ParquetOutputData data = new ParquetOutputData();
    ParquetOutput output = spy(createTransform(meta, data));
    output.setInputRowMeta(salesRowMeta());
    assertTrue(output.init());

    List<Object[]> remaining = new ArrayList<>(List.of(rows));
    doNothing().when(output).putRow(any(), any());
    // doAnswer(...).when(spy) rather than when(spy.getRow()): the latter invokes the real getRow()
    // while stubbing, which blocks forever in waitUntilPipelineIsStarted().
    doAnswer(invocation -> remaining.isEmpty() ? null : remaining.remove(0)).when(output).getRow();

    while (output.processRow()) {
      // keep going until the null row closes the files
    }
  }

  private ParquetOutput createTransform(ParquetOutputMeta meta, ParquetOutputData data) {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta("Parquet Output", meta);
    pipelineMeta.addTransform(transformMeta);
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    return new ParquetOutput(transformMeta, meta, data, 0, pipelineMeta, pipeline);
  }

  private static List<RowMetaAndData> readRows(Path file, String... fieldNames) throws Exception {
    List<org.apache.hop.parquet.transforms.input.ParquetField> fields = new ArrayList<>();
    for (String name : fieldNames) {
      fields.add(
          new org.apache.hop.parquet.transforms.input.ParquetField(
              name, name, "id".equals(name) ? "Integer" : "String", null, "0", "0"));
    }
    return ParquetTestUtil.readAllRows(file.toString(), fields);
  }

  private static List<Path> childFolders(Path parent) throws IOException {
    try (Stream<Path> stream = Files.list(parent)) {
      return stream.filter(Files::isDirectory).sorted(Comparator.naturalOrder()).toList();
    }
  }

  private static List<Path> listFiles(Path folder) throws IOException {
    try (Stream<Path> stream = Files.list(folder)) {
      return stream.filter(Files::isRegularFile).sorted(Comparator.naturalOrder()).toList();
    }
  }

  private static Path onlyFile(Path folder) throws IOException {
    List<Path> files = listFiles(folder);
    assertEquals(1, files.size(), "expected exactly one file in " + folder);
    return files.get(0);
  }

  private static List<String> names(List<Path> paths) {
    return paths.stream().map(p -> p.getFileName().toString()).toList();
  }
}
