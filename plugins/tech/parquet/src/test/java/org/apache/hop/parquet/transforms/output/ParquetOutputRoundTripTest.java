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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
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

/** Writes files through {@link ParquetOutput} and reads them back with the input transform. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ParquetOutputRoundTripTest {

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
  void everySupportedTypeSurvivesTheRoundTrip() throws Exception {
    ParquetOutputMeta meta = singleFileMeta();
    Date date = new Date(1_700_000_000_000L);
    Timestamp timestamp = new Timestamp(1_700_000_000_123L);

    runRows(
        meta,
        new Object[] {
          1L,
          2.5D,
          true,
          date,
          timestamp,
          "text",
          new BigDecimal("12345.6789"),
          "bytes".getBytes(StandardCharsets.UTF_8)
        },
        new Object[] {null, null, null, null, null, null, null, null});

    Path file = onlyFile(tempDir);
    IRowMeta schema = ParquetTestUtil.readSchema(file.toString());
    assertEquals(8, schema.size());

    List<RowMetaAndData> rows = readRows(file);
    assertEquals(2, rows.size());

    RowMetaAndData row = rows.get(0);
    assertEquals(1L, row.getInteger("id", -1L));
    assertEquals(2.5D, row.getNumber("amount", -1D), 0.0001);
    assertTrue(row.getBoolean("flag", false));
    assertEquals(date, row.getDate("d", null));
    // The timestamp-millis logical type keeps millisecond precision.
    assertEquals(timestamp.getTime(), row.getDate("ts", null).getTime());
    assertEquals("text", row.getString("name", null));
    assertEquals("12345.6789", row.getString("big", null));
    assertEquals("bytes", new String(row.getBinary("bin", null), StandardCharsets.UTF_8));

    RowMetaAndData nulls = rows.get(1);
    for (int i = 0; i < nulls.size(); i++) {
      assertTrue(nulls.isEmptyValue(nulls.getValueMeta(i).getName()));
    }
  }

  @Test
  void splitsFilesByRowCount() throws Exception {
    ParquetOutputMeta meta = singleFileMeta();
    meta.setFilenameIncludingSplitNr(true);
    meta.setFileSplitSize("2");

    runRows(meta, row(1), row(2), row(3), row(4), row(5));

    List<Path> files = listFiles(tempDir);
    assertEquals(3, files.size(), files.toString());
    assertEquals(2, readRows(files.get(0)).size());
    assertEquals(2, readRows(files.get(1)).size());
    assertEquals(1, readRows(files.get(2)).size());
  }

  @Test
  void logsRowGroupsAndFooterSizePerFileWhenDetailed() throws Exception {
    when(mockHelper.iLogChannel.isDetailed()).thenReturn(true);
    ParquetOutputMeta meta = singleFileMeta();

    runRows(meta, row(1), row(2), row(3));

    verify(mockHelper.iLogChannel).logDetailed(contains("3 rows in 1 row group(s), footer "));
  }

  @Test
  void warnsAboutARowGroupSizeThatIsReallyARowCount() {
    ParquetOutputMeta meta = singleFileMeta();
    // What Hop 2.3 and 2.4 saved into every pipeline.
    meta.setRowGroupSize("20000");

    ParquetOutput output = createTransform(meta, new ParquetOutputData());
    assertTrue(output.init());

    verify(mockHelper.iLogChannel).logBasic(contains("row group size of 20,000 bytes"));
  }

  @Test
  void closingWithoutAnyRowsIsHarmless() throws Exception {
    ParquetOutputMeta meta = singleFileMeta();
    ParquetOutput output = spy(createTransform(meta, new ParquetOutputData()));
    output.setInputRowMeta(rowMeta());
    assertTrue(output.init());

    // The single-threaded executor and Beam call this even when no row ever arrived.
    output.batchComplete();
    output.finishBundle();

    assertNull(onlyFileOrNull(tempDir));
  }

  @Test
  void parquetVersionOneIsReadableToo() throws Exception {
    ParquetOutputMeta meta = singleFileMeta();
    meta.setVersion(ParquetVersion.Version1);
    meta.setCompressionCodec(CompressionCodecName.SNAPPY);

    runRows(meta, row(1), row(2));

    assertEquals(2, readRows(onlyFile(tempDir)).size());
  }

  @Test
  void beamBundleHooksCloseAndReopenTheFile() throws Exception {
    ParquetOutputMeta meta = singleFileMeta();
    meta.setFilenameIncludingSplitNr(true);
    meta.setFileSplitSize("0");

    ParquetOutput output = spy(createTransform(meta, new ParquetOutputData()));
    output.setInputRowMeta(rowMeta());
    assertTrue(output.init());
    doNothing().when(output).putRow(any(), any());
    List<Object[]> remaining = new ArrayList<>();
    remaining.add(row(1));
    doAnswer(invocation -> remaining.isEmpty() ? null : remaining.remove(0)).when(output).getRow();

    // Before the first row there is nothing to open or close.
    output.startBundle();
    assertTrue(output.processRow());
    output.finishBundle();
    assertEquals(1, listFiles(tempDir).size());

    // A new bundle gets the next split file.
    output.startBundle();
    remaining.add(row(2));
    assertTrue(output.processRow());
    output.finishBundle();
    assertEquals(2, listFiles(tempDir).size());
  }

  @Test
  void failsClearlyWhenTheFileCannotBeCreated() throws Exception {
    // The parent "folder" is an existing file, so the output cannot be opened.
    Path blocker = tempDir.resolve("blocker");
    Files.writeString(blocker, "not a folder");
    ParquetOutputMeta meta = singleFileMeta();
    meta.setFilenameBase(blocker.resolve("out").toString());

    HopException e = assertThrows(HopException.class, () -> runRows(meta, row(1)));
    assertTrue(e.getMessage().contains("Unable to create output file"), e.getMessage());
  }

  private ParquetOutputMeta singleFileMeta() {
    ParquetOutputMeta meta = new ParquetOutputMeta();
    meta.setFilenameBase(tempDir.resolve("out").toString());
    meta.setCompressionCodec(CompressionCodecName.UNCOMPRESSED);
    meta.setFilenameIncludingSplitNr(false);
    meta.setFilenameIncludingCopyNr(false);
    return meta;
  }

  private static IRowMeta rowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaNumber("amount"));
    rowMeta.addValueMeta(new ValueMetaBoolean("flag"));
    rowMeta.addValueMeta(new ValueMetaDate("d"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("ts"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaBigNumber("big"));
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaBinary("bin"));
    return rowMeta;
  }

  private static Object[] row(long id) {
    return new Object[] {
      id, 1.0D, false, new Date(0), new Timestamp(0), "n" + id, BigDecimal.ONE, new byte[0]
    };
  }

  /** Feeds the given rows through a fresh transform instance and closes its files. */
  private void runRows(ParquetOutputMeta meta, Object[]... rows) throws Exception {
    ParquetOutput output = spy(createTransform(meta, new ParquetOutputData()));
    output.setInputRowMeta(rowMeta());
    assertTrue(output.init());

    List<Object[]> remaining = new ArrayList<>(List.of(rows));
    doNothing().when(output).putRow(any(), any());
    doAnswer(invocation -> remaining.isEmpty() ? null : remaining.remove(0)).when(output).getRow();

    while (output.processRow()) {
      // keep going until the null row closes the file
    }
  }

  private ParquetOutput createTransform(ParquetOutputMeta meta, ParquetOutputData data) {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta("Parquet Output", meta);
    pipelineMeta.addTransform(transformMeta);
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    return new ParquetOutput(transformMeta, meta, data, 0, pipelineMeta, pipeline);
  }

  private static List<RowMetaAndData> readRows(Path file) throws Exception {
    List<org.apache.hop.parquet.transforms.input.ParquetField> fields = new ArrayList<>();
    IRowMeta rowMeta = rowMeta();
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      fields.add(
          new org.apache.hop.parquet.transforms.input.ParquetField(
              valueMeta.getName(), valueMeta.getName(), valueMeta.getTypeDesc(), null, "-1", "-1"));
    }
    return ParquetTestUtil.readAllRows(file.toString(), fields);
  }

  private static List<Path> listFiles(Path folder) throws IOException {
    try (Stream<Path> stream = Files.list(folder)) {
      return stream.filter(Files::isRegularFile).sorted(Comparator.naturalOrder()).toList();
    }
  }

  private static Path onlyFile(Path folder) throws IOException {
    List<Path> files = listFiles(folder);
    assertEquals(1, files.size(), files.toString());
    return files.get(0);
  }

  private static Path onlyFileOrNull(Path folder) throws IOException {
    List<Path> files = listFiles(folder);
    return files.isEmpty() ? null : files.get(0);
  }
}
