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
package org.apache.hop.pipeline.transforms.zipfile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.compress.gzip.GzipCompressionLevel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The compression level Zip file writes its entries with. The content repeats enough for the levels
 * to give another size, so the compressed size of an entry tells which level wrote it.
 */
class ZipFileCompressionTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path tempDir;

  private TransformMockHelper<ZipFileMeta, ZipFileData> helper;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>("ZipFileCompressionTest", ZipFileMeta.class, ZipFileData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.logChannelFactory.create(any())).thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void theLevelsGiveAnotherSizeForTheTestContent() throws Exception {
    assertTrue(deflatedSize(1) > deflatedSize(6));
    assertTrue(deflatedSize(6) > deflatedSize(9));
  }

  /** A pipeline saved before the level could be chosen compresses as hard as it always did. */
  @Test
  void transformSavedWithoutALevelCompressesAtLevelNine() throws Exception {
    ZipFileMeta meta = meta();

    assertNull(meta.getCompressionLevel());
    Path zip = zip(meta, "saved.zip");

    assertEntry(zip, 9);
  }

  @Test
  void transformLoadedWithoutALevelCompressesAtLevelNine() throws Exception {
    ZipFileMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/zip-file-without-compression-level.xml", ZipFileMeta.class);

    assertNull(meta.getCompressionLevel());
    assertEquals(GzipCompressionLevel.SMALLEST, meta.compressionLevelOrNotStored());
    assertFalse(meta.getXml().contains("compression_level>"), "no level appears by loading");
    assertEntry(zip(meta, "loaded.zip"), 9);
  }

  @Test
  void theLevelIsSavedAndLoaded() throws Exception {
    ZipFileMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/zip-file-with-compression-level.xml", ZipFileMeta.class);

    assertEquals(GzipCompressionLevel.FASTEST, meta.getCompressionLevel());
    assertTrue(meta.getXml().contains("<compression_level>FASTEST</compression_level>"));
    assertEntry(zip(meta, "loaded.zip"), 1);
  }

  @Test
  void newTransformCompressesAtLevelSix() throws Exception {
    ZipFileMeta meta = meta();
    meta.setDefault();

    assertEquals(GzipCompressionLevel.STANDARD, meta.getCompressionLevel());
    assertEntry(zip(meta, "new.zip"), 6);
  }

  @ParameterizedTest
  @EnumSource(GzipCompressionLevel.class)
  void entryIsCompressedAtTheChosenLevel(GzipCompressionLevel level) throws Exception {
    ZipFileMeta meta = meta();
    meta.setCompressionLevel(level);

    assertEntry(zip(meta, level + ".zip"), level.getLevel());
  }

  /** Updating an archive keeps what is in it and adds the new entry at the chosen level. */
  @Test
  void updatedArchiveKeepsItsEntriesAndAddsTheNewOneAtTheChosenLevel() throws Exception {
    Path zip = tempDir.resolve("update.zip");
    try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(zip))) {
      out.putNextEntry(new ZipEntry("old.txt"));
      out.write("old".getBytes(StandardCharsets.UTF_8));
      out.closeEntry();
    }
    ZipFileMeta meta = meta();
    meta.setOverwriteZipEntry(true); // update the existing zip instead of replacing it
    meta.setCompressionLevel(GzipCompressionLevel.FASTEST);

    zip(meta, "update.zip");

    try (java.util.zip.ZipFile archive = new java.util.zip.ZipFile(zip.toFile())) {
      assertEquals(2, archive.size());
      try (InputStream in = archive.getInputStream(archive.getEntry("old.txt"))) {
        assertEquals("old", new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
      ZipEntry added = archive.getEntry("source.txt");
      assertEquals(deflatedSize(1), added.getCompressedSize());
      try (InputStream in = archive.getInputStream(added)) {
        assertArrayEquals(content(), in.readAllBytes());
      }
    }
  }

  private static ZipFileMeta meta() {
    ZipFileMeta meta = new ZipFileMeta();
    meta.setSourceFilenameField("source");
    meta.setTargetFilenameField("zip");
    return meta;
  }

  private Path zip(ZipFileMeta meta, String name) throws Exception {
    Path source = tempDir.resolve("source.txt");
    Files.write(source, content());
    Path zip = tempDir.resolve(name);

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("source"));
    rowMeta.addValueMeta(new ValueMetaString("zip"));
    ZipFile transform =
        new ZipFile(
            helper.transformMeta, meta, new ZipFileData(), 0, helper.pipelineMeta, helper.pipeline);
    BlockingRowSet input = new BlockingRowSet(1);
    input.putRow(rowMeta, new Object[] {source.toString(), zip.toString()});
    input.setDone();
    transform.setInputRowSets(new ArrayList<>(List.of(input)));
    transform.setOutputRowSets(List.of(new BlockingRowSet(2)));
    while (transform.processRow()) {
      // Process all rows. A row that fails stops the transform.
    }
    assertEquals(0, transform.getErrors());
    return zip;
  }

  /** The archive has the one entry, with the content of the source and the size of the level. */
  private static void assertEntry(Path zip, int level) throws Exception {
    try (java.util.zip.ZipFile archive = new java.util.zip.ZipFile(zip.toFile())) {
      assertEquals(1, archive.size());
      ZipEntry entry = archive.entries().nextElement();
      assertEquals(ZipEntry.DEFLATED, entry.getMethod());
      assertEquals(deflatedSize(level), entry.getCompressedSize());
      try (InputStream in = archive.getInputStream(entry)) {
        assertEquals(
            new String(content(), StandardCharsets.UTF_8),
            new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
    }
  }

  /** The size java.util.zip gives a zip entry with this content at a level. */
  private static long deflatedSize(int level) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    Deflater deflater = new Deflater(level, true);
    try (DeflaterOutputStream out = new DeflaterOutputStream(bytes, deflater)) {
      out.write(content());
    } finally {
      deflater.end();
    }
    return bytes.size();
  }

  private static byte[] content() {
    StringBuilder content = new StringBuilder();
    for (int i = 0; i < 20_000; i++) {
      content
          .append(i)
          .append(";customer ")
          .append(i % 97)
          .append(";city ")
          .append(i * 31 % 113)
          .append(';')
          .append((i % 1000) * 1.25)
          .append('\n');
    }
    return content.toString().getBytes(StandardCharsets.UTF_8);
  }
}
