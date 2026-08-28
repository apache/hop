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

package org.apache.hop.pipeline.transforms.filemetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers reading a CSV from inside an archive through a layered VFS URI, and the two ways such a
 * URI can be malformed.
 */
class FileMetadataArchiveTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String ENTRY_NAME = "report.csv";
  private static final String CSV_CONTENT = "colA;colB;colC\n1;2;3\n4;5;6\n7;8;9\n";

  @TempDir private Path tempDir;

  private TransformMockHelper<FileMetadataMeta, FileMetadataData> mockHelper;
  private String zipUri;

  @BeforeEach
  void setUp() throws IOException {
    mockHelper =
        new TransformMockHelper<>("FileMetadata", FileMetadataMeta.class, FileMetadataData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);

    Path zipFile = tempDir.resolve("report.zip");
    try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(zipFile))) {
      zip.putNextEntry(new ZipEntry(ENTRY_NAME));
      zip.write(CSV_CONTENT.getBytes(StandardCharsets.UTF_8));
      zip.closeEntry();
    }
    zipUri = "zip:" + zipFile.toUri();
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void readsCsvFromInsideZipArchive() throws Exception {
    BlockingRowSet output = new BlockingRowSet(10);
    FileMetadata transform = createTransform(zipUri + "!/" + ENTRY_NAME, output);

    assertFalse(transform.processRow(), "the transform generates its rows and is then done");

    // one row per detected column
    Object[] first = output.getRowWait(1, TimeUnit.SECONDS);
    assertNotNull(first, "expected metadata rows for the CSV inside the archive");
    assertEquals("US-ASCII", first[0].toString());
    assertEquals(';', first[1]);
    assertEquals("colA", first[7]);

    assertEquals("colB", output.getRowWait(1, TimeUnit.SECONDS)[7]);
    assertEquals("colC", output.getRowWait(1, TimeUnit.SECONDS)[7]);
  }

  @Test
  void archiveWithoutEntryNameFailsWithAnActionableMessage() {
    FileMetadata transform = createTransform(zipUri, new BlockingRowSet(10));

    HopException exception = assertThrows(HopException.class, transform::processRow);

    String expected =
        BaseMessages.getString(FileMetadata.class, "FileMetadata.Exception.NotAFile", zipUri);
    assertEquals(expected.trim(), exception.getMessage().trim());
  }

  @Test
  void unknownEntryNameLogsInsteadOfFailingSilently() throws Exception {
    OutputStream log = new ByteArrayOutputStream();
    mockHelper.redirectLog(log, LogLevel.BASIC);

    String missing = zipUri + "!/does-not-exist.csv";
    BlockingRowSet output = new BlockingRowSet(10);
    FileMetadata transform = createTransform(missing, output);

    assertFalse(transform.processRow(), "a missing file must not abort the pipeline");

    Object[] row = output.getRowWait(1, TimeUnit.SECONDS);
    assertNotNull(row, "an empty row is still emitted");
    assertNull(row[0], "the row carries no metadata");

    assertTrue(
        log.toString().contains(missing),
        () -> "the log should name the file that was not found, but was: " + log);
  }

  private FileMetadata createTransform(String fileName, BlockingRowSet output) {
    FileMetadataMeta meta = new FileMetadataMeta();
    meta.setDefault();
    meta.setFileName(fileName);

    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    FileMetadata transform =
        new FileMetadata(
            mockHelper.transformMeta,
            meta,
            new FileMetadataData(),
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    transform.addRowSetToOutputRowSets(output);
    return transform;
  }
}
