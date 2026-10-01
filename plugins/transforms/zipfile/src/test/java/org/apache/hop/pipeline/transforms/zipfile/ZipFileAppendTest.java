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

package org.apache.hop.pipeline.transforms.zipfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Updating an existing zip file must not go through the shared system temp folder. */
class ZipFileAppendTest {

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
    helper = new TransformMockHelper<>("ZipFileAppendTest", ZipFileMeta.class, ZipFileData.class);
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
  void updateKeepsExistingEntriesAndUsesTheZipFolderForTheTemporaryFile() throws Exception {
    Path source = Files.createDirectories(tempDir.resolve("source")).resolve("new.txt");
    Files.writeString(source, "new", StandardCharsets.UTF_8);
    Path zipDir = Files.createDirectories(tempDir.resolve("archive"));
    Path zip = zipDir.resolve("archive.zip");
    writeZip(zip, "old.txt", "old");

    ZipFileMeta meta = new ZipFileMeta();
    meta.setDefault();
    meta.setSourceFilenameField("source");
    meta.setTargetFilenameField("zip");
    meta.setOverwriteZipEntry(true); // update the existing zip instead of replacing it

    ZipFile transform =
        new ZipFile(
            helper.transformMeta, meta, new ZipFileData(), 0, helper.pipelineMeta, helper.pipeline);
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("source"));
    rowMeta.addValueMeta(new ValueMetaString("zip"));
    BlockingRowSet input = new BlockingRowSet(2);
    input.putRow(rowMeta, new Object[] {source.toUri().toString(), zip.toUri().toString()});
    input.setDone();
    transform.setInputRowSets(new ArrayList<>(List.of(input)));

    List<File> tempFolders = new ArrayList<>();
    try (MockedStatic<File> files = Mockito.mockStatic(File.class, Mockito.CALLS_REAL_METHODS)) {
      files
          .when(() -> File.createTempFile(anyString(), any(), any()))
          .thenAnswer(
              invocation -> {
                tempFolders.add(invocation.getArgument(2));
                return invocation.callRealMethod();
              });
      PipelineTestingUtil.execute(transform, 1, false);
    }

    assertEquals(0, transform.getErrors());
    assertEquals(Set.of("old.txt", "new.txt"), entries(zip));
    assertEquals(List.of(zipDir.toFile()), tempFolders, "temporary file folder");
    assertEquals(
        List.of(zip), Files.list(zipDir).toList(), "no temporary file should be left behind");
  }

  private static void writeZip(Path zip, String entry, String content) throws Exception {
    try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(zip))) {
      out.putNextEntry(new ZipEntry(entry));
      out.write(content.getBytes(StandardCharsets.UTF_8));
      out.closeEntry();
    }
  }

  private static Set<String> entries(Path zip) throws Exception {
    Set<String> names = new TreeSet<>();
    try (java.util.zip.ZipFile zipFile = new java.util.zip.ZipFile(zip.toFile())) {
      zipFile.stream().forEach(entry -> names.add(entry.getName()));
    }
    return names;
  }
}
