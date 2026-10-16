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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/** Updating an existing zip file, locally and on a VFS file system. */
class ZipFileAppendTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path tempDir;

  private TransformMockHelper<ZipFileMeta, ZipFileData> helper;
  private String ramFolder;

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
    ramFolder = "ram:///" + UUID.randomUUID();
  }

  @AfterEach
  void tearDown() throws Exception {
    helper.cleanUp();
    HopVfs.getFileObject(ramFolder).deleteAll();
  }

  @Test
  void updateLocalZipKeepsExistingEntries() throws Exception {
    Path source = Files.createDirectories(tempDir.resolve("source")).resolve("new.txt");
    Files.writeString(source, "new", StandardCharsets.UTF_8);
    Path zipDir = Files.createDirectories(tempDir.resolve("archive"));
    Path zip = zipDir.resolve("archive.zip");
    writeZip(zip.toUri().toString(), "old.txt", "old");

    ZipFile transform = run(updateMeta(), source.toUri().toString(), zip.toUri().toString());

    assertEquals(0, transform.getErrors());
    assertEquals(Map.of("new.txt", "new", "old.txt", "old"), entries(zip.toUri().toString()));
    assertEquals(
        List.of(zip), Files.list(zipDir).toList(), "no temporary file should be left behind");
  }

  @Test
  void updateVfsZipKeepsExistingEntries() throws Exception {
    String source = ramFolder + "/source/new.txt";
    writeFile(source, "new");
    String zip = ramFolder + "/archive/archive.zip";
    writeZip(zip, "old.txt", "old");

    ZipFile transform = run(updateMeta(), source, zip);

    assertEquals(0, transform.getErrors());
    assertEquals(Map.of("new.txt", "new", "old.txt", "old"), entries(zip));
    assertEquals(List.of("archive.zip"), children(ramFolder + "/archive"));
  }

  @Test
  void updateVfsZipRowByRow() throws Exception {
    String a = ramFolder + "/source/a.txt";
    String b = ramFolder + "/source/b.txt";
    writeFile(a, "a");
    writeFile(b, "b");
    String zip = ramFolder + "/archive/archive.zip";

    ZipFileMeta meta = updateMeta();
    meta.setCreateParentFolder(true);
    ZipFile transform = run(meta, new String[] {a, zip}, new String[] {b, zip});

    assertEquals(0, transform.getErrors());
    assertEquals(Map.of("a.txt", "a", "b.txt", "b"), entries(zip));
  }

  @Test
  void overwriteZipEntryReplacesAnEntryWithTheSameName() throws Exception {
    String source = ramFolder + "/source/data.txt";
    writeFile(source, "new");
    String zip = ramFolder + "/archive/archive.zip";
    writeZip(zip, "data.txt", "old");

    ZipFile transform = run(updateMeta(), source, zip);

    assertEquals(0, transform.getErrors());
    assertEquals(Map.of("data.txt", "new"), entries(zip));
  }

  @Test
  void failedUpdateLeavesTheOriginalZipIntact() throws Exception {
    String source = ramFolder + "/source/new.txt";
    writeFile(source, "new");
    String zip = ramFolder + "/archive/archive.zip";
    writeZip(zip, "old.txt", incompressible());
    // Cut the zip off in the middle of its entry: copying the existing entries fails
    byte[] truncated;
    try (InputStream in = HopVfs.getInputStream(zip)) {
      truncated = Arrays.copyOf(in.readAllBytes(), 1000);
    }
    try (OutputStream out = HopVfs.getOutputStream(zip, false)) {
      out.write(truncated);
    }

    ZipFile transform = run(updateMeta(), source, zip);

    assertEquals(1, transform.getErrors());
    try (InputStream in = HopVfs.getInputStream(zip)) {
      assertArrayEquals(truncated, in.readAllBytes(), "the original zip must not change");
    }
    assertEquals(List.of("archive.zip"), children(ramFolder + "/archive"));
  }

  @Test
  void keepSourceFolderOnVfsUsesThePathRelativeToTheBaseFolder() throws Exception {
    String source = ramFolder + "/base/in/src.txt";
    writeFile(source, "src");
    String zip = ramFolder + "/archive.zip";

    ZipFileMeta meta = updateMeta();
    meta.setKeepSourceFolder(true);
    meta.setBaseFolderField("base");

    RowMeta rowMeta = rowMeta();
    rowMeta.addValueMeta(new ValueMetaString("base"));
    ZipFile transform = run(meta, rowMeta, new Object[] {source, zip, ramFolder + "/base"});

    assertEquals(0, transform.getErrors());
    assertEquals(Map.of("in/src.txt", "src"), entries(zip));
  }

  @Test
  void keepSourceFolderOnVfsLeavesTheSchemeOut() throws Exception {
    String source = ramFolder + "/base/in/src.txt";
    writeFile(source, "src");
    String zip = ramFolder + "/archive.zip";

    ZipFileMeta meta = updateMeta();
    meta.setKeepSourceFolder(true);

    ZipFile transform = run(meta, source, zip);

    assertEquals(0, transform.getErrors());
    String expected = HopVfs.getFileObject(source).getName().getPath();
    assertEquals(Map.of(expected, "src"), entries(zip));
  }

  private static ZipFileMeta updateMeta() {
    ZipFileMeta meta = new ZipFileMeta();
    meta.setDefault();
    meta.setSourceFilenameField("source");
    meta.setTargetFilenameField("zip");
    meta.setOverwriteZipEntry(true); // update the existing zip instead of replacing it
    return meta;
  }

  private static RowMeta rowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("source"));
    rowMeta.addValueMeta(new ValueMetaString("zip"));
    return rowMeta;
  }

  private ZipFile run(ZipFileMeta meta, String source, String zip) throws Exception {
    return run(meta, rowMeta(), new Object[] {source, zip});
  }

  private ZipFile run(ZipFileMeta meta, String[]... rows) throws Exception {
    return run(meta, rowMeta(), (Object[][]) rows);
  }

  private ZipFile run(ZipFileMeta meta, RowMeta rowMeta, Object[]... rows) throws Exception {
    ZipFile transform =
        new ZipFile(
            helper.transformMeta, meta, new ZipFileData(), 0, helper.pipelineMeta, helper.pipeline);
    BlockingRowSet input = new BlockingRowSet(rows.length);
    for (Object[] row : rows) {
      input.putRow(rowMeta, row);
    }
    input.setDone();
    transform.setInputRowSets(new ArrayList<>(List.of(input)));
    transform.setOutputRowSets(List.of(new BlockingRowSet(rows.length + 1)));
    while (transform.processRow()) {
      // Process all rows. A row that fails stops the transform.
    }
    return transform;
  }

  private static String incompressible() {
    StringBuilder content = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      content.append(UUID.randomUUID());
    }
    return content.toString();
  }

  private static void writeFile(String filename, String content) throws Exception {
    FileObject file = HopVfs.getFileObject(filename);
    file.getParent().createFolder();
    try (OutputStream out = HopVfs.getOutputStream(file, false)) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
  }

  private static void writeZip(String filename, String entry, String content) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ZipOutputStream out = new ZipOutputStream(bytes)) {
      out.putNextEntry(new ZipEntry(entry));
      out.write(content.getBytes(StandardCharsets.UTF_8));
      out.closeEntry();
    }
    FileObject file = HopVfs.getFileObject(filename);
    file.getParent().createFolder();
    try (OutputStream out = HopVfs.getOutputStream(file, false)) {
      out.write(bytes.toByteArray());
    }
  }

  private static Map<String, String> entries(String filename) throws Exception {
    Map<String, String> entries = new TreeMap<>();
    try (ZipInputStream in = new ZipInputStream(HopVfs.getInputStream(filename))) {
      ZipEntry entry;
      while ((entry = in.getNextEntry()) != null) {
        entries.put(entry.getName(), new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
    }
    return entries;
  }

  private static List<String> children(String folder) throws Exception {
    List<String> names = new ArrayList<>();
    for (FileObject child : HopVfs.getFileObject(folder).getChildren()) {
      names.add(child.getName().getBaseName());
    }
    return names;
  }
}
