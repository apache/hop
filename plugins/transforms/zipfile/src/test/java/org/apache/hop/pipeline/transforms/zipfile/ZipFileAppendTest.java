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
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.Const;
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
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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

  @Test
  void updateCopiesAnEntryThatIsTwiceInTheZipOnce() throws Exception {
    String source = ramFolder + "/source/new.txt";
    writeFile(source, "new");
    String zip = ramFolder + "/archive/archive.zip";
    writeZipWithDuplicateEntry(zip, "old.txt");

    ZipFile transform = run(updateMeta(), source, zip);

    assertEquals(0, transform.getErrors());
    assertEquals(Map.of("new.txt", "new", "old.txt", "first"), entries(zip));
  }

  @Test
  void failedMoveAfterTheZipWasRemovedKeepsTheUpdatedZip() throws Exception {
    String source = ramFolder + "/source/new.txt";
    writeFile(source, "new");
    String zip = ramFolder + "/archive/archive.zip";
    writeZip(zip, "old.txt", "old");

    ZipFile transform;
    try (MockedStatic<HopVfs> vfs = Mockito.mockStatic(HopVfs.class, Mockito.CALLS_REAL_METHODS)) {
      vfs.when(() -> HopVfs.moveFile(any(), any()))
          .thenAnswer(
              invocation -> {
                // The move removes the zip file, then fails: as on S3 when the copy fails
                invocation.getArgument(1, FileObject.class).delete();
                throw new FileSystemException("move failed");
              });
      transform = run(updateMeta(), source, zip);
    }

    assertEquals(1, transform.getErrors());
    List<String> left = children(ramFolder + "/archive");
    assertEquals(1, left.size(), "the updated zip must be kept");
    assertTrue(left.get(0).startsWith("archive.zip_") && left.get(0).endsWith(".tmp"));
    assertEquals(
        Map.of("new.txt", "new", "old.txt", "old"), entries(ramFolder + "/archive/" + left.get(0)));
  }

  @Test
  void failedMoveWithTheZipStillThereRemovesTheTemporaryFile() throws Exception {
    String source = ramFolder + "/source/new.txt";
    writeFile(source, "new");
    String zip = ramFolder + "/archive/archive.zip";
    writeZip(zip, "old.txt", "old");

    ZipFile transform;
    try (MockedStatic<HopVfs> vfs = Mockito.mockStatic(HopVfs.class, Mockito.CALLS_REAL_METHODS)) {
      vfs.when(() -> HopVfs.moveFile(any(), any()))
          .thenThrow(new FileSystemException("move failed"));
      transform = run(updateMeta(), source, zip);
    }

    assertEquals(1, transform.getErrors());
    assertEquals(Map.of("old.txt", "old"), entries(zip));
    assertEquals(List.of("archive.zip"), children(ramFolder + "/archive"));
  }

  @Test
  void keepSourceFolderOnLocalFilesKeepsTheNamesItAlwaysHad() throws Exception {
    Path base = Files.createDirectories(tempDir.resolve("base"));
    Path source = Files.createDirectories(base.resolve("in")).resolve("src.txt");
    Files.writeString(source, "src", StandardCharsets.UTF_8);
    String withBase = ramFolder + "/with-base.zip";
    String withoutBase = ramFolder + "/without-base.zip";
    String trailingSeparator = ramFolder + "/trailing-separator.zip";

    ZipFileMeta meta = updateMeta();
    meta.setCreateParentFolder(true);
    meta.setKeepSourceFolder(true);
    meta.setBaseFolderField("base");
    RowMeta rowMeta = rowMeta();
    rowMeta.addValueMeta(new ValueMetaString("base"));
    ZipFile transform =
        run(
            meta,
            rowMeta,
            new Object[] {source.toString(), withBase, base.toString()},
            new Object[] {source.toString(), withoutBase, null},
            new Object[] {source.toString(), trailingSeparator, base + Const.FILE_SEPARATOR});

    assertEquals(0, transform.getErrors());
    // The full local path, minus the base folder and a separator, as before #8955
    String fullPath = HopVfs.getFilename(HopVfs.getFileObject(source.toString()));
    String relative = fullPath.replace(base + Const.FILE_SEPARATOR, "");
    assertEquals(Map.of(relative, "src"), entries(withBase));
    assertEquals(Map.of(fullPath, "src"), entries(withoutBase));
    // A base folder that ends with a separator never matched
    assertEquals(Map.of(fullPath, "src"), entries(trailingSeparator));
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

  /** A zip with the entry twice, "first" and "second": Java can't write one, so patch a name. */
  private static void writeZipWithDuplicateEntry(String filename, String entry) throws Exception {
    String other = entry.substring(0, entry.length() - 1) + "#";
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ZipOutputStream out = new ZipOutputStream(bytes)) {
      out.putNextEntry(new ZipEntry(entry));
      out.write("first".getBytes(StandardCharsets.UTF_8));
      out.putNextEntry(new ZipEntry(other));
      out.write("second".getBytes(StandardCharsets.UTF_8));
      out.closeEntry();
    }
    // The name is in the local header and in the central directory, and in no checksum
    String zip = new String(bytes.toByteArray(), StandardCharsets.ISO_8859_1);
    FileObject file = HopVfs.getFileObject(filename);
    file.getParent().createFolder();
    try (OutputStream out = HopVfs.getOutputStream(file, false)) {
      out.write(zip.replace(other, entry).getBytes(StandardCharsets.ISO_8859_1));
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
