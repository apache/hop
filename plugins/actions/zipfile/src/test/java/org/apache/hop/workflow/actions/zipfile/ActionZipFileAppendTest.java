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

package org.apache.hop.workflow.actions.zipfile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

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
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

/** Appending to an existing zip file, locally and on a VFS file system. */
class ActionZipFileAppendTest {

  @TempDir Path tempDir;

  private String ramFolder;

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    ramFolder = "ram:///" + UUID.randomUUID();
  }

  @AfterEach
  void tearDown() throws Exception {
    HopVfs.getFileObject(ramFolder).deleteAll();
  }

  @Test
  void appendToLocalZipKeepsExistingEntries() throws Exception {
    Path sourceDir = Files.createDirectories(tempDir.resolve("source"));
    Files.writeString(sourceDir.resolve("new.txt"), "new", StandardCharsets.UTF_8);
    Path zipDir = Files.createDirectories(tempDir.resolve("archive"));
    Path zip = zipDir.resolve("archive.zip");
    writeZip(zip.toUri().toString(), "old.txt", "old");

    Result result = append(sourceDir.toString(), zip.toString());

    assertTrue(result.isResult(), "zipping should succeed");
    assertEquals(0, result.getNrErrors());
    assertEquals(Map.of("new.txt", "new", "old.txt", "old"), entries(zip.toUri().toString()));
    assertEquals(
        List.of(zip), Files.list(zipDir).toList(), "no temporary file should be left behind");
  }

  @Test
  void appendToVfsZipKeepsExistingEntries() throws Exception {
    writeFile(ramFolder + "/source/new.txt", "new");
    String zip = ramFolder + "/archive/archive.zip";
    writeZip(zip, "old.txt", "old");

    Result result = append(ramFolder + "/source", zip);

    assertTrue(result.isResult(), "zipping should succeed");
    assertEquals(Map.of("new.txt", "new", "old.txt", "old"), entries(zip));
    assertEquals(List.of("archive.zip"), children(ramFolder + "/archive"));
  }

  @Test
  void appendReplacesAnEntryWithTheSameName() throws Exception {
    writeFile(ramFolder + "/source/data.txt", "new");
    String zip = ramFolder + "/archive/archive.zip";
    writeZip(zip, "data.txt", "old");

    Result result = append(ramFolder + "/source", zip);

    assertTrue(result.isResult(), "zipping should succeed");
    assertEquals(Map.of("data.txt", "new"), entries(zip));
  }

  @Test
  void failedAppendLeavesTheOriginalZipIntact() throws Exception {
    writeFile(ramFolder + "/source/new.txt", "new");
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

    Result result = append(ramFolder + "/source", zip);

    assertFalse(result.isResult(), "zipping should fail");
    try (InputStream in = HopVfs.getInputStream(zip)) {
      assertArrayEquals(truncated, in.readAllBytes(), "the original zip must not change");
    }
    assertEquals(List.of("archive.zip"), children(ramFolder + "/archive"));
  }

  @Test
  void appendCopiesAnEntryThatIsTwiceInTheZipOnce() throws Exception {
    writeFile(ramFolder + "/source/new.txt", "new");
    String zip = ramFolder + "/archive/archive.zip";
    writeZipWithDuplicateEntry(zip, "old.txt");

    Result result = append(ramFolder + "/source", zip);

    assertTrue(result.isResult(), "zipping should succeed");
    assertEquals(Map.of("new.txt", "new", "old.txt", "first"), entries(zip));
  }

  @Test
  void failedMoveAfterTheZipWasRemovedKeepsTheUpdatedZip() throws Exception {
    writeFile(ramFolder + "/source/new.txt", "new");
    String zip = ramFolder + "/archive/archive.zip";
    writeZip(zip, "old.txt", "old");

    Result result;
    try (MockedStatic<HopVfs> vfs = Mockito.mockStatic(HopVfs.class, Mockito.CALLS_REAL_METHODS)) {
      vfs.when(() -> HopVfs.moveFile(any(), any()))
          .thenAnswer(
              invocation -> {
                // The move removes the zip file, then fails: as on S3 when the copy fails
                invocation.getArgument(1, FileObject.class).delete();
                throw new FileSystemException("move failed");
              });
      result = append(ramFolder + "/source", zip);
    }

    assertFalse(result.isResult(), "zipping should fail");
    List<String> left = children(ramFolder + "/archive");
    assertEquals(1, left.size(), "the updated zip must be kept");
    assertTrue(left.get(0).startsWith("archive.zip_") && left.get(0).endsWith(".tmp"));
    assertEquals(
        Map.of("new.txt", "new", "old.txt", "old"), entries(ramFolder + "/archive/" + left.get(0)));
  }

  @Test
  void failedMoveThatLeavesAPartialZipKeepsTheUpdatedZip() throws Exception {
    writeFile(ramFolder + "/source/new.txt", "new");
    String zip = ramFolder + "/archive/archive.zip";
    writeZip(zip, "old.txt", "old");

    Result result;
    try (MockedStatic<HopVfs> vfs = Mockito.mockStatic(HopVfs.class, Mockito.CALLS_REAL_METHODS)) {
      vfs.when(() -> HopVfs.moveFile(any(), any()))
          .thenAnswer(ActionZipFileAppendTest::failPartWayThroughTheCopy);
      result = append(ramFolder + "/source", zip);
    }

    assertFalse(result.isResult(), "zipping should fail");
    List<String> left = children(ramFolder + "/archive");
    left.remove("archive.zip");
    assertEquals(1, left.size(), "the updated zip must be kept");
    assertTrue(left.get(0).startsWith("archive.zip_") && left.get(0).endsWith(".tmp"));
    assertEquals(
        Map.of("new.txt", "new", "old.txt", "old"), entries(ramFolder + "/archive/" + left.get(0)));
  }

  @Test
  void failedMoveWithTheZipStillThereRemovesTheTemporaryFile() throws Exception {
    writeFile(ramFolder + "/source/new.txt", "new");
    String zip = ramFolder + "/archive/archive.zip";
    writeZip(zip, "old.txt", "old");

    Result result;
    try (MockedStatic<HopVfs> vfs = Mockito.mockStatic(HopVfs.class, Mockito.CALLS_REAL_METHODS)) {
      vfs.when(() -> HopVfs.moveFile(any(), any()))
          .thenThrow(new FileSystemException("move failed"));
      result = append(ramFolder + "/source", zip);
    }

    assertFalse(result.isResult(), "zipping should fail");
    assertEquals(Map.of("old.txt", "old"), entries(zip));
    assertEquals(List.of("archive.zip"), children(ramFolder + "/archive"));
  }

  private static Result append(String sourceDirectory, String zipFilename) throws Exception {
    ActionZipFile action = new ActionZipFile();
    action.setParentWorkflow(new LocalWorkflowEngine(new WorkflowMeta()));
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    action.setLogLevel(LogLevel.NOTHING);
    action.setSourceDirectory(sourceDirectory);
    action.setWildCard(".*\\.txt");
    action.setZipFilename(zipFilename);
    action.setIfZipFileExists(1); // append
    action.setAfterZip(0); // leave the source files alone
    return action.execute(new Result(), 0);
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

  /** A move that fails part way through copying over the zip file, as the copy fallback can. */
  private static Object failPartWayThroughTheCopy(InvocationOnMock invocation) throws Exception {
    FileObject source = invocation.getArgument(0, FileObject.class);
    FileObject destination = invocation.getArgument(1, FileObject.class);
    byte[] start;
    try (InputStream in = source.getContent().getInputStream()) {
      start = in.readNBytes(10);
    }
    try (OutputStream out = destination.getContent().getOutputStream()) {
      out.write(start);
    }
    throw new FileSystemException("copy failed");
  }
}
