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
