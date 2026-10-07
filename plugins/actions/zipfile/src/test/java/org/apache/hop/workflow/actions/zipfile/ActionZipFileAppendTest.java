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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Appending to an existing zip file must not go through the shared system temp folder. */
class ActionZipFileAppendTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
    HopLogStore.init();
  }

  @Test
  void appendKeepsExistingEntriesAndUsesTheZipFolderForTheTemporaryFile() throws Exception {
    Path sourceDir = Files.createDirectories(tempDir.resolve("source"));
    Files.writeString(sourceDir.resolve("new.txt"), "new", StandardCharsets.UTF_8);
    Path zipDir = Files.createDirectories(tempDir.resolve("archive"));
    Path zip = zipDir.resolve("archive.zip");
    writeZip(zip, "old.txt", "old");

    ActionZipFile action = new ActionZipFile();
    action.setParentWorkflow(new LocalWorkflowEngine(new WorkflowMeta()));
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    action.setLogLevel(LogLevel.ERROR);
    action.setSourceDirectory(sourceDir.toString());
    action.setWildCard(".*\\.txt");
    action.setZipFilename(zip.toString());
    action.setIfZipFileExists(1); // append
    action.setAfterZip(0); // leave the source files alone

    List<File> tempFolders = new ArrayList<>();
    Result result;
    try (MockedStatic<File> files = Mockito.mockStatic(File.class, Mockito.CALLS_REAL_METHODS)) {
      files
          .when(() -> File.createTempFile(anyString(), any(), any()))
          .thenAnswer(
              invocation -> {
                tempFolders.add(invocation.getArgument(2));
                return invocation.callRealMethod();
              });
      result = action.execute(new Result(), 0);
    }

    assertTrue(result.isResult(), "zipping should succeed");
    assertEquals(0, result.getNrErrors());
    assertEquals(Set.of("old.txt", "new.txt"), entries(zip));
    assertEquals(List.of(zipDir.toFile()), tempFolders, "temporary file folder");
    assertEquals(
        List.of(zip), Files.list(zipDir).toList(), "no temporary file should be left behind");
  }

  static void writeZip(Path zip, String entry, String content) throws Exception {
    try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(zip))) {
      out.putNextEntry(new ZipEntry(entry));
      out.write(content.getBytes(StandardCharsets.UTF_8));
      out.closeEntry();
    }
  }

  static Set<String> entries(Path zip) throws Exception {
    Set<String> names = new TreeSet<>();
    try (ZipFile zipFile = new ZipFile(zip.toFile())) {
      zipFile.stream().forEach(entry -> names.add(entry.getName()));
    }
    return names;
  }
}
