/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class VfsFileScannerTest {
  @TempDir Path temporary;

  @BeforeAll
  static void initializeHop() throws Exception {
    HopEnvironment.init();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "space name.csv",
        "file#1.csv",
        "file%20.csv",
        "file%2520.csv",
        "áéíóú-测试.csv",
        "report[1].csv",
        "report{1}+.csv"
      })
  void portableNamesPreservePathsAndFileState(String filename) throws Exception {
    assertName(filename);
  }

  @EnabledOnOs(OS.LINUX)
  @ParameterizedTest
  @ValueSource(
      strings = {
        "report?.csv",
        "a<b.csv",
        "a>b.csv",
        "a\"b.csv",
        "a\\b.csv",
        "a\nb.csv",
        "a\tb.csv",
        "a\u0001b.csv",
        "a|b^`c.csv"
      })
  void unixNamesPreservePathsAndFileState(String filename) throws Exception {
    assertName(filename);
  }

  private void assertName(String filename) throws Exception {
    Path input = Files.createDirectory(temporary.resolve("input"));
    Path file = Files.writeString(input.resolve(filename), "data");
    try (FileObject root = VfsFileScanner.resolveFile(input.toString(), new Variables())) {
      VfsFileScanner scanner = new VfsFileScanner(root, true, "", "", 100, () -> false);
      var snapshot = scanner.snapshot();
      assertEquals(1, snapshot.size());
      FileState state = snapshot.values().iterator().next();
      assertEquals(filename, state.getShortFilename());
      assertEquals(Files.size(file), state.getSize());
      assertEquals(Files.getLastModifiedTime(file).toMillis(), state.getLastModified());
      assertEquals("file", state.getScheme());
      String relative = scanner.relativeUri(state.getUri());
      assertEquals(state, scanner.readRelative(relative));
      assertEquals(state.getUri(), scanner.uri(relative));
      for (String location :
          new String[] {file.toString(), file.toUri().toASCIIString(), state.getUri()}) {
        try (FileObject resolved = VfsFileScanner.resolveFile(location, new Variables())) {
          assertEquals(file.toAbsolutePath().normalize(), VfsFileScanner.localPath(resolved));
          assertTrue(resolved.exists());
          assertEquals(4, resolved.getContent().getSize());
        }
      }
    }
  }

  @Test
  void unusualRootAndNestedDirectoryPreserveRecursiveIdentity() throws Exception {
    assertNested("root á%20 #[]{}+", "nested %2520 [1]");
  }

  @Test
  @EnabledOnOs(OS.LINUX)
  void unixRootAndNestedDirectoryPreserveRecursiveIdentity() throws Exception {
    assertNested("root?\"<á%20>\\\n", "nested\\?\u0001");
  }

  private void assertNested(String rootName, String nestedName) throws Exception {
    Path input = Files.createDirectory(temporary.resolve(rootName));
    Path file =
        Files.writeString(
            Files.createDirectory(input.resolve(nestedName)).resolve("data.csv"), "data");
    for (String location : new String[] {input.toString(), input.toUri().toASCIIString()}) {
      try (FileObject root = VfsFileScanner.resolveFile(location, new Variables())) {
        assertEquals(input, VfsFileScanner.localPath(root));
        VfsFileScanner scanner = new VfsFileScanner(root, true, "", "", 100, () -> false);
        var snapshot = scanner.snapshot();
        assertEquals(1, snapshot.size());
        FileState state = snapshot.values().iterator().next();
        assertEquals(state, scanner.readRelative(scanner.relativeUri(state.getUri())));
        try (FileObject resolved = VfsFileScanner.resolveFile(state.getUri(), new Variables())) {
          assertEquals(file, VfsFileScanner.localPath(resolved));
        }
      }
    }
  }

  @Test
  @EnabledOnOs(OS.WINDOWS)
  void windowsDriveAndUncRootsArePreservedWithoutNetworkAccess() throws Exception {
    for (String location :
        new String[] {
          "C:\\folder\\file%20.csv", "C:/folder/file%20.csv", "file:///C:/folder/file%2520.csv"
        }) {
      try (FileObject file = VfsFileScanner.resolveFile(location, new Variables())) {
        assertEquals(Path.of("C:\\folder\\file%20.csv"), VfsFileScanner.localPath(file));
      }
    }
    for (String location :
        new String[] {
          "\\\\server\\share\\folder\\file%20.csv",
          "file:////server/share/folder/file%2520.csv",
          "file://server/share/folder/file%2520.csv"
        }) {
      try (FileObject file = VfsFileScanner.resolveFile(location, new Variables())) {
        assertEquals(
            Path.of("\\\\server\\share\\folder\\file%20.csv"), VfsFileScanner.localPath(file));
      }
    }
  }

  @Test
  void variablesAreResolvedBeforeLocalPathConversion() throws Exception {
    Variables variables = new Variables();
    variables.setVariable("INPUT", temporary.resolve("path %20 # á").toString());
    try (FileObject file = VfsFileScanner.resolveFile("${INPUT}", variables)) {
      assertEquals(temporary.resolve("path %20 # á"), VfsFileScanner.localPath(file));
    }
  }

  @Test
  void localIterationIoFailureRemainsRetryableAndPreservesCloseFailure() throws Exception {
    Path input = Files.createDirectory(temporary.resolve("input"));
    IOException original = new IOException("Listing interrupted");
    IOException closeFailure = new IOException("Stream close failed");
    @SuppressWarnings("unchecked")
    DirectoryStream<Path> entries = mock(DirectoryStream.class);
    @SuppressWarnings("unchecked")
    Iterator<Path> iterator = mock(Iterator.class);
    when(entries.iterator()).thenReturn(iterator);
    when(iterator.hasNext()).thenThrow(new DirectoryIteratorException(original));
    doThrow(closeFailure).when(entries).close();
    try (FileObject root = VfsFileScanner.resolveFile(input.toString(), new Variables());
        var files = mockStatic(Files.class)) {
      files.when(() -> Files.newDirectoryStream(input)).thenReturn(entries);
      IOException failure =
          assertThrows(
              IOException.class,
              () -> new VfsFileScanner(root, true, "", "", 100, () -> false).snapshot());
      assertSame(original, failure);
      assertEquals(1, failure.getSuppressed().length);
      assertSame(closeFailure, failure.getSuppressed()[0]);
      verify(entries).close();
    }
  }
}
