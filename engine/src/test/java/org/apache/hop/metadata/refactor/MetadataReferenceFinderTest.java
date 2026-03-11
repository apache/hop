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

package org.apache.hop.metadata.refactor;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MetadataReferenceFinder}.
 *
 * <p>XML tag scanning depends on the set of transform/action plugins registered at runtime. In the
 * engine module test classpath those plugin sets are minimal, so tests that scan for metadata
 * element names or file-path tags may return empty results. Full integration tests that exercise
 * the complete XML replacement pipeline (including all Hop plugins) should live in an
 * integration-test module or be run against a fully assembled Hop installation.
 *
 * <p>The tests here focus on guard conditions (null/empty inputs) and behaviors that are
 * independent of plugin registration.
 */
class MetadataReferenceFinderTest {

  private static MemoryMetadataProvider provider;
  private MetadataReferenceFinder finder;
  private Path tempDir;

  @BeforeAll
  static void setupEnvironment() throws Exception {
    HopEnvironment.init();
    provider = new MemoryMetadataProvider();
  }

  @BeforeEach
  void setup() throws Exception {
    finder = new MetadataReferenceFinder(provider);
    tempDir = Files.createTempDirectory("hop-refactor-test");
  }

  @AfterEach
  void cleanup() throws Exception {
    deleteDir(tempDir);
  }

  // ---------------------------------------------------------------------------
  // Construction
  // ---------------------------------------------------------------------------

  @Test
  void construction_doesNotThrow() {
    assertDoesNotThrow(() -> new MetadataReferenceFinder(provider));
  }

  // ---------------------------------------------------------------------------
  // findReferences — guard conditions
  // ---------------------------------------------------------------------------

  @Test
  void findReferences_emptyElementNameReturnsEmpty() throws Exception {
    writePipelineXml(tempDir, "test.hpl", "<pipeline/>");
    List<MetadataReferenceResult> results =
        finder.findReferences("rdbms", "", List.of(tempDir.toString()));
    assertTrue(results.isEmpty());
  }

  @Test
  void findReferences_nullSearchRootsReturnsEmpty() throws Exception {
    List<MetadataReferenceResult> results =
        finder.findReferences("rdbms", "MyDb", Collections.emptyList());
    assertTrue(results.isEmpty());
  }

  @Test
  void findReferences_unknownMetadataKeyReturnsEmpty() throws Exception {
    writePipelineXml(tempDir, "test.hpl", "<pipeline/>");
    List<MetadataReferenceResult> results =
        finder.findReferences("non-existent-key", "MyDb", List.of(tempDir.toString()));
    assertTrue(results.isEmpty());
  }

  // ---------------------------------------------------------------------------
  // findFileReferences — guard conditions
  // ---------------------------------------------------------------------------

  @Test
  void findFileReferences_emptyFilePathReturnsEmpty() throws Exception {
    List<MetadataReferenceResult> results =
        finder.findFileReferences(List.of(tempDir.toString()), "");
    assertTrue(results.isEmpty());
  }

  @Test
  void findFileReferences_emptySearchRootsReturnsEmpty() throws Exception {
    List<MetadataReferenceResult> results =
        finder.findFileReferences(Collections.emptyList(), "/some/path.hpl");
    assertTrue(results.isEmpty());
  }

  @Test
  void findFileReferences_withVariables_emptyFilePathReturnsEmpty() throws Exception {
    Variables vars = new Variables();
    vars.setVariable("PROJECT_HOME", "/opt/project");
    List<MetadataReferenceResult> results =
        finder.findFileReferences(List.of(tempDir.toString()), "", vars);
    assertTrue(results.isEmpty());
  }

  @Test
  void findFileReferences_nonExistentRootReturnsEmpty() throws Exception {
    List<MetadataReferenceResult> results =
        finder.findFileReferences(List.of("/does/not/exist"), "/some/path.hpl");
    assertTrue(results.isEmpty());
  }

  // ---------------------------------------------------------------------------
  // replaceFileReferences — guard conditions (no-op for same old/new path)
  // ---------------------------------------------------------------------------

  @Test
  void replaceFileReferences_sameOldAndNewPathIsNoOp() {
    assertDoesNotThrow(
        () ->
            finder.replaceFileReferences(
                List.of(new MetadataReferenceResult(tempDir.resolve("x.hpl").toString(), 1)),
                "/same/path.hpl",
                "/same/path.hpl"));
  }

  @Test
  void replaceFileReferences_emptyResultsIsNoOp() {
    assertDoesNotThrow(
        () ->
            finder.replaceFileReferences(
                Collections.emptyList(), "/old/path.hpl", "/new/path.hpl"));
  }

  // ---------------------------------------------------------------------------
  // replaceReferences — guard conditions
  // ---------------------------------------------------------------------------

  @Test
  void replaceReferences_emptyOldNameIsNoOp() {
    assertDoesNotThrow(
        () ->
            finder.replaceReferences(
                "rdbms", List.of(new MetadataReferenceResult(tempDir.toString(), 1)), "", "New"));
  }

  @Test
  void replaceReferences_sameOldAndNewNameIsNoOp() {
    assertDoesNotThrow(
        () ->
            finder.replaceReferences(
                "rdbms",
                List.of(new MetadataReferenceResult(tempDir.toString(), 1)),
                "Same",
                "Same"));
  }

  // ---------------------------------------------------------------------------
  // findReferencesInMetadata / replaceReferencesInMetadata — guard conditions
  // ---------------------------------------------------------------------------

  @Test
  void findReferencesInMetadata_unknownKeyReturnsEmpty() throws Exception {
    List<MetadataObjectReference> refs =
        finder.findReferencesInMetadata("non-existent-key", "anything");
    assertTrue(refs.isEmpty());
  }

  @Test
  void findReferencesInMetadata_emptyElementNameReturnsEmpty() throws Exception {
    List<MetadataObjectReference> refs =
        finder.findReferencesInMetadata("pipeline-run-configuration", "");
    assertTrue(refs.isEmpty());
  }

  @Test
  void replaceReferencesInMetadata_sameOldAndNewNameIsNoOp() {
    assertDoesNotThrow(
        () ->
            finder.replaceReferencesInMetadata(
                "pipeline-run-configuration",
                "Same",
                "Same",
                List.of(new MetadataObjectReference("pipeline-run-configuration", "Local"))));
  }

  // ---------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------

  private Path writePipelineXml(Path dir, String filename, String content) throws IOException {
    Path file = dir.resolve(filename);
    Files.writeString(
        file, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + content, StandardCharsets.UTF_8);
    return file;
  }

  private void deleteDir(Path dir) throws IOException {
    if (dir == null || !Files.exists(dir)) {
      return;
    }
    try (var stream = Files.walk(dir)) {
      stream
          .sorted(java.util.Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(java.io.File::delete);
    }
  }
}
