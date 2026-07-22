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

package org.apache.hop.projects.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProjectsMetadataExporterTest {

  @TempDir Path tempDir;

  @Test
  void resolveExportFilenameRelativeToProjectHome() throws Exception {
    Variables variables = new Variables();
    String home = tempDir.toAbsolutePath().toString();
    String resolved =
        ProjectsMetadataExporter.resolveExportFilename(home, "metadata.json", variables);
    assertTrue(resolved.contains("metadata.json"), "resolved path: " + resolved);
    assertTrue(
        resolved.contains(tempDir.getFileName().toString()) || resolved.startsWith("file:"),
        "resolved path should include home: " + resolved);
  }

  @Test
  void resolveExportFilenameAbsoluteUnchanged() throws Exception {
    Variables variables = new Variables();
    Path absolute = tempDir.resolve("out").resolve("export.json");
    String resolved =
        ProjectsMetadataExporter.resolveExportFilename(
            tempDir.toString(), absolute.toString(), variables);
    assertTrue(resolved.contains("export.json"), "resolved path: " + resolved);
  }

  @Test
  void resolveExportFilenameUsesDefaultWhenEmpty() throws Exception {
    Variables variables = new Variables();
    String resolved =
        ProjectsMetadataExporter.resolveExportFilename(tempDir.toString(), "", variables);
    assertTrue(
        resolved.endsWith(Defaults.DEFAULT_AUTO_EXPORT_METADATA_FILENAME)
            || resolved.contains(Defaults.DEFAULT_AUTO_EXPORT_METADATA_FILENAME),
        "resolved path: " + resolved);
  }

  @Test
  void exportToFileWritesJson() throws Exception {
    Path target = tempDir.resolve("metadata.json");
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    ProjectsMetadataExporter.exportToFile(provider, target.toString());

    assertTrue(Files.exists(target));
    String content = Files.readString(target, StandardCharsets.UTF_8);
    // Empty provider still produces a JSON object
    assertTrue(content.trim().startsWith("{"));
  }
}
