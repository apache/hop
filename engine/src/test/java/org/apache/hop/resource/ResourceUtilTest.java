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

package org.apache.hop.resource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link ResourceUtil#assignNamedResourceDirectoryVariables}, the resolution of the
 * generated named-resource folder variables (DATA_PATH_n) that are created when a pipeline is
 * exported for remote execution. See issue #7209.
 */
class ResourceUtilTest {

  /**
   * Without a source/target folder mapping, every generated variable must default to the same
   * (resolved) folder as on the local machine. Leaving it unset caused unresolved
   * ${DATA_PATH_n}/file paths on the remote server (#7209).
   */
  @Test
  void defaultsToLocalFolderWhenNoMapping() throws Exception {
    IVariables variables = new Variables();
    Map<String, String> directoryMap = new LinkedHashMap<>();
    directoryMap.put("file:///data/in", "DATA_PATH_1");
    Map<String, String> variablesMap = new java.util.HashMap<>();

    ResourceUtil.assignNamedResourceDirectoryVariables(
        variables, directoryMap, null, null, variablesMap);

    assertEquals("file:///data/in", variablesMap.get("DATA_PATH_1"));
  }

  /** In the default case the referenced folder is resolved, including ${PROJECT_HOME}. */
  @Test
  void defaultResolvesProjectHomeVariable() throws Exception {
    IVariables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "file:///home/user/project");

    Map<String, String> directoryMap = new LinkedHashMap<>();
    directoryMap.put("${PROJECT_HOME}/files", "DATA_PATH_1");
    Map<String, String> variablesMap = new java.util.HashMap<>();

    ResourceUtil.assignNamedResourceDirectoryVariables(
        variables, directoryMap, null, null, variablesMap);

    assertEquals("file:///home/user/project/files", variablesMap.get("DATA_PATH_1"));
  }

  /** Every generated folder variable gets a value, not just the first one. */
  @Test
  void defaultAssignsAllGeneratedVariables() throws Exception {
    IVariables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "file:///home/user/project");

    Map<String, String> directoryMap = new LinkedHashMap<>();
    directoryMap.put("${PROJECT_HOME}/in", "DATA_PATH_1");
    directoryMap.put("file:///absolute/out", "DATA_PATH_2");
    Map<String, String> variablesMap = new java.util.HashMap<>();

    ResourceUtil.assignNamedResourceDirectoryVariables(
        variables, directoryMap, null, null, variablesMap);

    assertEquals("file:///home/user/project/in", variablesMap.get("DATA_PATH_1"));
    assertEquals("file:///absolute/out", variablesMap.get("DATA_PATH_2"));
  }

  /**
   * With a source (${PROJECT_HOME}) and target (/server) folder configured, the referenced folder
   * is mapped relative to the source folder onto the target folder on the server.
   */
  @Test
  void mapsProjectHomeSourceFolderOntoTargetFolder() throws Exception {
    IVariables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "file:///home/user/project");

    Map<String, String> directoryMap = new LinkedHashMap<>();
    directoryMap.put("${PROJECT_HOME}/files", "DATA_PATH_1");
    Map<String, String> variablesMap = new java.util.HashMap<>();

    ResourceUtil.assignNamedResourceDirectoryVariables(
        variables, directoryMap, "${PROJECT_HOME}", "/server/", variablesMap);

    assertEquals("/server/files", variablesMap.get("DATA_PATH_1"));
  }

  /**
   * The generated folder variables are only of use to the remote server if they are part of the
   * execution configuration that travels inside the export archive. The remote pipeline engine used
   * to hand this method a clone of its configuration while the variables were written into the
   * original, so the archive carried none of them and the server created a folder literally called
   * ${DATA_PATH_1} (#8234).
   */
  @Test
  void generatedFolderVariablesEndUpInTheArchivedConfiguration(@TempDir File tempDir)
      throws Exception {
    File dataFolder = new File(tempDir, "data");
    assertTrue(dataFolder.mkdirs());
    File dataFile = new File(dataFolder, "input.txt");
    Files.write(dataFile.toPath(), "content".getBytes(StandardCharsets.UTF_8));

    IVariables variables = new Variables();
    PipelineExecutionConfiguration executionConfiguration = new PipelineExecutionConfiguration();

    // A minimal export that renames one referenced file, exactly as a file transform does.
    IResourceExport resourceExport =
        (vars, definitions, naming, provider) -> {
          try {
            String renamed =
                naming.nameResource(HopVfs.getFileObject(dataFile.getAbsolutePath()), vars, true);
            assertTrue(renamed.startsWith("${DATA_PATH_1}/"), renamed);
          } catch (FileSystemException e) {
            throw new HopException(e);
          }
          definitions.put("main.hpl", new ResourceDefinition("main.hpl", "<pipeline/>"));
          return "main.hpl";
        };

    File zip = new File(tempDir, "export.zip");
    ResourceUtil.serializeResourceExportInterface(
        zip.getAbsolutePath(),
        resourceExport,
        variables,
        new MemoryMetadataProvider(),
        executionConfiguration,
        Pipeline.CONFIGURATION_IN_EXPORT_FILENAME,
        null,
        null);

    String configurationXml = readEntry(zip, Pipeline.CONFIGURATION_IN_EXPORT_FILENAME);
    assertNotNull(configurationXml);
    assertTrue(
        configurationXml.contains("DATA_PATH_1"),
        "the archived execution configuration should carry the generated folder variable: "
            + configurationXml);
  }

  private static String readEntry(File zip, String name) throws IOException {
    try (ZipFile zipFile = new ZipFile(zip)) {
      ZipEntry entry = zipFile.getEntry(name);
      assertNotNull(entry, name + " is missing from the export archive");
      return IOUtils.toString(zipFile.getInputStream(entry), StandardCharsets.UTF_8);
    }
  }
}
