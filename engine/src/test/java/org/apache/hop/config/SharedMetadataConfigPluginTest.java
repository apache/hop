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

package org.apache.hop.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.config.ConfigFileSerializer;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.IHopConfigSerializer;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.util.TestUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.util.SharedMetadata;
import org.apache.hop.server.HopServerMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class SharedMetadataConfigPluginTest {

  @TempDir Path tempDir;

  private String originalConfigFilename;
  private IHopConfigSerializer originalSerializer;
  private IVariables variables;

  @BeforeEach
  void redirectConfigFile() throws Exception {
    TestUtil.registerTestPluginTypes();
    Path configFile = tempDir.resolve("hop-config.json");
    Files.writeString(configFile, "{}");
    HopConfig hopConfig = HopConfig.getInstance();
    originalConfigFilename = hopConfig.getConfigFilename();
    originalSerializer = hopConfig.getSerializer();
    hopConfig.setConfigFilename(configFile.toString());
    hopConfig.setSerializer(new ConfigFileSerializer());
    hopConfig.getConfigMap().remove(SharedMetadata.CONFIG_KEY);

    variables = new Variables();
    variables.setVariable("HOP_CONFIG_FOLDER", tempDir.toString());
  }

  @AfterEach
  void restoreConfigFile() {
    HopConfig hopConfig = HopConfig.getInstance();
    hopConfig.getConfigMap().remove(SharedMetadata.CONFIG_KEY);
    hopConfig.setConfigFilename(originalConfigFilename);
    hopConfig.setSerializer(originalSerializer);
  }

  @Test
  void setAndClearTheFolder() throws Exception {
    assertTrue(handle("--shared-metadata-folder=${HOP_CONFIG_FOLDER}/metadata"));
    assertEquals(
        "${HOP_CONFIG_FOLDER}/metadata", HopConfig.readOptionString(SharedMetadata.CONFIG_KEY, ""));

    assertTrue(handle("--shared-metadata-folder="));
    Object cleared = HopConfig.getInstance().getConfigMap().get(SharedMetadata.CONFIG_KEY);
    assertTrue(cleared == null || cleared.toString().isEmpty());
  }

  @Test
  void importWritesObjectsAndASecondImportReplacesTheSameName() throws Exception {
    Path export = tempDir.resolve("export.json");
    Files.writeString(export, exportJson("host-a"));

    assertTrue(handle("--shared-metadata-import=" + export));
    Path object = tempDir.resolve("metadata").resolve("server").resolve("server1.json");
    assertTrue(Files.exists(object));
    assertTrue(Files.readString(object).contains("host-a"));
    assertEquals(
        "${HOP_CONFIG_FOLDER}/metadata", HopConfig.readOptionString(SharedMetadata.CONFIG_KEY, ""));

    Path keep = tempDir.resolve("metadata").resolve("server").resolve("keep.json");
    Files.writeString(keep, "{ \"name\" : \"keep\" }");

    Files.writeString(export, exportJson("host-b"));
    assertTrue(handle("--shared-metadata-import=" + export));
    String replaced = Files.readString(object);
    assertTrue(replaced.contains("host-b"));
    assertFalse(replaced.contains("host-a"));
    assertTrue(Files.exists(keep));
  }

  @Test
  void importIfEmptySkipsAFolderThatAlreadyHasMetadata() throws Exception {
    Path export = tempDir.resolve("export.json");
    Files.writeString(export, exportJson("host-a"));
    assertTrue(handle("--shared-metadata-import-if-empty=" + export));

    Files.writeString(export, exportJson("host-b"));
    assertTrue(handle("--shared-metadata-import-if-empty=" + export));
    String stored =
        Files.readString(tempDir.resolve("metadata").resolve("server").resolve("server1.json"));
    assertTrue(stored.contains("host-a"));
    assertFalse(stored.contains("host-b"));
  }

  private boolean handle(String... args) throws HopException {
    SharedMetadataConfigPlugin plugin = new SharedMetadataConfigPlugin();
    new CommandLine(plugin).parseArgs(args);
    return plugin.handleOption(LogChannel.GENERAL, null, variables);
  }

  private static String exportJson(String hostname) throws HopException {
    MemoryMetadataProvider source = new MemoryMetadataProvider();
    source
        .getSerializer(HopServerMeta.class)
        .save(
            new HopServerMeta(
                "server1", hostname, "8080", "user", "password", null, null, null, false));
    return new SerializableMetadataProvider(source).toJson();
  }
}
