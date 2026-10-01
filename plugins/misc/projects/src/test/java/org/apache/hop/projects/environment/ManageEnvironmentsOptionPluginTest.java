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

package org.apache.hop.projects.environment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

class ManageEnvironmentsOptionPluginTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void beforeAll() {
    HopLogStore.init();
  }

  @Test
  void directoryOptionsAreParsed() {
    ManageEnvironmentsOptionPlugin plugin = new ManageEnvironmentsOptionPlugin();
    new CommandLine(plugin)
        .parseArgs(
            "--environment-config-file-directory=/data/config",
            "--environment-config-file-wildcard=.*\\.json",
            "--environment-config-file-exclude-wildcard=.*secret.*",
            "--environment-config-include-subfolders");

    assertEquals("/data/config", plugin.getEnvironmentConfigFileDirectory());
    assertEquals(".*\\.json", plugin.getEnvironmentConfigFileWildcard());
    assertEquals(".*secret.*", plugin.getEnvironmentConfigFileExcludeWildcard());
    assertTrue(plugin.isEnvironmentConfigIncludeSubfolders());
  }

  @Test
  void createEnvironmentStoresFilesFromADirectory() throws Exception {
    Files.writeString(tempDir.resolve("a.json"), "{}");
    Files.writeString(tempDir.resolve("notes.json"), "{}");
    Files.writeString(tempDir.resolve("readme.txt"), "text");
    Files.createDirectories(tempDir.resolve("nested"));
    Files.writeString(tempDir.resolve("nested").resolve("nested.json"), "{}");

    String name = "issue-4881-" + System.nanoTime();
    HopConfig.setInMemoryMode(true);
    try {
      ManageEnvironmentsOptionPlugin plugin = new ManageEnvironmentsOptionPlugin();
      new CommandLine(plugin)
          .parseArgs(
              "--environment-create",
              "--environment=" + name,
              "--environment-project=missing-project",
              "--environment-purpose=Testing",
              "--environment-config-files=" + tempDir.resolve("notes.json"),
              "--environment-config-file-directory=" + tempDir,
              "--environment-config-file-wildcard=.*\\.json");

      assertTrue(plugin.handleOption(LogChannel.GENERAL, null, new Variables()));

      LifecycleEnvironment environment = ProjectsConfigSingleton.getConfig().findEnvironment(name);
      List<String> files = environment.getConfigurationFiles();
      assertEquals(2, files.size());
      assertTrue(files.get(0).endsWith("notes.json"));
      assertTrue(files.get(1).endsWith("a.json"));
    } finally {
      ProjectsConfigSingleton.getConfig().removeEnvironment(name);
      HopConfig.setInMemoryMode(false);
    }
  }

  @Test
  void shellGlobDoesNotCreateTheEnvironment() throws Exception {
    Files.writeString(tempDir.resolve("a.json"), "{}");
    String name = "issue-4881-glob-" + System.nanoTime();
    HopConfig.setInMemoryMode(true);
    try {
      ManageEnvironmentsOptionPlugin plugin = new ManageEnvironmentsOptionPlugin();
      new CommandLine(plugin)
          .parseArgs(
              "--environment-create",
              "--environment=" + name,
              "--environment-project=missing-project",
              "--environment-config-file-directory=" + tempDir,
              "--environment-config-file-wildcard=*.json");

      HopException exception =
          assertThrows(
              HopException.class,
              () -> plugin.handleOption(LogChannel.GENERAL, null, new Variables()));
      assertTrue(exception.getCause().getMessage().contains("regular expression"));
      assertNull(ProjectsConfigSingleton.getConfig().findEnvironment(name));
    } finally {
      ProjectsConfigSingleton.getConfig().removeEnvironment(name);
      HopConfig.setInMemoryMode(false);
    }
  }
}
