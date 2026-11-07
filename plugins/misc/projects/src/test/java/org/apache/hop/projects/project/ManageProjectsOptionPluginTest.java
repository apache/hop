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

package org.apache.hop.projects.project;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
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

class ManageProjectsOptionPluginTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void beforeAll() {
    HopLogStore.init();
  }

  @Test
  void createWithAMissingHomeFolderDoesNotRegisterTheProject() {
    String name = "issue-9007-" + System.nanoTime();
    Path missingHome = tempDir.resolve("missing");
    HopConfig.setInMemoryMode(true);
    try {
      ManageProjectsOptionPlugin plugin = new ManageProjectsOptionPlugin();
      new CommandLine(plugin)
          .parseArgs("--project-create", "--project=" + name, "--project-home=" + missingHome);

      HopException exception =
          assertThrows(
              HopException.class,
              () -> plugin.handleOption(LogChannel.GENERAL, null, new Variables()));
      assertTrue(exception.getCause().getMessage().contains("does not exist"));
      assertNull(ProjectsConfigSingleton.getConfig().findProjectConfig(name));
    } finally {
      ProjectsConfigSingleton.getConfig().removeProjectConfig(name);
      HopConfig.setInMemoryMode(false);
    }
  }

  @Test
  void createThatFailsAfterRegisteringRollsTheProjectBack() {
    String name = "issue-9007-parent-" + System.nanoTime();
    HopConfig.setInMemoryMode(true);
    try {
      ManageProjectsOptionPlugin plugin = new ManageProjectsOptionPlugin();
      new CommandLine(plugin)
          .parseArgs(
              "--project-create",
              "--project=" + name,
              "--project-home=" + tempDir,
              "--project-parent=" + name);

      HopException exception =
          assertThrows(
              HopException.class,
              () -> plugin.handleOption(LogChannel.GENERAL, null, new Variables()));
      assertTrue(exception.getCause().getMessage().contains("can not be the same"));
      assertNull(ProjectsConfigSingleton.getConfig().findProjectConfig(name));
      assertFalse(Files.exists(tempDir.resolve("project-config.json")));
    } finally {
      ProjectsConfigSingleton.getConfig().removeProjectConfig(name);
      HopConfig.setInMemoryMode(false);
    }
  }
}
