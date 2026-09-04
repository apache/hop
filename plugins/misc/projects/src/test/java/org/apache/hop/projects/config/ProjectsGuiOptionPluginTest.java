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

package org.apache.hop.projects.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.projects.util.ProjectsConfigHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProjectsGuiOptionPluginTest {

  @TempDir Path tempRoot;

  @BeforeAll
  static void beforeAll() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    ProjectsGuiOptionPlugin.clearRequested();
    ProjectsConfigHelper.clearSessionRegisteredProjects();
  }

  @AfterEach
  void tearDown() {
    ProjectsGuiOptionPlugin.clearRequested();
    ProjectsConfigHelper.clearSessionRegisteredProjects();
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    config.removeProjectConfig("gui-proj");
    HopConfig.setInMemoryMode(false);
  }

  @Test
  void handleOptionRemembersProjectForGuiStartup() throws Exception {
    Path projectDir = tempRoot.resolve("gui-proj");
    Files.createDirectories(projectDir);
    Files.writeString(
        projectDir.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME),
        "{\n  \"metadataBaseFolder\" : \"${PROJECT_HOME}/metadata\"\n}\n",
        StandardCharsets.UTF_8);

    ProjectsGuiOptionPlugin plugin = new ProjectsGuiOptionPlugin();
    plugin.setProjectLocations(new String[] {"gui-proj=" + projectDir.toAbsolutePath()});
    plugin.setProjectOption("gui-proj");

    plugin.handleOption(LogChannel.GENERAL, null, new Variables());

    assertEquals("gui-proj", ProjectsGuiOptionPlugin.getRequestedProjectName());
    assertNull(ProjectsGuiOptionPlugin.getRequestedEnvironmentName());
  }

  @Test
  void handleOptionRemembersProjectFromLocationsWithoutMinusJ() throws Exception {
    Path projectDir = tempRoot.resolve("gui-proj");
    Files.createDirectories(projectDir);
    Files.writeString(
        projectDir.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME),
        "{\n  \"metadataBaseFolder\" : \"${PROJECT_HOME}/metadata\"\n}\n",
        StandardCharsets.UTF_8);

    ProjectsGuiOptionPlugin plugin = new ProjectsGuiOptionPlugin();
    plugin.setProjectLocations(new String[] {"gui-proj=" + projectDir.toAbsolutePath()});
    plugin.handleOption(LogChannel.GENERAL, null, new Variables());

    assertEquals("gui-proj", ProjectsGuiOptionPlugin.getRequestedProjectName());
  }

  @Test
  void handleOptionRemembersEnvironmentForGuiStartup() throws Exception {
    Path projectDir = tempRoot.resolve("gui-proj");
    Files.createDirectories(projectDir);
    Files.writeString(
        projectDir.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME),
        "{\n  \"metadataBaseFolder\" : \"${PROJECT_HOME}/metadata\"\n}\n",
        StandardCharsets.UTF_8);

    ProjectsConfigSingleton.getConfig()
        .addProjectConfig(
            new org.apache.hop.projects.project.ProjectConfig(
                "gui-proj",
                projectDir.toAbsolutePath().toString(),
                ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME));
    org.apache.hop.projects.environment.LifecycleEnvironment env =
        new org.apache.hop.projects.environment.LifecycleEnvironment(
            "gui-prod", "test", "gui-proj", java.util.List.of());
    ProjectsConfigSingleton.getConfig().addEnvironment(env);

    try {
      ProjectsGuiOptionPlugin plugin = new ProjectsGuiOptionPlugin();
      plugin.setEnvironmentOption("gui-prod");
      plugin.handleOption(LogChannel.GENERAL, null, new Variables());

      assertEquals("gui-prod", ProjectsGuiOptionPlugin.getRequestedEnvironmentName());
    } finally {
      ProjectsConfigSingleton.getConfig().removeEnvironment("gui-prod");
    }
  }
}
