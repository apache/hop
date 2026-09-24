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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.util.ProjectsUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Parent project, default project and standard parent project references (#4275). */
class ProjectReferencesTest {

  @TempDir Path tempRoot;

  private final List<String> registeredProjectNames = new ArrayList<>();
  private String originalDefaultProject;
  private String originalStandardParentProject;

  @BeforeAll
  static void beforeAll() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    originalDefaultProject = config.getDefaultProject();
    originalStandardParentProject = config.getStandardParentProject();
  }

  @AfterEach
  void tearDown() {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    registeredProjectNames.forEach(config::removeProjectConfig);
    config.setDefaultProject(originalDefaultProject);
    config.setStandardParentProject(originalStandardParentProject);
  }

  @Test
  void removingProjectClearsDefaultAndStandardParentProject() throws Exception {
    registerProject("ref-parent", null);
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    config.setDefaultProject("ref-parent");
    config.setStandardParentProject("ref-parent");
    assertEquals("ref-parent", config.findRegisteredStandardParentProject());

    config.removeProjectConfig("ref-parent");

    assertNull(config.getDefaultProject());
    assertNull(config.getStandardParentProject());
    assertNull(config.findRegisteredStandardParentProject());
  }

  @Test
  void standardParentProjectMustBeRegistered() {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    config.setStandardParentProject("ref-does-not-exist");

    assertNull(config.findRegisteredStandardParentProject());
  }

  @Test
  void renamingRegistrationInPlaceRenamesDefaultAndStandardParentProject() throws Exception {
    ProjectConfig projectConfig = registerProject("ref-old", null);
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    config.setDefaultProject("ref-old");
    config.setStandardParentProject("ref-old");

    // The projects browser dialog renames the registered instance before updating it
    projectConfig.setProjectName("ref-new");
    registeredProjectNames.add("ref-new");
    config.updateProjectConfig("ref-old", projectConfig);

    assertEquals("ref-new", config.getDefaultProject());
    assertEquals("ref-new", config.getStandardParentProject());
  }

  @Test
  void parentProjectReferencesSkipProjectsWhichCantBeLoaded() throws Exception {
    registerProject("ref-parent", null);
    registerProject("ref-child", "ref-parent");
    registerProject("ref-broken", null);
    Files.writeString(
        tempRoot.resolve("ref-broken").resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME),
        "{ not json",
        StandardCharsets.UTF_8);

    List<String> references =
        ProjectsUtil.getParentProjectReferences("ref-parent", new Variables(), LogChannel.GENERAL);

    assertEquals(List.of("ref-child"), references);
  }

  @Test
  void renamingParentProjectSavesChildProjects() throws Exception {
    registerProject("ref-parent", null);
    ProjectConfig childConfig = registerProject("ref-child", "ref-parent");

    List<String> changed =
        ProjectsUtil.changeParentProjectReferences(
            "ref-parent", "ref-renamed", new Variables(), LogChannel.GENERAL);

    assertEquals(List.of("ref-child"), changed);
    Project child = childConfig.loadProject(new Variables());
    assertEquals("ref-renamed", child.getParentProjectName());
  }

  @Test
  void renamingParentProjectLeavesReadOnlyChildProjectsAlone() throws Exception {
    registerProject("ref-parent", null);
    ProjectConfig childConfig = registerProject("ref-child", "ref-parent");
    childConfig.setReadOnly(true);

    List<String> changed =
        ProjectsUtil.changeParentProjectReferences(
            "ref-parent", "ref-renamed", new Variables(), LogChannel.GENERAL);

    assertEquals(List.of(), changed);
    Project child = childConfig.loadProject(new Variables());
    assertEquals("ref-parent", child.getParentProjectName());
  }

  @Test
  void commandLineDeleteRefusesParentProject() throws Exception {
    registerProject("ref-parent", null);
    registerProject("ref-child", "ref-parent");

    ManageProjectsOptionPlugin plugin = new ManageProjectsOptionPlugin();
    plugin.setProjectName("ref-parent");
    plugin.setDeleteProject(true);

    HopException exception =
        assertThrows(
            HopException.class,
            () -> plugin.handleOption(LogChannel.GENERAL, null, new Variables()));

    assertNotNull(ProjectsConfigSingleton.getConfig().findProjectConfig("ref-parent"));
    assertTrue(
        exception
            .getCause()
            .getMessage()
            .contains("'ref-parent' can't be deleted, it is the parent project of: ref-child"));
  }

  private ProjectConfig registerProject(String name, String parentProjectName) throws Exception {
    Path home = tempRoot.resolve(name);
    Files.createDirectories(home);
    String parentJson = parentProjectName == null ? "null" : "\"" + parentProjectName + "\"";
    Files.writeString(
        home.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME),
        "{ \"parentProjectName\" : " + parentJson + " }",
        StandardCharsets.UTF_8);

    ProjectConfig projectConfig =
        new ProjectConfig(name, home.toString(), ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
    ProjectsConfigSingleton.getConfig().addProjectConfig(projectConfig);
    registeredProjectNames.add(name);
    return projectConfig;
  }
}
