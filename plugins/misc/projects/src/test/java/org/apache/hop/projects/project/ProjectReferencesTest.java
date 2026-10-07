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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
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
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.util.ProjectRenameBlockedException;
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
  private final List<String> registeredEnvironmentNames = new ArrayList<>();
  private int saveCount;
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
    registeredEnvironmentNames.forEach(config::removeEnvironment);
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
  void renamingProjectUpdatesChildProjectsEnvironmentsAndDefaults() throws Exception {
    ProjectConfig parentConfig = registerProject("ref-parent", null);
    ProjectConfig childConfig = registerProject("ref-child", "ref-parent");
    registerProject("ref-other", null);
    LifecycleEnvironment environment = registerEnvironment("ref-env", "ref-parent");
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    config.setDefaultProject("ref-parent");

    List<String> changed = rename(parentConfig, "ref-parent", "ref-renamed");

    assertEquals(List.of("ref-child"), changed);
    assertEquals(1, saveCount);
    assertEquals("ref-renamed", childConfig.loadProject(new Variables()).getParentProjectName());
    assertEquals("ref-renamed", environment.getProjectName());
    assertEquals("ref-renamed", config.getDefaultProject());
    assertNotNull(config.findProjectConfig("ref-renamed"));
    assertNull(config.findProjectConfig("ref-parent"));
  }

  @Test
  void caseOnlyRenameUpdatesReferences() throws Exception {
    ProjectConfig parentConfig = registerProject("ref-parent", null);
    ProjectConfig childConfig = registerProject("ref-child", "ref-parent");
    LifecycleEnvironment environment = registerEnvironment("ref-env", "ref-parent");
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    config.setStandardParentProject("ref-parent");

    List<String> changed = rename(parentConfig, "ref-parent", "REF-Parent");

    assertEquals(List.of("ref-child"), changed);
    assertEquals("REF-Parent", childConfig.loadProject(new Variables()).getParentProjectName());
    assertEquals("REF-Parent", environment.getProjectName());
    assertEquals("REF-Parent", config.getStandardParentProject());
    assertEquals("REF-Parent", config.findProjectConfig("ref-parent").getProjectName());
  }

  @Test
  void environmentsOfProjectAreFoundRegardlessOfCase() {
    registerEnvironment("ref-env", "Ref-Parent");

    List<LifecycleEnvironment> environments =
        ProjectsConfigSingleton.getConfig().findEnvironmentsOfProject("ref-parent");

    assertEquals(1, environments.size());
    assertEquals("ref-env", environments.get(0).getName());
  }

  @Test
  void removingProjectLeavesEnvironmentsAlone() throws Exception {
    registerProject("ref-parent", null);
    LifecycleEnvironment environment = registerEnvironment("ref-env", "ref-parent");

    ProjectsConfigSingleton.getConfig().removeProjectConfig("ref-parent");

    // A null project would make the environment show up for every project
    assertEquals("ref-parent", environment.getProjectName());
  }

  @Test
  void readOnlyChildProjectBlocksRenameAndNothingChanges() throws Exception {
    ProjectConfig parentConfig = registerProject("ref-parent", null);
    ProjectConfig writableChild = registerProject("ref-child-a", "ref-parent");
    registerProject("ref-child-b", "ref-parent").setReadOnly(true);
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    config.setDefaultProject("ref-parent");

    ProjectRenameBlockedException exception =
        assertThrows(
            ProjectRenameBlockedException.class,
            () -> rename(parentConfig, "ref-parent", "ref-renamed"));

    assertEquals(List.of("ref-child-b"), exception.getBlockingProjects());
    assertTrue(
        exception
            .getUserMessage()
            .contains("Project 'ref-parent' can't be renamed to 'ref-renamed'"),
        exception.getUserMessage());
    assertTrue(
        exception
            .getUserMessage()
            .contains("'ref-child-b' uses 'ref-parent' as its parent project, but it is read-only"),
        exception.getUserMessage());
    assertEquals(0, saveCount);
    assertEquals("ref-parent", parentConfig.getProjectName());
    assertEquals("ref-parent", config.getDefaultProject());
    assertEquals("ref-parent", writableChild.loadProject(new Variables()).getParentProjectName());
  }

  @Test
  void unreadableProjectBlocksRename() throws Exception {
    ProjectConfig parentConfig = registerProject("ref-parent", null);
    registerProject("ref-broken", null);
    Files.writeString(
        tempRoot.resolve("ref-broken").resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME),
        "{ not json",
        StandardCharsets.UTF_8);

    ProjectRenameBlockedException exception =
        assertThrows(
            ProjectRenameBlockedException.class,
            () ->
                ProjectsUtil.checkProjectRename(
                    "ref-parent", "ref-renamed", new Variables(), LogChannel.GENERAL));

    assertEquals(List.of("ref-broken"), exception.getBlockingProjects());
    assertTrue(
        exception.getUserMessage().contains("'ref-broken': its configuration can't be read"),
        exception.getUserMessage());
    assertEquals("ref-parent", parentConfig.getProjectName());
  }

  @Test
  void projectWithoutHomeFolderDoesNotBlockRename() throws Exception {
    registerProject("ref-parent", null);
    ProjectConfig gone =
        new ProjectConfig(
            "ref-gone",
            tempRoot.resolve("does-not-exist").toString(),
            ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
    ProjectsConfigSingleton.getConfig().addProjectConfig(gone);
    registeredProjectNames.add("ref-gone");

    assertEquals(
        List.of(),
        ProjectsUtil.checkProjectRename(
            "ref-parent", "ref-renamed", new Variables(), LogChannel.GENERAL));
  }

  @Test
  void failingRegistrationSaveUndoesRename() throws Exception {
    ProjectConfig parentConfig = registerProject("ref-parent", null);
    ProjectConfig childConfig = registerProject("ref-child", "ref-parent");
    LifecycleEnvironment environment = registerEnvironment("ref-env", "ref-parent");

    parentConfig.setProjectName("ref-renamed");
    registeredProjectNames.add("ref-renamed");
    assertThrows(
        HopException.class,
        () ->
            ProjectsUtil.saveProjectConfig(
                "ref-parent",
                parentConfig,
                new Variables(),
                LogChannel.GENERAL,
                () -> {
                  throw new HopException("disk full");
                }));

    assertEquals("ref-parent", parentConfig.getProjectName());
    assertEquals("ref-parent", environment.getProjectName());
    assertEquals("ref-parent", childConfig.loadProject(new Variables()).getParentProjectName());
  }

  @Test
  void failingChildSaveRestoresEveryProject() throws Exception {
    ProjectConfig parentConfig = registerProject("ref-parent", null);
    ProjectConfig firstChild = registerProject("ref-child-a", "ref-parent");
    registerProject("ref-child-b", "ref-parent");
    // A home folder which can't be written makes saving the second child fail
    File secondHome = tempRoot.resolve("ref-child-b").toFile();
    File secondConfig = new File(secondHome, ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
    assumeTrue(secondConfig.setWritable(false) && !secondConfig.canWrite());
    assumeTrue(secondHome.setWritable(false) && !secondHome.canWrite());

    try {
      HopException exception =
          assertThrows(HopException.class, () -> rename(parentConfig, "ref-parent", "ref-renamed"));

      assertTrue(
          exception
              .getMessage()
              .contains(
                  "Project 'ref-parent' wasn't renamed to 'ref-renamed': the parent project of"
                      + " 'ref-child-b' couldn't be saved. The rename was undone."),
          exception.getMessage());
      assertEquals("ref-parent", parentConfig.getProjectName());
      assertEquals("ref-parent", firstChild.loadProject(new Variables()).getParentProjectName());
      // Registration saved with the new name, then saved again with the old one
      assertEquals(2, saveCount);
    } finally {
      secondHome.setWritable(true);
      secondConfig.setWritable(true);
    }
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

  /** Rename the registered instance in place, the way the project dialogs do. */
  private List<String> rename(ProjectConfig projectConfig, String currentName, String newName)
      throws HopException {
    projectConfig.setProjectName(newName);
    registeredProjectNames.add(newName);
    return ProjectsUtil.saveProjectConfig(
        currentName, projectConfig, new Variables(), LogChannel.GENERAL, () -> saveCount++);
  }

  private LifecycleEnvironment registerEnvironment(String name, String projectName) {
    LifecycleEnvironment environment =
        new LifecycleEnvironment(name, "Testing", projectName, List.of());
    ProjectsConfigSingleton.getConfig().addEnvironment(environment);
    registeredEnvironmentNames.add(name);
    return environment;
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
