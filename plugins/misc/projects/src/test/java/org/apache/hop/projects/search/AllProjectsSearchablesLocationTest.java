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

package org.apache.hop.projects.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.config.ConfigFileSerializer;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchablesLocation;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.search.HopGuiSearchHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AllProjectsSearchablesLocationTest {

  private final List<String> projectNames = new ArrayList<>();
  private final List<String> environmentNames = new ArrayList<>();
  private Path tempRoot;
  private String previousNamespace;
  private List<ProjectConfig> previousProjects;
  private List<LifecycleEnvironment> previousEnvironments;

  @BeforeEach
  void setUp() {
    HopLogStore.init();
    try {
      previousNamespace = HopNamespace.getNamespace();
    } catch (RuntimeException e) {
      previousNamespace = null;
    }
    // Search reads the live configuration. Keep this test off the developer's projects.
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    previousProjects = new ArrayList<>(safeProjects(config));
    previousEnvironments = new ArrayList<>(safeEnvironments(config));
    safeProjects(config).clear();
    safeEnvironments(config).clear();
  }

  @AfterEach
  void tearDown() throws Exception {
    HopNamespace.setNamespace(previousNamespace);
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    safeProjects(config).clear();
    safeProjects(config).addAll(previousProjects);
    safeEnvironments(config).clear();
    safeEnvironments(config).addAll(previousEnvironments);
    environmentNames.clear();
    projectNames.clear();
    if (tempRoot != null && Files.exists(tempRoot)) {
      try (Stream<Path> walk = Files.walk(tempRoot)) {
        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
      tempRoot = null;
    }
  }

  @Test
  void searchesEveryProjectAndEveryEnvironment() throws Exception {
    tempRoot = Files.createTempDirectory("hop-6146");
    Path projectA = tempRoot.resolve("project-a");
    Path projectB = tempRoot.resolve("project-b");
    Files.createDirectories(projectA);
    Files.createDirectories(projectB);

    Path devFile = projectA.resolve("env-dev.json");
    Path prodFile = projectA.resolve("env-prod.json");
    Path betaFile = projectB.resolve("env-beta.json");
    writeVariables(devFile, "ISSUE_6146_ALPHA", "from-dev");
    writeVariables(prodFile, "ISSUE_6146_PROD", "from-prod");
    writeVariables(betaFile, "ISSUE_6146_BETA", "from-beta");

    registerProject("issue-6146-a", projectA);
    registerProject("issue-6146-b", projectB);
    registerProject("issue-6146-missing", tempRoot.resolve("does-not-exist"));
    registerEnvironment("issue-6146-dev", "issue-6146-a", devFile);
    registerEnvironment("issue-6146-prod", "issue-6146-a", prodFile);
    registerEnvironment("issue-6146-beta", "issue-6146-b", betaFile);

    AllProjectsSearchablesLocation location = new AllProjectsSearchablesLocation();
    assertEquals(AllProjectsSearchablesLocation.LOCATION_ID, location.getLocationId());
    assertEquals(AllProjectsSearchablesLocation.DESCRIPTION, location.getLocationDescription());
    assertFalse(location.isIncludedInDefaultSearch());
    assertTrue(HopGuiSearchHelper.selectLocations(List.of(location), 0).isEmpty());
    assertEquals(location, HopGuiSearchHelper.selectLocations(List.of(location), 1).get(0));

    List<ISearchable> searchables = new ArrayList<>();
    Iterator<ISearchable> iterator = location.getSearchables(null, new Variables());
    while (iterator.hasNext()) {
      searchables.add(iterator.next());
    }

    assertTrue(names(searchables).contains("ISSUE_6146_ALPHA"), names(searchables).toString());
    assertTrue(names(searchables).contains("ISSUE_6146_PROD"), names(searchables).toString());
    assertTrue(names(searchables).contains("ISSUE_6146_BETA"), names(searchables).toString());
    assertEquals(1, names(searchables).stream().filter("ISSUE_6146_PROD"::equals).count());
  }

  @Test
  void extensionPointAddsAllProjectsEvenWithoutAnActiveProject() throws Exception {
    HopNamespace.setNamespace(null);
    List<ISearchablesLocation> locations = new ArrayList<>();
    new AddProjectsSearchablesLocationExtensionPoint()
        .callExtensionPoint(LogChannel.GENERAL, new Variables(), locations);

    assertEquals(1, locations.size());
    assertInstanceOf(AllProjectsSearchablesLocation.class, locations.get(0));
    assertFalse(locations.get(0).isIncludedInDefaultSearch());
  }

  @Test
  void extensionPointKeepsTheActiveProjectAndAppendsAllProjects() throws Exception {
    tempRoot = Files.createTempDirectory("hop-6146-active");
    Path home = tempRoot.resolve("active");
    Files.createDirectories(home);
    registerProject("issue-6146-active", home);
    HopNamespace.setNamespace("issue-6146-active");

    List<ISearchablesLocation> locations = new ArrayList<>();
    new AddProjectsSearchablesLocationExtensionPoint()
        .callExtensionPoint(LogChannel.GENERAL, new Variables(), locations);

    assertEquals(2, locations.size());
    assertInstanceOf(ProjectsSearchablesLocation.class, locations.get(0));
    assertEquals("Project issue-6146-active", locations.get(0).getLocationDescription());
    assertInstanceOf(AllProjectsSearchablesLocation.class, locations.get(1));
  }

  private static List<String> names(List<ISearchable> searchables) {
    List<String> names = new ArrayList<>();
    for (ISearchable searchable : searchables) {
      names.add(searchable.getName());
    }
    return names;
  }

  private void registerProject(String name, Path home) {
    ProjectsConfigSingleton.getConfig()
        .addProjectConfig(
            new ProjectConfig(name, home.toAbsolutePath().toString(), "project-config.json"));
    projectNames.add(name);
  }

  private void registerEnvironment(String name, String projectName, Path configFile) {
    LifecycleEnvironment environment =
        new LifecycleEnvironment(
            name, "Testing", projectName, List.of(configFile.toAbsolutePath().toString()));
    ProjectsConfigSingleton.getConfig().addEnvironment(environment);
    environmentNames.add(name);
  }

  private static void writeVariables(Path file, String name, String value) throws Exception {
    DescribedVariablesConfigFile configFile =
        new DescribedVariablesConfigFile(file.toAbsolutePath().toString());
    configFile.setDescribedVariables(List.of(new DescribedVariable(name, value, "test")));
    new ConfigFileSerializer()
        .writeToFile(file.toAbsolutePath().toString(), configFile.getConfigMap());
  }

  private static List<ProjectConfig> safeProjects(ProjectsConfig config) {
    if (config.getProjectConfigurations() == null) {
      config.setProjectConfigurations(new ArrayList<>());
    }
    return config.getProjectConfigurations();
  }

  private static List<LifecycleEnvironment> safeEnvironments(ProjectsConfig config) {
    if (config.getLifecycleEnvironments() == null) {
      config.setLifecycleEnvironments(new ArrayList<>());
    }
    return config.getLifecycleEnvironments();
  }
}
