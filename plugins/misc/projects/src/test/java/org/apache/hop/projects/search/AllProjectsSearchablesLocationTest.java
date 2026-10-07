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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hop.core.config.ConfigFileSerializer;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchablesLocation;
import org.apache.hop.core.security.HopRole;
import org.apache.hop.core.security.HopSecurity;
import org.apache.hop.core.security.HopSecurityContext;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.security.ProjectsAccessConfig;
import org.apache.hop.projects.security.ProjectsAccessRule;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.projects.util.ProjectsUtil;
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
    HopSecurity.reset();
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
    HopSecurity.reset();
    ProjectsAccessConfig.clearCache();
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
    assertTrue(
        ((AllProjectsSearchablesLocation) locations.get(1))
            .getAllowedProjectNames()
            .contains("issue-6146-active"));
  }

  @Test
  void accessControlFiltersTheAllProjectsList() throws Exception {
    tempRoot = Files.createTempDirectory("hop-6146-access");
    Path projectA = tempRoot.resolve("allowed");
    Path projectB = tempRoot.resolve("denied");
    Files.createDirectories(projectA);
    Files.createDirectories(projectB);
    writeVariables(projectA.resolve("env.json"), "ISSUE_6146_ALLOWED", "yes");
    writeVariables(projectB.resolve("env.json"), "ISSUE_6146_DENIED", "no");
    registerProject("issue-6146-allowed", projectA);
    registerProject("issue-6146-denied", projectB);
    registerEnvironment(
        "issue-6146-allowed-env", "issue-6146-allowed", projectA.resolve("env.json"));
    registerEnvironment("issue-6146-denied-env", "issue-6146-denied", projectB.resolve("env.json"));

    ProjectsAccessConfig access = new ProjectsAccessConfig();
    access.setEnabled(true);
    access.setDefaultAllowAll(false);
    access.setRules(
        List.of(
            new ProjectsAccessRule(
                ProjectsAccessRule.TYPE_USER, "viewer", false, List.of("issue-6146-allowed"))));
    setAccessCache(access);
    HopSecurity.setProvider(() -> HopSecurityContext.forUser("viewer", Set.of(HopRole.READ_ONLY)));
    try {
      HopNamespace.setNamespace(null);
      List<ISearchablesLocation> locations = new ArrayList<>();
      new AddProjectsSearchablesLocationExtensionPoint()
          .callExtensionPoint(LogChannel.GENERAL, new Variables(), locations);

      AllProjectsSearchablesLocation all = null;
      for (ISearchablesLocation location : locations) {
        if (location instanceof AllProjectsSearchablesLocation candidate) {
          all = candidate;
        }
      }
      assertNotNull(all);
      assertTrue(all.getAllowedProjectNames().contains("issue-6146-allowed"));
      assertFalse(containsIgnoreCase(all.getAllowedProjectNames(), "issue-6146-denied"));

      // The search thread may not see the session. A live check would allow every project again.
      HopSecurity.reset();
      ProjectsAccessConfig.clearCache();
      assertTrue(
          containsIgnoreCase(
              AllProjectsSearchablesLocation.allowedProjectNames(), "issue-6146-denied"));

      List<String> found = names(search(all, new Variables()));
      assertTrue(found.contains("ISSUE_6146_ALLOWED"), found.toString());
      assertFalse(found.contains("ISSUE_6146_DENIED"), found.toString());
    } finally {
      HopSecurity.reset();
      ProjectsAccessConfig.clearCache();
    }
  }

  @Test
  void defaultSearchUsesOnlyTheActiveEnvironment() throws Exception {
    tempRoot = Files.createTempDirectory("hop-6146-env");
    Path home = tempRoot.resolve("home");
    Files.createDirectories(home);
    writeVariables(home.resolve("env-dev.json"), "ISSUE_6146_DEV", "dev");
    writeVariables(home.resolve("env-prod.json"), "ISSUE_6146_PROD", "prod");
    registerProject("issue-6146-env", home);
    // dev is first. The active environment is prod, so a "first environment" search would be wrong.
    registerEnvironment("issue-6146-dev", "issue-6146-env", "${PROJECT_HOME}/env-dev.json");
    registerEnvironment("issue-6146-prod", "issue-6146-env", "${PROJECT_HOME}/env-prod.json");

    IVariables variables = new Variables();
    variables.setVariable(ProjectsUtil.VARIABLE_PROJECT_HOME, home.toAbsolutePath().toString());
    variables.setVariable(Defaults.VARIABLE_HOP_ENVIRONMENT_NAME, "issue-6146-prod");

    ProjectConfig projectConfig =
        ProjectsConfigSingleton.getConfig().findProjectConfig("issue-6146-env");
    List<ISearchable> active = search(new ProjectsSearchablesLocation(projectConfig), variables);
    List<String> activeNames = names(active);
    assertTrue(activeNames.contains("ISSUE_6146_PROD"), activeNames.toString());
    assertFalse(activeNames.contains("ISSUE_6146_DEV"), activeNames.toString());
    assertEquals(
        home.resolve("env-prod.json").toAbsolutePath().toString(),
        find(active, "ISSUE_6146_PROD").getFilename());

    List<String> allNames = names(search(new AllProjectsSearchablesLocation(), new Variables()));
    assertTrue(allNames.contains("ISSUE_6146_DEV"), allNames.toString());
    assertTrue(allNames.contains("ISSUE_6146_PROD"), allNames.toString());
  }

  @Test
  void variableSearchableKeyUsesTheResolvedConfigurationFile() throws Exception {
    tempRoot = Files.createTempDirectory("hop-6146-resolved");
    Path projectA = tempRoot.resolve("project-a");
    Path projectB = tempRoot.resolve("project-b");
    Files.createDirectories(projectA);
    Files.createDirectories(projectB);
    writeVariables(projectA.resolve("env-dev.json"), "ISSUE_6146_SHARED", "from-a");
    writeVariables(projectB.resolve("env-dev.json"), "ISSUE_6146_SHARED", "from-b");
    registerProject("issue-6146-a", projectA);
    registerProject("issue-6146-b", projectB);
    registerEnvironment("issue-6146-dev-a", "issue-6146-a", "${PROJECT_HOME}/env-dev.json");
    registerEnvironment("issue-6146-dev-b", "issue-6146-b", "${PROJECT_HOME}/env-dev.json");

    List<ISearchable> shared = new ArrayList<>();
    for (ISearchable searchable : search(new AllProjectsSearchablesLocation(), new Variables())) {
      if ("ISSUE_6146_SHARED".equals(searchable.getName())) {
        shared.add(searchable);
      }
    }
    assertEquals(2, shared.size(), names(shared).toString());
    assertNotEquals(
        HopGuiSearchHelper.searchableKey(shared.get(0)),
        HopGuiSearchHelper.searchableKey(shared.get(1)));
    List<String> filenames = new ArrayList<>();
    for (ISearchable searchable : shared) {
      filenames.add(searchable.getFilename());
      assertFalse(searchable.getFilename().contains("${"));
    }
    assertTrue(
        filenames.contains(projectA.resolve("env-dev.json").toAbsolutePath().toString()),
        filenames.toString());
    assertTrue(
        filenames.contains(projectB.resolve("env-dev.json").toAbsolutePath().toString()),
        filenames.toString());
  }

  private static List<String> names(List<ISearchable> searchables) {
    List<String> names = new ArrayList<>();
    for (ISearchable searchable : searchables) {
      names.add(searchable.getName());
    }
    return names;
  }

  private static List<ISearchable> search(ISearchablesLocation location, IVariables variables)
      throws Exception {
    List<ISearchable> searchables = new ArrayList<>();
    Iterator<ISearchable> iterator =
        location.getSearchables(
            HopMetadataUtil.getStandardHopMetadataProvider(variables), variables);
    while (iterator.hasNext()) {
      searchables.add(iterator.next());
    }
    return searchables;
  }

  private static ISearchable find(List<ISearchable> searchables, String name) {
    for (ISearchable searchable : searchables) {
      if (name.equals(searchable.getName())) {
        return searchable;
      }
    }
    return null;
  }

  private static boolean containsIgnoreCase(List<String> names, String value) {
    for (String name : names) {
      if (name != null && name.equalsIgnoreCase(value)) {
        return true;
      }
    }
    return false;
  }

  /** Install an access config without writing the developer's projects-access.json. */
  private static void setAccessCache(ProjectsAccessConfig config) throws Exception {
    Field field = ProjectsAccessConfig.class.getDeclaredField("cached");
    field.setAccessible(true);
    field.set(null, config);
  }

  private void registerProject(String name, Path home) {
    ProjectsConfigSingleton.getConfig()
        .addProjectConfig(
            new ProjectConfig(name, home.toAbsolutePath().toString(), "project-config.json"));
    projectNames.add(name);
  }

  private void registerEnvironment(String name, String projectName, Path configFile) {
    registerEnvironment(name, projectName, configFile.toAbsolutePath().toString());
  }

  private void registerEnvironment(String name, String projectName, String configFile) {
    LifecycleEnvironment environment =
        new LifecycleEnvironment(name, "Testing", projectName, List.of(configFile));
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
