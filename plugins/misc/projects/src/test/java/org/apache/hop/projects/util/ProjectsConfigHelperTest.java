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

package org.apache.hop.projects.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.config.ProjectsGuiOptionPlugin;
import org.apache.hop.projects.config.ProjectsOptionPlugin;
import org.apache.hop.projects.config.ProjectsRunOptionPlugin;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProjectsConfigHelperTest {

  private Path tempRoot;
  private final List<String> registeredProjects = new ArrayList<>();
  private final List<String> registeredEnvironments = new ArrayList<>();
  private MultiMetadataProvider previousMetadataProvider;

  @BeforeAll
  public static void beforeAll() {
    HopLogStore.init();
  }

  @BeforeEach
  public void setUp() throws Exception {
    tempRoot = Files.createTempDirectory("hop-in-memory-test");
    previousMetadataProvider = HopMetadataInstance.getMetadataProvider();
  }

  @AfterEach
  public void tearDown() throws Exception {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    for (String p : registeredProjects) {
      config.removeProjectConfig(p);
    }
    registeredProjects.clear();
    for (String e : registeredEnvironments) {
      config.removeEnvironment(e);
    }
    registeredEnvironments.clear();
    ProjectsConfigHelper.clearSessionRegisteredProjects();
    ProjectsGuiOptionPlugin.clearRequested();
    HopMetadataInstance.setMetadataProvider(previousMetadataProvider);

    if (tempRoot != null && Files.exists(tempRoot)) {
      try (Stream<Path> walk = Files.walk(tempRoot)) {
        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
      tempRoot = null;
    }
    HopConfig.setInMemoryMode(false);
  }

  private static final class TestMetadataHolder implements IHasHopMetadataProvider {
    private MultiMetadataProvider metadataProvider;

    @Override
    public MultiMetadataProvider getMetadataProvider() {
      return metadataProvider;
    }

    @Override
    public void setMetadataProvider(MultiMetadataProvider metadataProvider) {
      this.metadataProvider = metadataProvider;
    }
  }

  @Test
  public void testInMemoryMode() throws Exception {
    HopConfig.setInMemoryMode(true);
    assertTrue(HopConfig.isInMemoryMode());

    // Saving option should not throw and should not write to disk
    HopConfig.getInstance().saveOption("test_in_memory_key", "test_val");
    assertEquals("test_val", HopConfig.readOption("test_in_memory_key"));
    HopConfig.getInstance().saveToFile();
  }

  @Test
  public void testNormalizeProjectHome() {
    IVariables variables = new Variables();
    variables.setVariable("MY_DIR", "/projects/test");

    String normalDir = ProjectsConfigHelper.normalizeProjectHome("${MY_DIR}", variables);
    assertEquals("/projects/test", normalDir);

    String zipPath = "/tmp/my-archive.zip";
    String normalZip = ProjectsConfigHelper.normalizeProjectHome(zipPath, variables);
    assertTrue(normalZip.startsWith("zip:"));
    assertTrue(normalZip.endsWith(".zip!/"));

    String alreadyArchive = "zip:file:///path/project.zip!/subfolder";
    assertEquals(
        alreadyArchive, ProjectsConfigHelper.normalizeProjectHome(alreadyArchive, variables));
  }

  @Test
  public void testAddProjectLocationsAndParentChildRelationship() throws Exception {
    Path sharedDir = tempRoot.resolve("shared");
    Path edwDir = tempRoot.resolve("edw");
    Files.createDirectories(sharedDir);
    Files.createDirectories(edwDir);

    // Write minimal config for shared (parent)
    String sharedConfig =
        "{\n"
            + "  \"description\" : \"shared project\",\n"
            + "  \"metadataBaseFolder\" : \"${PROJECT_HOME}/metadata\",\n"
            + "  \"parentProjectName\" : null,\n"
            + "  \"config\" : { \"variables\" : [ ] }\n"
            + "}\n";
    Files.writeString(
        sharedDir.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME),
        sharedConfig,
        StandardCharsets.UTF_8);

    // Write minimal config for edw (child referencing shared) using hop-project.config filename
    String edwConfig =
        "{\n"
            + "  \"description\" : \"edw project\",\n"
            + "  \"metadataBaseFolder\" : \"${PROJECT_HOME}/metadata\",\n"
            + "  \"parentProjectName\" : \"shared\",\n"
            + "  \"config\" : { \"variables\" : [ ] }\n"
            + "}\n";
    Files.writeString(edwDir.resolve("hop-project.config"), edwConfig, StandardCharsets.UTF_8);

    IVariables variables = new Variables();
    String locParam = "shared=" + sharedDir.toAbsolutePath() + ",edw=" + edwDir.toAbsolutePath();

    List<String> registered =
        ProjectsConfigHelper.addProjectLocations(
            LogChannel.GENERAL, variables, new String[] {locParam});
    registeredProjects.addAll(registered);

    assertEquals(2, registered.size());
    assertTrue(registered.contains("shared"));
    assertTrue(registered.contains("edw"));

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    ProjectConfig sharedPc = config.findProjectConfig("shared");
    assertNotNull(sharedPc);
    ProjectConfig edwPc = config.findProjectConfig("edw");
    assertNotNull(edwPc);
    assertEquals("hop-project.config", edwPc.getConfigFilename());

    // Verify leaf project resolution correctly identifies edw (child) as leaf
    String leaf = ProjectsConfigHelper.findLeafProject(registered, config, variables);
    assertEquals("edw", leaf);

    // Load edw and verify parentProjectName resolves shared
    Project edwProject = edwPc.loadProject(variables);
    assertEquals("shared", edwProject.getParentProjectName());
  }

  @Test
  public void testAddEnvironments() throws Exception {
    Path edwDir = tempRoot.resolve("edw");
    Files.createDirectories(edwDir);
    Path confFile = tempRoot.resolve("edw-production.json");
    Files.writeString(confFile, "{}", StandardCharsets.UTF_8);

    ProjectConfig edwPc =
        new ProjectConfig("edw", edwDir.toAbsolutePath().toString(), "project-config.json");
    ProjectsConfigSingleton.getConfig().addProjectConfig(edwPc);
    registeredProjects.add("edw");

    IVariables variables = new Variables();
    String[] envDefs = new String[] {"edw-prod=" + confFile.toAbsolutePath()};

    ProjectsConfigHelper.addEnvironments(LogChannel.GENERAL, variables, envDefs, List.of("edw"));
    registeredEnvironments.add("edw-prod");

    LifecycleEnvironment env = ProjectsConfigSingleton.getConfig().findEnvironment("edw-prod");
    assertNotNull(env);
    assertEquals("edw-prod", env.getName());
    assertEquals("edw", env.getProjectName());
    assertEquals(1, env.getConfigurationFiles().size());
    assertEquals(confFile.toAbsolutePath().toString(), env.getConfigurationFiles().get(0));
  }

  @Test
  public void testProjectExportZipHandling() throws Exception {
    Path zipFile = tempRoot.resolve("export.zip");

    String projConfig =
        "{\n"
            + "  \"description\" : \"exported project\",\n"
            + "  \"metadataBaseFolder\" : \"${PROJECT_HOME}/metadata\",\n"
            + "  \"parentProjectName\" : null,\n"
            + "  \"config\" : { \"variables\" : [ ] }\n"
            + "}\n";

    String variablesJson = "{\"MY_EXPORTED_VAR\":\"hello_world\"}";
    String metadataJson = "{}";

    try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipFile))) {
      // Add project-config.json
      zos.putNextEntry(new ZipEntry(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME));
      zos.write(projConfig.getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();

      // Add variables.json
      zos.putNextEntry(new ZipEntry("variables.json"));
      zos.write(variablesJson.getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();

      // Add metadata.json
      zos.putNextEntry(new ZipEntry("metadata.json"));
      zos.write(metadataJson.getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    }

    IVariables variables = new Variables();
    String loc = "exported=" + zipFile.toAbsolutePath();
    List<String> registered =
        ProjectsConfigHelper.addProjectLocations(LogChannel.GENERAL, variables, new String[] {loc});
    registeredProjects.addAll(registered);

    assertEquals(1, registered.size());
    assertEquals("exported", registered.get(0));

    ProjectConfig pc = ProjectsConfigSingleton.getConfig().findProjectConfig("exported");
    assertNotNull(pc);
    assertTrue(pc.isReadOnly());

    MultiMetadataProvider metadataProvider =
        HopMetadataUtil.getStandardHopMetadataProvider(variables);
    int initialProvidersCount = metadataProvider.getProviders().size();

    ProjectsConfigHelper.applyProjectExportFiles(
        LogChannel.GENERAL, pc.getProjectHome(), variables, metadataProvider);

    assertEquals("hello_world", variables.getVariable("MY_EXPORTED_VAR"));
    assertEquals(initialProvidersCount + 1, metadataProvider.getProviders().size());
  }

  @Test
  public void testProjectsOptionPlugin() throws Exception {
    Path projectDir = tempRoot.resolve("my-proj");
    Files.createDirectories(projectDir);
    Path confFile = tempRoot.resolve("my-conf.json");
    Files.writeString(
        confFile,
        "{\n"
            + "  \"description\" : \"my environment config\",\n"
            + "  \"variables\" : [ {\"name\" : \"TEST_ENV_VAR\", \"value\" : \"test_val\"} ]\n"
            + "}\n",
        StandardCharsets.UTF_8);

    String projectConfig =
        "{\n"
            + "  \"description\" : \"my test project\",\n"
            + "  \"metadataBaseFolder\" : \"${PROJECT_HOME}/metadata\",\n"
            + "  \"parentProjectName\" : null,\n"
            + "  \"config\" : { \"variables\" : [ ] }\n"
            + "}\n";
    Files.writeString(
        projectDir.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME),
        projectConfig,
        StandardCharsets.UTF_8);

    ProjectsOptionPlugin plugin = new ProjectsOptionPlugin();
    plugin.setProjectLocations(new String[] {"my-proj=" + projectDir.toAbsolutePath()});
    plugin.setEnvironments(new String[] {"my-env=" + confFile.toAbsolutePath()});
    plugin.setEnvironmentOption("my-env");

    IVariables variables = new Variables();
    plugin.handleOption(LogChannel.GENERAL, null, variables);

    registeredProjects.add("my-proj");
    registeredEnvironments.add("my-env");

    assertTrue(HopConfig.isInMemoryMode());
    assertNotNull(ProjectsConfigSingleton.getConfig().findProjectConfig("my-proj"));
    assertNotNull(ProjectsConfigSingleton.getConfig().findEnvironment("my-env"));
  }

  @Test
  public void testDetermineActiveProjectFromSessionRegistration() throws Exception {
    Path projectDir = tempRoot.resolve("session-proj");
    Files.createDirectories(projectDir);
    Files.writeString(
        projectDir.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME),
        "{\n  \"metadataBaseFolder\" : \"${PROJECT_HOME}/metadata\"\n}\n",
        StandardCharsets.UTF_8);

    IVariables variables = new Variables();
    List<String> registered =
        ProjectsConfigHelper.addProjectLocations(
            LogChannel.GENERAL,
            variables,
            new String[] {"session-proj=" + projectDir.toAbsolutePath()});
    registeredProjects.addAll(registered);

    // Subcommand mixin has no --project-locations of its own
    String determined =
        ProjectsConfigHelper.determineActiveProject(null, null, List.of(), variables);
    assertEquals("session-proj", determined);
  }

  @Test
  public void testRootThenRunMixinUsesProjectMetadataFolder() throws Exception {
    Path projectDir = tempRoot.resolve("ttt");
    Path metadataDir = projectDir.resolve("metadata").resolve("pipeline-run-configuration");
    Files.createDirectories(metadataDir);
    Files.writeString(
        projectDir.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME),
        "{\n"
            + "  \"description\" : \"in-memory project\",\n"
            + "  \"metadataBaseFolder\" : \"${PROJECT_HOME}/metadata\",\n"
            + "  \"parentProjectName\" : null,\n"
            + "  \"config\" : { \"variables\" : [ ] }\n"
            + "}\n",
        StandardCharsets.UTF_8);
    Files.writeString(
        metadataDir.resolve("local.json"),
        "{ \"name\" : \"local\", \"engine\" : \"local\" }\n",
        StandardCharsets.UTF_8);

    // Root mixin: hop --project-locations ttt=/path run ...
    ProjectsOptionPlugin rootPlugin = new ProjectsOptionPlugin();
    rootPlugin.setProjectLocations(new String[] {"ttt=" + projectDir.toAbsolutePath()});
    IVariables variables = new Variables();
    rootPlugin.handleOption(LogChannel.GENERAL, null, variables);
    registeredProjects.add("ttt");

    String metadataFolder = variables.getVariable(Const.HOP_METADATA_FOLDER);
    assertNotNull(metadataFolder);
    assertTrue(
        metadataFolder.replace('\\', '/').contains("ttt")
            && metadataFolder.replace('\\', '/').contains("metadata"),
        "HOP_METADATA_FOLDER should point at the project metadata: " + metadataFolder);

    MultiMetadataProvider instanceProvider = HopMetadataInstance.getMetadataProvider();
    assertNotNull(instanceProvider);
    assertTrue(
        instanceProvider.getDescription().contains("metadata"), instanceProvider.getDescription());

    // Run mixin: no --project-locations on the subcommand, but the root already registered ttt
    TestMetadataHolder runHolder = new TestMetadataHolder();
    runHolder.setMetadataProvider(HopMetadataUtil.getStandardHopMetadataProvider(new Variables()));
    ProjectsOptionPlugin runPlugin = new ProjectsRunOptionPlugin();
    runPlugin.handleOption(LogChannel.GENERAL, runHolder, variables);

    assertNotNull(runHolder.getMetadataProvider());
    String runDescription = runHolder.getMetadataProvider().getDescription();
    assertTrue(
        runDescription.contains("ttt") && runDescription.contains("metadata"),
        "run metadata provider should use the project folder: " + runDescription);
  }
}
