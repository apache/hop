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

package org.apache.hop.projects.environment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.EmbeddedEnvironmentMaterializer.MaterializeResult;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EmbeddedEnvironmentMaterializerTest {

  private final List<String> registeredEnvironments = new ArrayList<>();
  private Path tempRoot;

  @BeforeEach
  void setUp() throws Exception {
    HopLogStore.init();
    tempRoot = Files.createTempDirectory("hop-embedded-env");
  }

  @AfterEach
  void tearDown() throws Exception {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    for (String name : registeredEnvironments) {
      config.removeEnvironment(name);
    }
    registeredEnvironments.clear();
    if (tempRoot != null && Files.exists(tempRoot)) {
      try (Stream<Path> walk = Files.walk(tempRoot)) {
        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
      tempRoot = null;
    }
  }

  @Test
  void configFileNameSanitizesPathCharacters() {
    assertEquals("dev.json", EmbeddedEnvironmentMaterializer.configFileName("dev"));
    assertEquals("_dev.json", EmbeddedEnvironmentMaterializer.configFileName("../dev"));
    assertEquals("a_b.json", EmbeddedEnvironmentMaterializer.configFileName("a/b"));
    assertEquals("My_Project.json", EmbeddedEnvironmentMaterializer.configFileName("My Project"));
    assertEquals("_hidden.json", EmbeddedEnvironmentMaterializer.configFileName(".hidden"));
    assertEquals("environment.json", EmbeddedEnvironmentMaterializer.configFileName("   "));
  }

  @Test
  void suggestedConfigFolderStaysOutsideTheProject() {
    assertEquals(
        "${HOP_CONFIG_FOLDER}/environments/My_Project",
        EmbeddedEnvironmentMaterializer.suggestedConfigFolder("My Project"));
    assertEquals(
        "${HOP_CONFIG_FOLDER}/environments/project",
        EmbeddedEnvironmentMaterializer.suggestedConfigFolder(null));
    assertEquals(
        "${HOP_CONFIG_FOLDER}/environments/_dev",
        EmbeddedEnvironmentMaterializer.suggestedConfigFolder("../dev"));
  }

  @Test
  void resolveFolderUsesTheProjectVariable() {
    assertNull(EmbeddedEnvironmentMaterializer.resolveFolder(null));

    IVariables missing = new Variables();
    assertNull(EmbeddedEnvironmentMaterializer.resolveFolder(missing));

    IVariables blank = new Variables();
    blank.setVariable(EmbeddedEnvironmentMaterializer.VARIABLE_ENVIRONMENTS_FOLDER, "   ");
    assertNull(EmbeddedEnvironmentMaterializer.resolveFolder(blank));

    IVariables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "/data/proj");
    variables.setVariable(
        EmbeddedEnvironmentMaterializer.VARIABLE_ENVIRONMENTS_FOLDER,
        "${PROJECT_HOME}/environments");
    assertEquals(
        "/data/proj/environments", EmbeddedEnvironmentMaterializer.resolveFolder(variables));
  }

  @Test
  void configFilePathDoesNotEscapeTheFolder() throws Exception {
    String path = EmbeddedEnvironmentMaterializer.configFilePath(tempRoot.toString(), "../dev");
    assertTrue(path.endsWith("_dev.json"));
    assertFalse(path.contains(".."));
    assertTrue(path.contains(tempRoot.getFileName().toString()));
  }

  @Test
  void insideProjectHomeMatchesTheHomeAndItsChildren() throws Exception {
    Path home = tempRoot.resolve("proj");
    Path extra = tempRoot.resolve("proj-extra");
    Path child = home.resolve("environments");
    Files.createDirectories(child);
    Files.createDirectories(extra);

    assertTrue(
        EmbeddedEnvironmentMaterializer.isInsideProjectHome(home.toString(), home.toString()));
    assertTrue(
        EmbeddedEnvironmentMaterializer.isInsideProjectHome(child.toString(), home.toString()));
    assertFalse(
        EmbeddedEnvironmentMaterializer.isInsideProjectHome(extra.toString(), home.toString()));
    assertFalse(EmbeddedEnvironmentMaterializer.isInsideProjectHome("", home.toString()));
    assertFalse(EmbeddedEnvironmentMaterializer.isInsideProjectHome(child.toString(), "  "));
  }

  @Test
  void variablesToStoreKeepsMandatoryAndSecretsOnly() {
    EmbeddedEnvironment environment = sampleEnvironment("dev");
    List<DescribedVariable> stored = EmbeddedEnvironmentMaterializer.variablesToStore(environment);

    assertEquals(2, stored.size());
    assertEquals("DB_HOST", stored.get(0).getName());
    assertEquals("specify the database host", stored.get(0).getValue());
    assertEquals("JDBC host", stored.get(0).getDescription());
    assertEquals("DB_PASSWORD", stored.get(1).getName());
    assertEquals("change-to-your-password", stored.get(1).getValue());
    assertEquals("JDBC password", stored.get(1).getDescription());
    assertTrue(EmbeddedEnvironmentMaterializer.variablesToStore(null).isEmpty());
  }

  @Test
  void materializeWritesMandatoryAndSecrets() throws Exception {
    EmbeddedEnvironment environment = sampleEnvironment("dev");
    environment
        .getMandatoryVariables()
        .add(new EmbeddedEnvironmentVariable("EMPTY_DEFAULT", null, null));
    Project project = new Project();
    project.getEmbeddedEnvironments().add(environment);
    project.getEmbeddedEnvironments().add(new EmbeddedEnvironment());
    project.getEmbeddedEnvironments().add(null);

    MaterializeResult result =
        EmbeddedEnvironmentMaterializer.materialize(
            new ProjectsConfig(), project, "warehouse", tempRoot.toString());

    assertEquals(1, result.getCreated().size());
    assertTrue(result.getSkippedExistingNames().isEmpty());
    assertTrue(result.getKeptExistingFiles().isEmpty());
    LifecycleEnvironment created = result.getCreated().get(0);
    assertEquals("dev", created.getName());
    assertEquals("", created.getPurpose());
    assertEquals("warehouse", created.getProjectName());
    assertEquals("dev", created.getEmbeddedEnvironmentName());
    assertEquals(1, created.getConfigurationFiles().size());

    String path = created.getConfigurationFiles().get(0);
    assertTrue(fileExists(path));
    String json = Files.readString(Path.of(path));
    assertFalse(json.contains("OPTIONAL_VAR"));
    assertFalse(json.contains("configurationFiles"));

    DescribedVariablesConfigFile configFile = new DescribedVariablesConfigFile(path);
    configFile.readFromFile();
    assertEquals("Developer workstation", configFile.getDescription());
    List<DescribedVariable> stored = configFile.getDescribedVariables();
    assertEquals(3, stored.size());
    assertEquals("DB_HOST", stored.get(0).getName());
    assertEquals("EMPTY_DEFAULT", stored.get(1).getName());
    assertEquals("", stored.get(1).getValue());
    assertEquals("", stored.get(1).getDescription());
    assertEquals("DB_PASSWORD", stored.get(2).getName());
    assertEquals("change-to-your-password", stored.get(2).getValue());

    assertTrue(
        EmbeddedEnvironmentMaterializer.materialize(null, null, "warehouse", tempRoot.toString())
            .getCreated()
            .isEmpty());
  }

  @Test
  void materializeSkipsAnEnvironmentThatAlreadyExists() throws Exception {
    ProjectsConfig config = new ProjectsConfig();
    config.addEnvironment(new LifecycleEnvironment("dev", "keep", "other", new ArrayList<>()));
    Project project = new Project();
    project.getEmbeddedEnvironments().add(sampleEnvironment("dev"));

    MaterializeResult result =
        EmbeddedEnvironmentMaterializer.materialize(
            config, project, "warehouse", tempRoot.toString());

    assertTrue(result.getCreated().isEmpty());
    assertEquals(List.of("dev"), result.getSkippedExistingNames());
    assertEquals("keep", config.findEnvironment("dev").getPurpose());
    assertFalse(
        fileExists(EmbeddedEnvironmentMaterializer.configFilePath(tempRoot.toString(), "dev")));
  }

  @Test
  void materializeKeepsAnExistingConfigurationFile() throws Exception {
    String path = EmbeddedEnvironmentMaterializer.configFilePath(tempRoot.toString(), "dev");
    DescribedVariablesConfigFile existing = new DescribedVariablesConfigFile(path);
    existing.setDescribedVariables(
        List.of(new DescribedVariable("DB_PASSWORD", "real-password", "JDBC password")));
    existing.saveToFile();

    Project project = new Project();
    project.getEmbeddedEnvironments().add(sampleEnvironment("dev"));
    MaterializeResult result =
        EmbeddedEnvironmentMaterializer.materialize(
            new ProjectsConfig(), project, "warehouse", tempRoot.toString());

    assertEquals(List.of("dev"), result.getKeptExistingFiles());
    assertEquals(1, result.getCreated().size());
    assertEquals(path, result.getCreated().get(0).getConfigurationFiles().get(0));

    DescribedVariablesConfigFile reread = new DescribedVariablesConfigFile(path);
    reread.readFromFile();
    assertEquals("real-password", reread.findDescribedVariableValue("DB_PASSWORD"));
    assertFalse(Files.readString(Path.of(path)).contains("change-to-your-password"));
  }

  @Test
  void materializeRequiresAFolder() {
    Project project = new Project();
    project.getEmbeddedEnvironments().add(sampleEnvironment("dev"));
    assertThrows(
        HopException.class,
        () ->
            EmbeddedEnvironmentMaterializer.materialize(
                new ProjectsConfig(), project, "warehouse", "  "));
  }

  @Test
  void lifecycleEnvironmentLinkIsCopiedAndOmittedWhenNull() throws Exception {
    LifecycleEnvironment environment =
        new LifecycleEnvironment("dev", "", "warehouse", new ArrayList<>());
    String withoutLink = HopJson.newMapper().writeValueAsString(environment);
    assertFalse(withoutLink.contains("embeddedEnvironmentName"));

    environment.setEmbeddedEnvironmentName("");
    String emptyLink = HopJson.newMapper().writeValueAsString(environment);
    assertTrue(emptyLink.contains("\"embeddedEnvironmentName\":\"\""));

    environment.setEmbeddedEnvironmentName("dev");
    String withLink = HopJson.newMapper().writeValueAsString(environment);
    assertTrue(withLink.contains("\"embeddedEnvironmentName\":\"dev\""));

    LifecycleEnvironment copy = new LifecycleEnvironment(environment);
    assertEquals("dev", copy.getEmbeddedEnvironmentName());
    copy.setEmbeddedEnvironmentName("prod");
    assertEquals("dev", environment.getEmbeddedEnvironmentName());
    copy.getConfigurationFiles().add("local.json");
    assertTrue(environment.getConfigurationFiles().isEmpty());
  }

  @Test
  void modifyVariablesAppliesDefaultsThenFilesThenProjectVariables() throws Exception {
    String name = unique("dev");
    String configPath = writeConfigFile(name, "DB_HOST", "db.example", "LOG_LEVEL", "FromFile");
    Project project = projectWithSample(name);
    project.setDescribedVariable(new DescribedVariable("LOG_LEVEL", "FromProject", "level"));
    register(name, name, List.of(configPath));

    IVariables variables = new Variables();
    project.modifyVariables(variables, projectConfig(), List.of(configPath), name);

    assertEquals("FromProject", variables.getVariable("LOG_LEVEL"));
    assertEquals("FromDefinition", variables.getVariable("OPTIONAL_VAR"));
    assertEquals("db.example", variables.getVariable("DB_HOST"));
    assertEquals("change-to-your-password", variables.getVariable("DB_PASSWORD"));
  }

  @Test
  void unlinkedEnvironmentDoesNotApplyEmbeddedDefaults() throws Exception {
    String name = unique("plain");
    String configPath = writeConfigFile(name, "FILE_ONLY", "yes");
    Project project = projectWithSample("dev");
    register(name, null, List.of(configPath));

    IVariables variables = new Variables();
    project.modifyVariables(variables, projectConfig(), List.of(configPath), name);

    assertEquals("yes", variables.getVariable("FILE_ONLY"));
    assertNull(variables.getVariable("OPTIONAL_VAR"));
    assertNull(variables.getVariable("DB_HOST"));
  }

  @Test
  void unknownEmbeddedEnvironmentIsLoggedAndSkipped() throws Exception {
    String name = unique("missing");
    Project project = projectWithSample("dev");
    register(name, "does-not-exist", List.of());

    int from = HopLogStore.getLastBufferLineNr();
    IVariables variables = new Variables();
    project.modifyVariables(variables, projectConfig(), List.of(), name);

    assertNull(variables.getVariable("OPTIONAL_VAR"));
    assertTrue(logContains(from, "does-not-exist"));
    assertTrue(logContains(from, "is not defined in this project"));
  }

  @Test
  void missingEnvironmentNameSkipsEmbeddedDefaults() throws Exception {
    Project project = projectWithSample("dev");
    IVariables variables = new Variables();
    project.modifyVariables(variables, projectConfig(), List.of(), null);
    assertNull(variables.getVariable("OPTIONAL_VAR"));
  }

  private EmbeddedEnvironment sampleEnvironment(String name) {
    EmbeddedEnvironment environment = new EmbeddedEnvironment();
    environment.setName(name);
    environment.setDescription("Developer workstation");
    environment
        .getVariables()
        .add(new EmbeddedEnvironmentVariable("OPTIONAL_VAR", "FromDefinition", "Optional"));
    environment
        .getVariables()
        .add(new EmbeddedEnvironmentVariable("LOG_LEVEL", "FromDefinition", "Hop log level"));
    environment
        .getMandatoryVariables()
        .add(new EmbeddedEnvironmentVariable("DB_HOST", "specify the database host", "JDBC host"));
    environment
        .getSecretVariables()
        .add(
            new EmbeddedEnvironmentVariable(
                "DB_PASSWORD", "change-to-your-password", "JDBC password"));
    return environment;
  }

  private Project projectWithSample(String embeddedName) {
    Project project = new Project();
    project.getEmbeddedEnvironments().add(sampleEnvironment(embeddedName));
    return project;
  }

  private ProjectConfig projectConfig() {
    return new ProjectConfig("embedded-project", tempRoot.toString(), "project-config.json");
  }

  private void register(String environmentName, String embeddedName, List<String> files) {
    LifecycleEnvironment environment =
        new LifecycleEnvironment(environmentName, "", "embedded-project", new ArrayList<>(files));
    environment.setEmbeddedEnvironmentName(embeddedName);
    ProjectsConfigSingleton.getConfig().addEnvironment(environment);
    registeredEnvironments.add(environmentName);
  }

  private String writeConfigFile(String environmentName, String... nameValuePairs)
      throws Exception {
    String path =
        EmbeddedEnvironmentMaterializer.configFilePath(tempRoot.toString(), environmentName);
    List<DescribedVariable> variables = new ArrayList<>();
    for (int i = 0; i < nameValuePairs.length; i += 2) {
      variables.add(new DescribedVariable(nameValuePairs[i], nameValuePairs[i + 1], ""));
    }
    DescribedVariablesConfigFile configFile = new DescribedVariablesConfigFile(path);
    configFile.setDescribedVariables(variables);
    configFile.saveToFile();
    return path;
  }

  private String unique(String suffix) {
    return "ee4193-" + suffix + "-" + System.nanoTime();
  }

  private static boolean fileExists(String path) throws Exception {
    try (FileObject file = HopVfs.getFileObject(path)) {
      return file.exists();
    }
  }

  private static boolean logContains(int from, String text) {
    List<HopLoggingEvent> events =
        HopLogStore.getLogBufferFromTo(
            List.of(LogChannel.GENERAL.getLogChannelId()),
            true,
            from,
            HopLogStore.getLastBufferLineNr());
    for (HopLoggingEvent event : events) {
      if (String.valueOf(event.getMessage()).contains(text)) {
        return true;
      }
    }
    return false;
  }
}
