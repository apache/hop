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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.projects.util.ProjectsUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class ProjectTest {

  private final java.util.List<String> registeredProjectNames = new ArrayList<>();
  private Path tempRoot;

  @AfterEach
  public void tearDown() throws Exception {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    for (String name : registeredProjectNames) {
      config.removeProjectConfig(name);
    }
    registeredProjectNames.clear();
    if (tempRoot != null && Files.exists(tempRoot)) {
      try (Stream<Path> walk = Files.walk(tempRoot)) {
        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
      tempRoot = null;
    }
  }

  @Test
  public void testEnforcingExecutionInHomeSerialization() throws Exception {
    File tempFile = Files.createTempFile("project-config-test", ".json").toFile();
    tempFile.deleteOnExit();

    try {
      Project project = new Project(tempFile.getAbsolutePath());
      project.setEnforcingExecutionInHome(false);
      project.saveToFile();

      // Read back
      Project readProject = new Project(tempFile.getAbsolutePath());
      readProject.readFromFile();

      assertFalse(readProject.isEnforcingExecutionInHome());

      // Test true
      project.setEnforcingExecutionInHome(true);
      project.saveToFile();

      readProject = new Project(tempFile.getAbsolutePath());
      readProject.readFromFile();

      assertTrue(readProject.isEnforcingExecutionInHome());
    } finally {
      tempFile.delete();
    }
  }

  @Test
  public void testParentProjectVariablesWhenNoParent() throws Exception {
    tempRoot = Files.createTempDirectory("hop-project-no-parent");
    Path home = tempRoot.resolve("child");
    Files.createDirectories(home);
    writeMinimalConfig(home, null);

    ProjectConfig projectConfig =
        new ProjectConfig(
            "orphan", home.toString(), ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
    registerProject(projectConfig);

    Project project = projectConfig.loadProject(new Variables());
    IVariables variables = new Variables();
    project.modifyVariables(variables, projectConfig, new ArrayList<>(), null);

    assertEquals("orphan", variables.getVariable(Defaults.VARIABLE_HOP_PROJECT_NAME));
    assertEquals(home.toString(), variables.getVariable(ProjectsUtil.VARIABLE_PROJECT_HOME));
    assertEquals("", variables.getVariable(ProjectsUtil.VARIABLE_PARENT_PROJECT_NAME));
    assertEquals("", variables.getVariable(ProjectsUtil.VARIABLE_PARENT_PROJECT_HOME));
  }

  @Test
  public void testParentProjectVariablesWithParent() throws Exception {
    tempRoot = Files.createTempDirectory("hop-project-with-parent");
    Path parentHome = tempRoot.resolve("parent");
    Path childHome = tempRoot.resolve("child");
    Files.createDirectories(parentHome);
    Files.createDirectories(childHome);
    writeMinimalConfig(parentHome, null);
    writeMinimalConfig(childHome, "parent-proj");

    ProjectConfig parentConfig =
        new ProjectConfig(
            "parent-proj", parentHome.toString(), ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
    ProjectConfig childConfig =
        new ProjectConfig(
            "child-proj", childHome.toString(), ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
    registerProject(parentConfig);
    registerProject(childConfig);

    Project child = childConfig.loadProject(new Variables());
    IVariables variables = new Variables();
    child.modifyVariables(variables, childConfig, new ArrayList<>(), null);

    assertEquals("child-proj", variables.getVariable(Defaults.VARIABLE_HOP_PROJECT_NAME));
    assertEquals(childHome.toString(), variables.getVariable(ProjectsUtil.VARIABLE_PROJECT_HOME));
    assertEquals("parent-proj", variables.getVariable(ProjectsUtil.VARIABLE_PARENT_PROJECT_NAME));
    assertEquals(
        parentHome.toString(), variables.getVariable(ProjectsUtil.VARIABLE_PARENT_PROJECT_HOME));
  }

  @Test
  public void testParentProjectVariablesImmediateParentOnly() throws Exception {
    tempRoot = Files.createTempDirectory("hop-project-chain");
    Path grandHome = tempRoot.resolve("grand");
    Path parentHome = tempRoot.resolve("parent");
    Path childHome = tempRoot.resolve("child");
    Files.createDirectories(grandHome);
    Files.createDirectories(parentHome);
    Files.createDirectories(childHome);
    writeMinimalConfig(grandHome, null);
    writeMinimalConfig(parentHome, "grand-proj");
    writeMinimalConfig(childHome, "parent-proj");

    registerProject(
        new ProjectConfig(
            "grand-proj", grandHome.toString(), ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME));
    registerProject(
        new ProjectConfig(
            "parent-proj", parentHome.toString(), ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME));
    ProjectConfig childConfig =
        new ProjectConfig(
            "child-proj", childHome.toString(), ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
    registerProject(childConfig);

    Project child = childConfig.loadProject(new Variables());
    IVariables variables = new Variables();
    child.modifyVariables(variables, childConfig, new ArrayList<>(), null);

    // Immediate parent only — not the grandparent
    assertEquals("parent-proj", variables.getVariable(ProjectsUtil.VARIABLE_PARENT_PROJECT_NAME));
    assertEquals(
        parentHome.toString(), variables.getVariable(ProjectsUtil.VARIABLE_PARENT_PROJECT_HOME));
    assertEquals("child-proj", variables.getVariable(Defaults.VARIABLE_HOP_PROJECT_NAME));
    assertEquals(childHome.toString(), variables.getVariable(ProjectsUtil.VARIABLE_PROJECT_HOME));
  }

  @Test
  public void testIsArchiveUri() {
    assertTrue(ProjectConfig.isArchiveUri("zip:file:///tmp/project.zip!/my-project"));
    assertTrue(ProjectConfig.isArchiveUri("zip:http://example.com/p.zip!/folder"));
    assertTrue(ProjectConfig.isArchiveUri("jar:file:///tmp/lib.jar!/META-INF"));
    assertTrue(ProjectConfig.isArchiveUri("tar:file:///tmp/a.tar!/proj"));
    assertTrue(ProjectConfig.isArchiveUri("tgz:file:///tmp/a.tgz!/proj"));
    assertTrue(ProjectConfig.isArchiveUri("tbz2:file:///tmp/a.tbz2!/proj"));
    assertTrue(ProjectConfig.isArchiveUri("jar:zip:outer.zip!/nested.jar!/dir"));
    assertTrue(ProjectConfig.isArchiveUri("tar:gz:http://host/mytar.tar.gz!/mytar.tar!/path"));
    assertTrue(ProjectConfig.isArchiveUri("  ZIP:/tmp/a.zip!/proj  "));

    assertFalse(ProjectConfig.isArchiveUri(null));
    assertFalse(ProjectConfig.isArchiveUri(""));
    assertFalse(ProjectConfig.isArchiveUri("/home/user/project"));
    assertFalse(ProjectConfig.isArchiveUri("file:///home/user/project"));
    assertFalse(ProjectConfig.isArchiveUri("http://example.com/project"));
    assertFalse(ProjectConfig.isArchiveUri("s3://bucket/project"));
    // A path ending in .zip without an archive scheme is not an archive URI
    assertFalse(ProjectConfig.isArchiveUri("/tmp/project.zip"));
  }

  @Test
  public void testReadOnlyFlagDefaultsAndSetters() {
    ProjectConfig config =
        new ProjectConfig("demo", "/tmp/demo", ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
    assertFalse(config.isReadOnly());
    config.setReadOnly(true);
    assertTrue(config.isReadOnly());
  }

  @Test
  public void testLoadProjectFromZipArchive() throws Exception {
    tempRoot = Files.createTempDirectory("hop-project-zip");
    Path zipFile = tempRoot.resolve("project.zip");

    String configJson =
        "{\n"
            + "  \"description\" : \"zipped project\",\n"
            + "  \"metadataBaseFolder\" : \"${PROJECT_HOME}/metadata\",\n"
            + "  \"unitTestsBasePath\" : \"${PROJECT_HOME}\",\n"
            + "  \"dataSetsCsvFolder\" : \"${PROJECT_HOME}/datasets\",\n"
            + "  \"enforcingExecutionInHome\" : true,\n"
            + "  \"parentProjectName\" : null,\n"
            + "  \"config\" : {\n"
            + "    \"variables\" : [ ]\n"
            + "  }\n"
            + "}\n";

    try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipFile))) {
      ZipEntry entry = new ZipEntry("my-project/" + ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
      zos.putNextEntry(entry);
      zos.write(configJson.getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    }

    String homeUri = "zip:file://" + zipFile.toAbsolutePath() + "!/my-project";
    ProjectConfig projectConfig =
        new ProjectConfig("zipped", homeUri, ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
    projectConfig.setReadOnly(true);
    registerProject(projectConfig);

    Project project = projectConfig.loadProject(new Variables());
    assertEquals("zipped project", project.getDescription());
    assertTrue(projectConfig.isReadOnly());
  }

  private void registerProject(ProjectConfig projectConfig) {
    ProjectsConfigSingleton.getConfig().addProjectConfig(projectConfig);
    registeredProjectNames.add(projectConfig.getProjectName());
  }

  private static void writeMinimalConfig(Path projectHome, String parentProjectName)
      throws Exception {
    String parentJson =
        parentProjectName == null ? "null" : "\"" + parentProjectName.replace("\"", "\\\"") + "\"";
    String json =
        "{\n"
            + "  \"metadataBaseFolder\" : \"${PROJECT_HOME}/metadata\",\n"
            + "  \"unitTestsBasePath\" : \"${PROJECT_HOME}\",\n"
            + "  \"dataSetsCsvFolder\" : \"${PROJECT_HOME}/datasets\",\n"
            + "  \"enforcingExecutionInHome\" : true,\n"
            + "  \"parentProjectName\" : "
            + parentJson
            + ",\n"
            + "  \"config\" : {\n"
            + "    \"variables\" : [ ]\n"
            + "  }\n"
            + "}\n";
    Files.writeString(
        projectHome.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME),
        json,
        StandardCharsets.UTF_8);
  }
}
