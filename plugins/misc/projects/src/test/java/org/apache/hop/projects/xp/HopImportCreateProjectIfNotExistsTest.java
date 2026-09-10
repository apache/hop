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

package org.apache.hop.projects.xp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.projects.util.ProjectsUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HopImportCreateProjectIfNotExistsTest {

  private Path tempRoot;
  private ProjectConfig previousImportProject;

  @BeforeAll
  static void beforeAll() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    tempRoot = Files.createTempDirectory("hop-import-create-project");
    HopConfig.setInMemoryMode(true);
    previousImportProject =
        copyOf(
            ProjectsConfigSingleton.getConfig()
                .findProjectConfig(HopImportCreateProjectIfNotExists.IMPORT_PROJECT_NAME));
  }

  @AfterEach
  void tearDown() throws Exception {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    config.removeProjectConfig(HopImportCreateProjectIfNotExists.IMPORT_PROJECT_NAME);
    if (previousImportProject != null) {
      config.addProjectConfig(previousImportProject);
    }
    HopConfig.setInMemoryMode(false);
    if (tempRoot != null && Files.exists(tempRoot)) {
      try (Stream<Path> walk = Files.walk(tempRoot)) {
        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
      tempRoot = null;
    }
  }

  @Test
  void creatingAnImportProjectDoesNotChangeCallerProjectHome() throws Exception {
    Path originalHome = tempRoot.resolve("original-home");
    Path importHome = tempRoot.resolve("import-dest");
    Files.createDirectories(originalHome);
    Files.createDirectories(importHome);

    IVariables variables = new Variables();
    variables.setVariable(ProjectsUtil.VARIABLE_PROJECT_HOME, originalHome.toString());
    variables.setVariable(Defaults.VARIABLE_HOP_PROJECT_NAME, "original");
    variables.setVariable(ProjectsUtil.VARIABLE_PARENT_PROJECT_HOME, "/parent");
    variables.setVariable(Const.HOP_METADATA_FOLDER, originalHome + "/metadata");

    ProjectConfig created =
        HopImportCreateProjectIfNotExists.createImportProject(
            variables, importHome.toString(), false);

    assertEquals(
        originalHome.toString(), variables.getVariable(ProjectsUtil.VARIABLE_PROJECT_HOME));
    assertEquals("original", variables.getVariable(Defaults.VARIABLE_HOP_PROJECT_NAME));
    assertEquals("/parent", variables.getVariable(ProjectsUtil.VARIABLE_PARENT_PROJECT_HOME));
    assertEquals(originalHome + "/metadata", variables.getVariable(Const.HOP_METADATA_FOLDER));

    assertNotNull(created);
    assertEquals(HopImportCreateProjectIfNotExists.IMPORT_PROJECT_NAME, created.getProjectName());
    assertEquals(importHome.toString(), created.getProjectHome());
    assertTrue(
        Files.isRegularFile(importHome.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME)));
    assertNotNull(
        ProjectsConfigSingleton.getConfig()
            .findProjectConfig(HopImportCreateProjectIfNotExists.IMPORT_PROJECT_NAME));
  }

  @Test
  void emptyProjectPathIsANoOp() throws Exception {
    IVariables variables = new Variables();
    variables.setVariable(ProjectsUtil.VARIABLE_PROJECT_HOME, "/original-home");

    assertNull(HopImportCreateProjectIfNotExists.createImportProject(variables, "", false));
    assertNull(HopImportCreateProjectIfNotExists.createImportProject(variables, null, false));
    assertEquals("/original-home", variables.getVariable(ProjectsUtil.VARIABLE_PROJECT_HOME));
  }

  private static ProjectConfig copyOf(ProjectConfig source) {
    if (source == null) {
      return null;
    }
    ProjectConfig copy =
        new ProjectConfig(
            source.getProjectName(), source.getProjectHome(), source.getConfigFilename());
    copy.setReadOnly(source.isReadOnly());
    copy.setGroup(source.getGroup());
    copy.setTags(source.getTags());
    return copy;
  }
}
