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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PathVariableReplacerTest {

  @TempDir Path tempDir;

  private Path projectHome;
  private Path sourceFiles;
  private Variables variables;

  @BeforeEach
  void setUp() throws Exception {
    projectHome = tempDir.resolve("project");
    sourceFiles = tempDir.resolve("source");
    Files.createDirectories(projectHome.resolve("data"));
    Files.createDirectories(sourceFiles.resolve("input"));
    Files.writeString(projectHome.resolve("file.csv"), "a");
    Files.writeString(sourceFiles.resolve("input").resolve("data.csv"), "b");

    variables = new Variables();
    // Avoid loading full system property set into candidates for most tests:
    // set only the variables we care about without initializeFrom().
    variables.setVariable(ProjectsUtil.VARIABLE_PROJECT_HOME, projectHome.toString());
    variables.setVariable("SOURCE_FILES", sourceFiles.toString());
  }

  @Test
  void replacesProjectHomeChildPath() {
    String selected = projectHome.resolve("file.csv").toString();
    assertEquals(
        "${PROJECT_HOME}/file.csv",
        PathVariableReplacer.replacePathWithVariable(variables, selected));
  }

  @Test
  void replacesProjectHomeNestedPath() {
    String selected = projectHome.resolve("data").resolve("x.txt").toString();
    // create nested file path (dir already exists)
    assertEquals(
        "${PROJECT_HOME}/data/x.txt",
        PathVariableReplacer.replacePathWithVariable(variables, selected));
  }

  @Test
  void replacesEnvironmentVariableOutsideProject() {
    String selected = sourceFiles.resolve("input").resolve("data.csv").toString();
    assertEquals(
        "${SOURCE_FILES}/input/data.csv",
        PathVariableReplacer.replacePathWithVariable(variables, selected));
  }

  @Test
  void longestPrefixWinsOverProjectHome() throws Exception {
    Path nested = projectHome.resolve("data");
    variables.setVariable("DATA_FOLDER", nested.toString());
    String selected = nested.resolve("report.csv").toString();
    assertEquals(
        "${DATA_FOLDER}/report.csv",
        PathVariableReplacer.replacePathWithVariable(variables, selected));
  }

  @Test
  void exactDirectoryMatchUsesVariableOnly() {
    assertEquals(
        "${PROJECT_HOME}",
        PathVariableReplacer.replacePathWithVariable(variables, projectHome.toString()));
    assertEquals(
        "${SOURCE_FILES}",
        PathVariableReplacer.replacePathWithVariable(variables, sourceFiles.toString()));
  }

  @Test
  void noMatchLeavesPathUnchanged() {
    Path outside = tempDir.resolve("other").resolve("file.txt");
    String selected = outside.toString();
    assertEquals(selected, PathVariableReplacer.replacePathWithVariable(variables, selected));
  }

  @Test
  void nullOrEmptyInputsUnchanged() {
    assertEquals("/tmp/x", PathVariableReplacer.replacePathWithVariable(null, "/tmp/x"));
    assertEquals(null, PathVariableReplacer.replacePathWithVariable(variables, null));
    assertEquals("", PathVariableReplacer.replacePathWithVariable(variables, ""));
  }

  @Test
  void ignoresSystemPropertiesAsCandidates() {
    Variables withSystem = new Variables();
    withSystem.setVariable(ProjectsUtil.VARIABLE_PROJECT_HOME, projectHome.toString());
    withSystem.setVariable("java.io.tmpdir", tempDir.toString());
    withSystem.setVariable("user.home", tempDir.toString());

    // Path under tempDir (where java.io.tmpdir points) but not under project home
    Path other = tempDir.resolve("not-in-project");
    String selected = other.toString();
    assertEquals(selected, PathVariableReplacer.replacePathWithVariable(withSystem, selected));
  }

  @Test
  void ignoresNonPathManagedNames() {
    assertFalse(PathVariableReplacer.isCandidateVariableName(Defaults.VARIABLE_HOP_PROJECT_NAME));
    assertFalse(
        PathVariableReplacer.isCandidateVariableName(Defaults.VARIABLE_HOP_ENVIRONMENT_NAME));
    assertFalse(
        PathVariableReplacer.isCandidateVariableName(ProjectsUtil.VARIABLE_PARENT_PROJECT_NAME));
    assertFalse(
        PathVariableReplacer.isCandidateVariableName(
            Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY));
    assertTrue(PathVariableReplacer.isCandidateVariableName(ProjectsUtil.VARIABLE_PROJECT_HOME));
    assertTrue(PathVariableReplacer.isCandidateVariableName("SOURCE_FILES"));
  }

  @Test
  void ignoresNonPathValues() {
    assertFalse(PathVariableReplacer.isCandidatePathValue(""));
    assertFalse(PathVariableReplacer.isCandidatePathValue("localhost"));
    assertFalse(PathVariableReplacer.isCandidatePathValue("${PROJECT_HOME}/data"));
    assertFalse(PathVariableReplacer.isCandidatePathValue("/meta1,/meta2"));
    assertTrue(PathVariableReplacer.isCandidatePathValue("/tmp/source"));
    assertTrue(PathVariableReplacer.isCandidatePathValue("file:///tmp/source"));
    assertTrue(PathVariableReplacer.isCandidatePathValue("C:\\data\\files"));
  }

  @Test
  void resolvesNestedVariableValuesBeforeMatching() throws Exception {
    Path dataDir = projectHome.resolve("data");
    variables.setVariable("DATA_FOLDER", "${PROJECT_HOME}/data");
    String selected = dataDir.resolve("report.csv").toString();
    assertEquals(
        "${DATA_FOLDER}/report.csv",
        PathVariableReplacer.replacePathWithVariable(variables, selected));
  }

  @Test
  void trailingSlashOnVariableValueStillMatches() {
    variables.setVariable("SOURCE_FILES", sourceFiles.toString() + "/");
    String selected = sourceFiles.resolve("input").resolve("data.csv").toString();
    assertEquals(
        "${SOURCE_FILES}/input/data.csv",
        PathVariableReplacer.replacePathWithVariable(variables, selected));
  }
}
