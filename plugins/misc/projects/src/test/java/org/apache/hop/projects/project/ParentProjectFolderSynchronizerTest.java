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

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.util.ProjectsUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ParentProjectFolderSynchronizerTest {

  @TempDir Path tempDir;

  @Test
  void copyOnceIntoEmptyDestinationThenSkip() throws Exception {
    Path parent = tempDir.resolve("parent");
    Path child = tempDir.resolve("child");
    Files.createDirectories(parent.resolve("templates"));
    Files.writeString(parent.resolve("templates/core.hpl"), "v1");
    Files.createDirectories(child);
    Files.writeString(child.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME), "{}");

    Project project = projectWith(mapping("templates", true, false, false, null));

    ParentProjectFolderSynchronizer.synchronize(
        LogChannel.GENERAL, project, childConfig(child), variables(parent, child));

    Path copied = child.resolve("templates/core.hpl");
    assertEquals("v1", Files.readString(copied));

    Files.writeString(parent.resolve("templates/core.hpl"), "v2");
    Files.writeString(parent.resolve("templates/extra.hpl"), "extra");

    ParentProjectFolderSynchronizer.synchronize(
        LogChannel.GENERAL, project, childConfig(child), variables(parent, child));

    assertEquals("v1", Files.readString(copied));
    assertFalse(Files.exists(child.resolve("templates/extra.hpl")));
  }

  @Test
  void copyOnEnablePicksUpNewParentFiles() throws Exception {
    Path parent = tempDir.resolve("parent");
    Path child = tempDir.resolve("child");
    Files.createDirectories(parent.resolve("templates"));
    Files.writeString(parent.resolve("templates/core.hpl"), "v1");
    Files.createDirectories(child);

    Project project = projectWith(mapping("templates", false, true, false, null));

    ParentProjectFolderSynchronizer.synchronize(
        LogChannel.GENERAL, project, childConfig(child), variables(parent, child));
    assertEquals("v1", Files.readString(child.resolve("templates/core.hpl")));

    Files.writeString(parent.resolve("templates/extra.hpl"), "extra");
    ParentProjectFolderSynchronizer.synchronize(
        LogChannel.GENERAL, project, childConfig(child), variables(parent, child));
    assertEquals("extra", Files.readString(child.resolve("templates/extra.hpl")));
  }

  @Test
  void overwriteFalseKeepsExistingFile() throws Exception {
    Path parent = tempDir.resolve("parent");
    Path child = tempDir.resolve("child");
    Files.createDirectories(parent.resolve("templates"));
    Files.writeString(parent.resolve("templates/core.hpl"), "parent");
    Files.createDirectories(child.resolve("templates"));
    Files.writeString(child.resolve("templates/core.hpl"), "child");

    Project project = projectWith(mapping("templates", false, true, false, null));

    ParentProjectFolderSynchronizer.synchronize(
        LogChannel.GENERAL, project, childConfig(child), variables(parent, child));

    assertEquals("child", Files.readString(child.resolve("templates/core.hpl")));
  }

  @Test
  void overwriteTrueReplacesExistingFile() throws Exception {
    Path parent = tempDir.resolve("parent");
    Path child = tempDir.resolve("child");
    Files.createDirectories(parent.resolve("templates"));
    Files.writeString(parent.resolve("templates/core.hpl"), "parent");
    Files.createDirectories(child.resolve("templates"));
    Files.writeString(child.resolve("templates/core.hpl"), "child");

    Project project = projectWith(mapping("templates", false, true, true, null));

    ParentProjectFolderSynchronizer.synchronize(
        LogChannel.GENERAL, project, childConfig(child), variables(parent, child));

    assertEquals("parent", Files.readString(child.resolve("templates/core.hpl")));
  }

  @Test
  void exclusionRegexSkipsBasenameAndRelativePath() throws Exception {
    Path parent = tempDir.resolve("parent");
    Path child = tempDir.resolve("child");
    Files.createDirectories(parent.resolve("templates/secret"));
    Files.writeString(parent.resolve("templates/keep.hpl"), "keep");
    Files.writeString(parent.resolve("templates/skip.tmp"), "tmp");
    Files.writeString(parent.resolve("templates/secret/hidden.txt"), "secret");
    Files.createDirectories(child);

    Project project = projectWith(mapping("templates", false, true, true, ".*\\.tmp|secret/.*"));

    ParentProjectFolderSynchronizer.synchronize(
        LogChannel.GENERAL, project, childConfig(child), variables(parent, child));

    assertEquals("keep", Files.readString(child.resolve("templates/keep.hpl")));
    assertFalse(Files.exists(child.resolve("templates/skip.tmp")));
    assertFalse(Files.exists(child.resolve("templates/secret/hidden.txt")));
  }

  @Test
  void neverOverwritesChildProjectConfigOrGit() throws Exception {
    Path parent = tempDir.resolve("parent");
    Path child = tempDir.resolve("child");
    Files.createDirectories(parent.resolve(".git"));
    Files.writeString(parent.resolve(".git/config"), "parent-git");
    Files.writeString(
        parent.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME), "parent-config");
    Files.writeString(parent.resolve("readme.txt"), "hello");
    Files.createDirectories(child.resolve(".git"));
    Files.writeString(child.resolve(".git/config"), "child-git");
    Files.writeString(
        child.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME), "child-config");

    Project project = projectWith(mapping(".", false, true, true, null));

    ParentProjectFolderSynchronizer.synchronize(
        LogChannel.GENERAL, project, childConfig(child), variables(parent, child));

    assertEquals("hello", Files.readString(child.resolve("readme.txt")));
    assertEquals(
        "child-config",
        Files.readString(child.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME)));
    assertEquals("child-git", Files.readString(child.resolve(".git/config")));
  }

  @Test
  void pathTraversalIsRejected() throws Exception {
    Path parent = tempDir.resolve("parent");
    Path child = tempDir.resolve("child");
    Path escape = tempDir.resolve("escape");
    Files.createDirectories(parent);
    Files.createDirectories(child);
    Files.createDirectories(escape);
    Files.writeString(parent.resolve("safe.txt"), "parent");
    Files.writeString(escape.resolve("marker.txt"), "untouched");

    Project project = projectWith(mapping("../escape", false, true, true, null));

    ParentProjectFolderSynchronizer.synchronize(
        LogChannel.GENERAL, project, childConfig(child), variables(parent, child));

    assertEquals("untouched", Files.readString(escape.resolve("marker.txt")));
    assertFalse(Files.exists(escape.resolve("safe.txt")));
  }

  @Test
  void invalidExclusionRegexSkipsMapping() throws Exception {
    Path parent = tempDir.resolve("parent");
    Path child = tempDir.resolve("child");
    Files.createDirectories(parent.resolve("templates"));
    Files.writeString(parent.resolve("templates/core.hpl"), "v1");
    Files.createDirectories(child);

    Project project = projectWith(mapping("templates", false, true, true, "*.tmp"));

    ParentProjectFolderSynchronizer.synchronize(
        LogChannel.GENERAL, project, childConfig(child), variables(parent, child));

    assertFalse(Files.exists(child.resolve("templates/core.hpl")));
  }

  @Test
  void readOnlyProjectIsNotCopiedInto() throws Exception {
    Path parent = tempDir.resolve("parent");
    Path child = tempDir.resolve("child");
    Files.createDirectories(parent.resolve("templates"));
    Files.writeString(parent.resolve("templates/core.hpl"), "v1");
    Files.createDirectories(child);

    Project project = projectWith(mapping("templates", false, true, true, null));
    ProjectConfig config = childConfig(child);
    config.setReadOnly(true);

    ParentProjectFolderSynchronizer.synchronize(
        LogChannel.GENERAL, project, config, variables(parent, child));

    assertFalse(Files.exists(child.resolve("templates/core.hpl")));
  }

  @Test
  void copyOnceOfParentRootIgnoresExistingProjectConfig() throws Exception {
    Path parent = tempDir.resolve("parent");
    Path child = tempDir.resolve("child");
    Files.createDirectories(parent.resolve("pipelines"));
    Files.writeString(parent.resolve("pipelines/start.hpl"), "start");
    Files.createDirectories(child);
    Files.writeString(child.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME), "{}");

    Project project = projectWith(mapping(".", true, false, false, null));

    ParentProjectFolderSynchronizer.synchronize(
        LogChannel.GENERAL, project, childConfig(child), variables(parent, child));

    assertEquals("start", Files.readString(child.resolve("pipelines/start.hpl")));
  }

  @Test
  void matchesExclusionUsesBasenameAndRelativePath() {
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(".*\\.tmp|secret/.*");
    assertTrue(ParentProjectFolderSynchronizer.matchesExclusion(pattern, "skip.tmp", "skip.tmp"));
    assertTrue(
        ParentProjectFolderSynchronizer.matchesExclusion(
            pattern, "hidden.txt", "secret/hidden.txt"));
    assertFalse(ParentProjectFolderSynchronizer.matchesExclusion(pattern, "keep.hpl", "keep.hpl"));
  }

  private static Project projectWith(ParentProjectFolder mapping) {
    Project project = new Project();
    project.setParentProjectName("parent");
    project.getParentProjectFolders().add(mapping);
    return project;
  }

  private static ParentProjectFolder mapping(
      String folder, boolean copyOnce, boolean copyOnEnable, boolean overwrite, String exclusion) {
    ParentProjectFolder mapping = new ParentProjectFolder();
    mapping.setFolder(folder);
    mapping.setCopyOnce(copyOnce);
    mapping.setCopyOnEnable(copyOnEnable);
    mapping.setOverwrite(overwrite);
    mapping.setExclusionWildcard(exclusion);
    return mapping;
  }

  private static ProjectConfig childConfig(Path child) {
    return new ProjectConfig(
        "child", child.toString(), ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
  }

  private static IVariables variables(Path parent, Path child) {
    IVariables variables = new Variables();
    variables.setVariable(ProjectsUtil.VARIABLE_PROJECT_HOME, child.toString());
    variables.setVariable(ProjectsUtil.VARIABLE_PARENT_PROJECT_HOME, parent.toString());
    return variables;
  }
}
