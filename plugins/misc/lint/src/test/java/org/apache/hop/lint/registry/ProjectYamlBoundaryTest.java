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

package org.apache.hop.lint.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.IHopLoggingEventListener;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.lint.CustomLintRule;
import org.apache.hop.lint.HopLinter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A hop-lint.yml above the project root is not the project's (#9387). It used to be found by
 * walking up to the root of the file system, so one left in a home or workspace folder changed the
 * findings of every project below it, and its folder became the root of exclude and suppress paths.
 */
class ProjectYamlBoundaryTest {

  @TempDir private Path workspace;
  private Path project;
  private File pipeline;

  @BeforeEach
  void createProject() throws Exception {
    project = workspace.resolve("samples");
    Files.createDirectories(project.resolve("transforms/zero-shot"));
    Files.writeString(project.resolve("project-config.json"), "{}\n");
    pipeline = project.resolve("transforms/zero-shot/openai.hpl").toFile();
    Files.writeString(pipeline.toPath(), "<pipeline/>\n");
  }

  @Test
  void aHopLintYmlAboveTheProjectIsNotUsed() throws Exception {
    Files.writeString(
        workspace.resolve("hop-lint.yml"), "rules:\n  SEC-002:\n    enabled: false\n");

    assertNull(RuleRegistry.getInstance().findProjectYaml(pipeline));
    assertTrue(ruleOf("SEC-002", pipeline).isEnabled(), "SEC-002 was switched off from outside");
  }

  @Test
  void theProjectRootHopLintYmlIsUsed() throws Exception {
    Files.writeString(workspace.resolve("hop-lint.yml"), "rules:\n  SEC-002:\n    enabled: true\n");
    Path projectYaml = project.resolve("hop-lint.yml");
    Files.writeString(projectYaml, "rules:\n  SEC-002:\n    enabled: false\n");

    assertEquals(projectYaml.toFile(), RuleRegistry.getInstance().findProjectYaml(pipeline));
    assertFalse(ruleOf("SEC-002", pipeline).isEnabled());
  }

  @Test
  void aHopLintYmlInAFolderOfTheProjectStillWins() throws Exception {
    Files.writeString(project.resolve("hop-lint.yml"), "rules: {}\n");
    Path nested = project.resolve("transforms/hop-lint.yml");
    Files.writeString(nested, "rules: {}\n");

    assertEquals(nested.toFile(), RuleRegistry.getInstance().findProjectYaml(pipeline));
  }

  @Test
  void aProjectNestedInAnotherProjectStopsAtItsOwnRoot() throws Exception {
    Files.writeString(workspace.resolve("project-config.json"), "{}\n");
    Files.writeString(workspace.resolve("hop-lint.yml"), "rules: {}\n");

    assertNull(RuleRegistry.getInstance().findProjectYaml(pipeline));
  }

  @Test
  void aFileInNoProjectStillUsesTheNearestHopLintYmlAboveIt() throws Exception {
    Path loose = workspace.resolve("loose/load.hpl");
    Files.createDirectories(loose.getParent());
    Files.writeString(loose, "<pipeline/>\n");
    Path workspaceYaml = workspace.resolve("hop-lint.yml");
    Files.writeString(workspaceYaml, "rules: {}\n");

    assertEquals(
        workspaceYaml.toFile(), RuleRegistry.getInstance().findProjectYaml(loose.toFile()));
  }

  @Test
  void excludePathsAreNotRootedAboveTheProject() throws Exception {
    // Relative to the workspace, which is how the file was excluded before.
    Files.writeString(
        workspace.resolve("hop-lint.yml"), "exclude:\n  - \"samples/transforms/**\"\n");
    HopLinter linter = new HopLinter();
    linter.loadConfigurationStrictly(pipeline);

    assertFalse(linter.isExcluded(pipeline.getAbsolutePath()));
  }

  @Test
  void excludePathsAreRelativeToTheProjectRoot() throws Exception {
    Files.writeString(project.resolve("hop-lint.yml"), "exclude:\n  - \"transforms/**\"\n");
    HopLinter linter = new HopLinter();
    linter.loadConfigurationStrictly(pipeline);

    assertTrue(linter.isExcluded(pipeline.getAbsolutePath()));
  }

  @Test
  void aHopProjectConfigFileMarksTheProjectRootToo() throws Exception {
    Files.delete(project.resolve("project-config.json"));
    Files.writeString(project.resolve("hop-project.config"), "{}\n");
    Files.writeString(workspace.resolve("hop-lint.yml"), "rules: {}\n");

    assertNull(RuleRegistry.getInstance().findProjectYaml(pipeline));
  }

  /** A project whose configuration file has a name of its own has no marker; its home stops it. */
  @Test
  void theProjectHomeIsTheRootOfAProjectWithoutAMarker() throws Exception {
    Files.delete(project.resolve("project-config.json"));
    Files.writeString(workspace.resolve("hop-lint.yml"), "rules: {}\n");

    assertNull(RuleRegistry.getInstance().findProjectYaml(pipeline, project.toString(), false));
    // Written with ".." it is the same folder.
    String throughParent = project.resolve("transforms/..").toString();
    assertNull(RuleRegistry.getInstance().findProjectYaml(pipeline, throughParent, false));
  }

  /** With a project in use the nearest hop-lint.yml still wins, as it does without one. */
  @Test
  void aHopLintYmlInAFolderOfTheProjectInUseWins() throws Exception {
    Files.writeString(project.resolve("hop-lint.yml"), "rules: {}\n");
    Path nested = project.resolve("transforms/hop-lint.yml");
    Files.writeString(nested, "rules: {}\n");

    assertEquals(
        nested.toFile(),
        RuleRegistry.getInstance().findProjectYaml(pipeline, project.toString(), false));
  }

  /**
   * hop-run, hop-server or a container started in a project's subfolder has that subfolder as its
   * working directory. That is no project root, and the project's own hop-lint.yml still applies.
   */
  @Test
  void theWorkingDirectoryIsNotAProjectRoot() throws Exception {
    Path projectYaml = project.resolve("hop-lint.yml");
    Files.writeString(projectYaml, "rules: {}\n");

    File found =
        withWorkingDirectory(
            project.resolve("transforms"),
            () -> RuleRegistry.getInstance().findProjectYaml(pipeline));

    assertEquals(projectYaml.toFile(), found);
  }

  /** A hop-lint.yml left in the folder Hop runs from, the installation, is no project's. */
  @Test
  void aHopLintYmlInTheWorkingDirectoryIsNotTheCurrentProjects() throws Exception {
    Path hopHome = workspace.resolve("hop");
    Files.createDirectories(hopHome);
    Files.writeString(hopHome.resolve("hop-lint.yml"), "rules:\n  SEC-002:\n    enabled: false\n");

    EffectiveRuleSet ruleSet =
        withWorkingDirectory(hopHome, () -> RuleRegistry.getInstance().resolveForCurrentProject());

    assertTrue(
        ruleSet.getRules().stream()
            .filter(rule -> "SEC-002".equals(rule.generateRuleId()))
            .findFirst()
            .orElseThrow()
            .isEnabled());
  }

  /** The parents of a relative path run out before the project root; it is made absolute first. */
  @Test
  void aRelativePathIsResolvedAgainstTheWorkingDirectory() throws Exception {
    Path projectYaml = project.resolve("hop-lint.yml");
    Files.writeString(projectYaml, "rules: {}\n");
    File relative = Path.of("").toAbsolutePath().relativize(pipeline.toPath()).toFile();

    assertEquals(projectYaml.toFile(), RuleRegistry.getInstance().findProjectYaml(relative));
  }

  private static <T> T withWorkingDirectory(Path directory, Supplier<T> action) {
    String previous = System.getProperty("user.dir");
    System.setProperty("user.dir", directory.toString());
    try {
      return action.get();
    } finally {
      System.setProperty("user.dir", previous);
    }
  }

  /**
   * A hop-lint.yml shared above several projects, at the root of a monorepo, stops applying. Hop
   * says so once, not for every file it lints.
   */
  @Test
  void aHopLintYmlAboveTheProjectIsReportedOnce() throws Exception {
    Path workspaceYaml = workspace.resolve("hop-lint.yml");
    Files.writeString(workspaceYaml, "rules: {}\n");
    File other = project.resolve("transforms/other.hpl").toFile();
    Files.writeString(other.toPath(), "<pipeline/>\n");

    List<String> logged =
        warningsAbout(
            workspaceYaml,
            () -> {
              RuleRegistry.getInstance().findProjectYaml(pipeline);
              RuleRegistry.getInstance().findProjectYaml(other);
              RuleRegistry.getInstance().findProjectYaml(pipeline);
            });

    assertEquals(1, logged.size(), logged.toString());
    assertTrue(logged.get(0).contains("--config"), logged.get(0));
  }

  @Test
  void theHopLintYmlOfAnEnclosingProjectIsNotReported() throws Exception {
    Files.writeString(workspace.resolve("project-config.json"), "{}\n");
    Path workspaceYaml = workspace.resolve("hop-lint.yml");
    Files.writeString(workspaceYaml, "rules: {}\n");

    List<String> logged =
        warningsAbout(workspaceYaml, () -> RuleRegistry.getInstance().findProjectYaml(pipeline));

    assertTrue(logged.isEmpty(), logged.toString());
  }

  /** Passed with --config, as the warning says, it applies the way it did before #9387. */
  @Test
  void aHopLintYmlAboveTheProjectPassedAsConfigRootsItsExcludePaths() throws Exception {
    Path workspaceYaml = workspace.resolve("hop-lint.yml");
    Files.writeString(workspaceYaml, "exclude:\n  - \"samples/transforms/**\"\n");
    HopLinter linter = new HopLinter();
    linter.loadConfig(workspaceYaml.toString());

    List<String> logged =
        warningsAbout(
            workspaceYaml, () -> assertTrue(linter.isExcluded(pipeline.getAbsolutePath())));

    assertTrue(logged.isEmpty(), "the file is in use: " + logged);
  }

  private static List<String> warningsAbout(Path hopLintYml, Runnable action) {
    HopLogStore.init();
    String path = hopLintYml.toFile().getAbsolutePath();
    List<String> logged = new ArrayList<>();
    IHopLoggingEventListener listener =
        event -> {
          if (event.getLevel() == LogLevel.MINIMAL
              && String.valueOf(event.getMessage()).contains(path)) {
            logged.add(String.valueOf(event.getMessage()));
          }
        };
    HopLogStore.getAppender().addLoggingEventListener(listener);
    try {
      action.run();
    } finally {
      HopLogStore.getAppender().removeLoggingEventListener(listener);
    }
    return logged;
  }

  private static CustomLintRule ruleOf(String ruleId, File context) {
    return RuleRegistry.getInstance().resolveForContext(context).getRules().stream()
        .filter(rule -> ruleId.equals(rule.generateRuleId()))
        .findFirst()
        .orElseThrow();
  }
}
