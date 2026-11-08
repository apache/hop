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

  private static CustomLintRule ruleOf(String ruleId, File context) {
    return RuleRegistry.getInstance().resolveForContext(context).getRules().stream()
        .filter(rule -> ruleId.equals(rule.generateRuleId()))
        .findFirst()
        .orElseThrow();
  }
}
