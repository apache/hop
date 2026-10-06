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
package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Linting in the context of a project.
 *
 * <p>{@code hop lint} ignored the project and its environment, so variables they define were
 * unresolved. Lint Project and Lint Selected Folder in Hop Gui left out the rules that need the
 * whole project, STRUCT-004 and STRUCT-005, which the CLI reported.
 *
 * @see <a href="https://github.com/apache/hop/issues/8732">#8732</a>
 */
public class ProjectContextTest {

  @TempDir private Path dir;

  private Path project;

  @BeforeEach
  void createProject() throws Exception {
    project = dir.resolve("project");
    Files.createDirectories(project.resolve("pipelines/load"));
    Files.writeString(
        project.resolve("hop-lint.yml"), "rules:\n  STRUCT-004:\n    enabled: true\n");
    writePipeline(project.resolve("pipelines/load/customers.hpl"));
    writePipeline(project.resolve("pipelines/report.hpl"));
  }

  @AfterEach
  void clearIndex() {
    CustomRuleExecutor.setProjectIndex(null);
  }

  private static void writePipeline(Path file) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName(file.getFileName().toString().replace(".hpl", ""));
    Files.writeString(file, pipelineMeta.getXml(Variables.getADefaultVariableSpace()));
  }

  private IVariables projectVariables() {
    IVariables variables = Variables.getADefaultVariableSpace();
    variables.setVariable("PROJECT_HOME", project.toString());
    return variables;
  }

  private static long count(List<LintResult> results, String ruleId) {
    return results.stream().filter(r -> ruleId.equals(r.getRuleId())).count();
  }

  /** What Lint Project in Hop Gui does: run() with no index of its own. */
  @Test
  public void aProjectLintReportsUnreferencedPipelines() {
    List<LintResult> results =
        new HopLinter()
            .run(project.toString(), new MemoryMetadataProvider(), projectVariables(), null);

    assertEquals(2, count(results, "STRUCT-004"), results.toString());
    assertFalse(CustomRuleExecutor.hasProjectIndex(), "the run leaves no index behind");
  }

  /** A folder is indexed against the whole project, not only itself. */
  @Test
  public void aFolderInAProjectIsIndexedAgainstTheProject() {
    LintProjectIndex index =
        new HopLinter()
            .buildProjectIndex(
                project.resolve("pipelines/load").toString(), null, projectVariables());

    assertTrue(
        index.getIndexedFiles().stream().anyMatch(f -> f.endsWith("pipelines/report.hpl")),
        index.getIndexedFiles().toString());
  }

  @Test
  public void aFolderOutsideAProjectIsIndexedOnItsOwn() {
    LintProjectIndex index =
        new HopLinter()
            .buildProjectIndex(
                project.resolve("pipelines/load").toString(),
                null,
                Variables.getADefaultVariableSpace());

    assertEquals(1, index.getIndexedFiles().size(), index.getIndexedFiles().toString());
  }

  @Test
  public void aSingleFileOutsideAProjectHasNoIndex() {
    assertNull(
        new HopLinter()
            .buildProjectIndex(
                project.resolve("pipelines/report.hpl").toString(),
                null,
                Variables.getADefaultVariableSpace()));
  }

  // ------------------------------------------------------------------ hop lint -j / -e

  /**
   * Stands in for the projects plugin's -j / -e. It enables its project whether or not -j is given,
   * as the real one enables the default project of hop-config.json.
   */
  public static class FakeProjectOptions implements IConfigOptions {
    @CommandLine.Option(
        names = {"-j", "--project"},
        description = "The project")
    private String projectName;

    private final String projectHome;
    private final MultiMetadataProvider projectMetadata;

    FakeProjectOptions(String projectHome, MultiMetadataProvider projectMetadata) {
      this.projectHome = projectHome;
      this.projectMetadata = projectMetadata;
    }

    @Override
    public boolean handleOption(
        ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables) {
      variables.setVariable("PROJECT_HOME", projectHome);
      hasHopMetadataProvider.setMetadataProvider(projectMetadata);
      return true;
    }
  }

  /** Runs hop lint with the stand-in project; returns stdout and stderr. */
  private String[] lint(String projectHome, String... args) throws Exception {
    LintCommand command = new LintCommand();
    CommandLine commandLine = new CommandLine(command);
    command.initialize(
        commandLine,
        Variables.getADefaultVariableSpace(),
        new MultiMetadataProvider(Variables.getADefaultVariableSpace()));
    MultiMetadataProvider projectMetadata =
        new MultiMetadataProvider(Variables.getADefaultVariableSpace());
    commandLine.addMixin("project", new FakeProjectOptions(projectHome, projectMetadata));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    PrintStream systemOut = System.out;
    PrintStream systemErr = System.err;
    try {
      System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
      System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
      commandLine.execute(args);
    } finally {
      System.setOut(systemOut);
      System.setErr(systemErr);
    }
    return new String[] {
      out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8)
    };
  }

  @Test
  public void theProjectsMetadataIsUsed() throws Exception {
    String[] output = lint(project.toString(), "-v", project.resolve("pipelines").toString());

    assertTrue(output[0].contains("Using the metadata of project home"), output[0]);
    assertFalse(output[1].contains("outside the project"), output[1]);
  }

  /** Another project, with its own metadata folder, outside the default one. */
  private Path otherProject() throws Exception {
    Path other = dir.resolve("other");
    Files.createDirectories(other.resolve("metadata/rdbms"));
    writePipeline(other.resolve("orders.hpl"));
    return other;
  }

  /**
   * A stock hop-config.json has a default project. Applied to a project it does not contain, it
   * replaced that project's own metadata and reported its connections as missing.
   */
  @Test
  public void aDefaultProjectDoesNotApplyToAnotherProject() throws Exception {
    Path other = otherProject();

    String[] output = lint(project.toString(), "-v", other.toString());

    assertTrue(
        output[0].contains("Using metadata folder: " + other.resolve("metadata")), output[0]);
    assertFalse(output[0].contains("Using the metadata of project home"), output[0]);
    assertFalse(output[1].contains("outside the project"), output[1]);
  }

  @Test
  public void theHookUsesTheMetadataOfTheProjectItCommitsTo() throws Exception {
    Path other = otherProject();
    Path staged = dir.resolve("staged.txt");
    Files.writeString(staged, other.resolve("orders.hpl") + "\n");

    String[] output =
        lint(project.toString(), "-v", "--pre-commit", "--staged-file", staged.toString());

    assertTrue(
        output[0].contains("Using metadata folder: " + other.resolve("metadata")), output[0]);
    assertFalse(output[0].contains("Using the metadata of project home"), output[0]);
  }

  /** A project asked for with -j applies wherever the target is, with a warning. */
  @Test
  public void aChosenProjectAppliesOutsideItsHomeWithAWarning() throws Exception {
    Path other = otherProject();

    String[] output = lint(project.toString(), "-v", "-j", "default", other.toString());

    assertTrue(output[0].contains("Using the metadata of project home"), output[0]);
    assertTrue(output[1].contains("is outside the project in use"), output[1]);
  }
}
