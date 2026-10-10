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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lint.registry.LintConfigurationException;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * The Run Linter action, and that it agrees with {@code hop lint}.
 *
 * @see <a href="https://github.com/apache/hop/issues/8826">#8826</a>
 */
public class ActionRunLinterTest {

  @TempDir private Path dir;

  private Path project;

  @BeforeAll
  static void initHop() throws Exception {
    // Pipelines are only read properly with the transform plugins registered.
    HopEnvironment.init();
  }

  /**
   * A project with one error (a pipeline that cannot be read) and two warnings (two pipelines that
   * nothing calls, with STRUCT-004 switched on).
   */
  @BeforeEach
  void createProject() throws Exception {
    project = dir.resolve("project");
    Files.createDirectories(project.resolve("pipelines"));
    Files.writeString(
        project.resolve("hop-lint.yml"), "rules:\n  STRUCT-004:\n    enabled: true\n");
    writePipeline(project.resolve("pipelines/customers.hpl"));
    writePipeline(project.resolve("pipelines/orders.hpl"));
    Files.writeString(project.resolve("pipelines/broken.hpl"), "this is not a pipeline");
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

  private ActionRunLinter action(Path target) {
    ActionRunLinter action = new ActionRunLinter("Run Linter");
    action.setTarget(target.toString());
    action.setMetadataProvider(new MemoryMetadataProvider());
    return action;
  }

  /** Runs hop lint; returns the exit code, with stderr in {@code err}. */
  private static int hopLint(StringBuilder err, String... args) {
    ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
    PrintStream systemOut = System.out;
    PrintStream systemErr = System.err;
    int exitCode;
    try {
      System.setOut(new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
      System.setErr(new PrintStream(errBytes, true, StandardCharsets.UTF_8));
      exitCode = new CommandLine(new LintCommand()).execute(args);
    } finally {
      System.setOut(systemOut);
      System.setErr(systemErr);
    }
    err.append(errBytes.toString(StandardCharsets.UTF_8));
    return exitCode;
  }

  private static List<String> column(Result result, String name) throws Exception {
    List<String> values = new ArrayList<>();
    for (RowMetaAndData row : result.getRows()) {
      values.add(row.getString(name, null));
    }
    return values;
  }

  // ------------------------------------------------------------------ the same as hop lint

  @Test
  public void givesTheSameFindingsAndOutcomeAsHopLint() throws Exception {
    Path cliReport = dir.resolve("cli.json");
    int exitCode =
        hopLint(new StringBuilder(), "-f", "JSON", "-o", cliReport.toString(), project.toString());

    Path actionReport = dir.resolve("action.json");
    ActionRunLinter action = action(project);
    action.setReportFile(actionReport.toString());
    action.setReportFormat(LintReportFormat.JSON);
    Result result = action.execute(new Result(), 0);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode cli = mapper.readTree(cliReport.toFile());
    JsonNode fromAction = mapper.readTree(actionReport.toFile());
    assertEquals(cli, fromAction);
    assertEquals(1, cli.get("summary").get("errors").asInt(), cli.toString());
    assertEquals(2, cli.get("summary").get("warnings").asInt(), cli.toString());

    assertEquals(1, exitCode);
    assertFalse(result.getResult(), "hop lint exits 1, so the action fails");
    assertEquals(0, result.getNrErrors(), "failing on findings is not an error in the action");
  }

  /**
   * A target reached through "..", as PROJECT_HOME is in the integration tests, gave absolute paths
   * in the report and the baseline of hop lint, so neither matched the action's.
   */
  @Test
  public void aTargetWithDotDotGivesRelativePaths() throws Exception {
    String target = project.resolve("pipelines").resolve("..").toString();
    Path cliReport = dir.resolve("cli.json");
    hopLint(new StringBuilder(), "-f", "JSON", "-o", cliReport.toString(), target);
    Path baseline = dir.resolve("baseline.json");
    hopLint(new StringBuilder(), "--write-baseline", baseline.toString(), target);

    for (JsonNode finding : new ObjectMapper().readTree(cliReport.toFile()).get("findings")) {
      String file = finding.get("file").asText();
      assertFalse(file.startsWith("/") || file.contains(".."), file);
    }
    String accepted = Files.readString(baseline);
    assertFalse(accepted.contains(project.toString()), accepted);

    // The action accepts every finding from that baseline.
    ActionRunLinter action = action(project);
    action.setBaselineFile(baseline.toString());
    action.setFailOn(LintSeverity.FailOn.WARNING);
    assertTrue(action.execute(new Result(), 0).getResult());
  }

  @Test
  public void passesWhereHopLintPasses() {
    int exitCode = hopLint(new StringBuilder(), "--fail-on", "NONE", project.toString());

    ActionRunLinter action = action(project);
    action.setFailOn(LintSeverity.FailOn.NONE);
    Result result = action.execute(new Result(), 0);

    assertEquals(0, exitCode);
    assertTrue(result.getResult());
  }

  @Test
  public void maxWarningsFailsAsInHopLint() {
    ActionRunLinter action = action(project);
    action.setFailOn(LintSeverity.FailOn.NONE);

    action.setMaxWarnings("1");
    assertFalse(action.execute(new Result(), 0).getResult());
    assertEquals(
        1,
        hopLint(
            new StringBuilder(), "--fail-on", "NONE", "--max-warnings", "1", project.toString()));

    action.setMaxWarnings("2");
    assertTrue(action.execute(new Result(), 0).getResult());
    assertEquals(
        0,
        hopLint(
            new StringBuilder(), "--fail-on", "NONE", "--max-warnings", "2", project.toString()));
  }

  @Test
  public void aBaselineHidesAcceptedFindingsAsInHopLint() throws Exception {
    Path baseline = dir.resolve("baseline.json");
    assertEquals(
        0,
        hopLint(new StringBuilder(), "--write-baseline", baseline.toString(), project.toString()));

    ActionRunLinter action = action(project);
    action.setBaselineFile(baseline.toString());
    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult(), "every finding is accepted");
    assertTrue(result.getRows().isEmpty(), result.getRows().toString());
    assertEquals("0", action.getVariable(ActionRunLinter.VAR_ERRORS));
  }

  @Test
  public void aMissingBaselineFailsTheAction() {
    ActionRunLinter action = action(project);
    action.setBaselineFile(dir.resolve("missing.json").toString());

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
  }

  // ------------------------------------------------------------------ results

  @Test
  public void oneRowPerFindingAndTheCountsAsVariables() throws Exception {
    Path report = dir.resolve("report.sarif");
    ActionRunLinter action = action(project);
    action.setReportFile(report.toString());

    Result result = action.execute(new Result(), 0);

    assertEquals(3, result.getRows().size(), result.getRows().toString());
    assertEquals(
        List.of("ERROR", "WARNING", "WARNING"),
        column(result, "severity").stream().sorted().toList());
    assertTrue(
        column(result, "rule_id").contains("STRUCT-004"), column(result, "rule_id").toString());
    assertEquals("1", action.getVariable(ActionRunLinter.VAR_ERRORS));
    assertEquals("2", action.getVariable(ActionRunLinter.VAR_WARNINGS));
    assertEquals("0", action.getVariable(ActionRunLinter.VAR_INFOS));
    assertEquals(report.toString(), action.getVariable(ActionRunLinter.VAR_REPORT_FILE));

    // SARIF by default, and in the result files for a Mail action to attach.
    JsonNode sarif = new ObjectMapper().readTree(report.toFile());
    assertEquals("2.1.0", sarif.get("version").asText(), sarif.toString());
    assertEquals(1, result.getResultFilesList().size());
    assertTrue(
        result.getResultFilesList().get(0).getFile().getName().getPath().endsWith("report.sarif"));
  }

  /** The minimum severity narrows the rows and the report, never the outcome or the counts. */
  @Test
  public void theMinimumSeverityNarrowsTheRowsOnly() throws Exception {
    ActionRunLinter action = action(project);
    action.setMinimumSeverity(LintSeverity.Level.ERROR);

    Result result = action.execute(new Result(), 0);

    assertEquals(List.of("ERROR"), column(result, "severity"));
    assertEquals("2", action.getVariable(ActionRunLinter.VAR_WARNINGS));
    assertFalse(result.getResult());
  }

  @Test
  public void withoutAReportTheReportVariableIsEmpty() {
    ActionRunLinter action = action(project);

    action.execute(new Result(), 0);

    assertEquals("", action.getVariable(ActionRunLinter.VAR_REPORT_FILE));
  }

  @Test
  public void metadataIsLeftOutWhenAskedTo() throws Exception {
    Files.createDirectories(project.resolve("metadata/rdbms"));
    Files.writeString(project.resolve("metadata/rdbms/broken.json"), "{ not json");
    ActionRunLinter action = action(project);
    action.setFailOn(LintSeverity.FailOn.NONE);

    action.execute(new Result(), 0);
    assertEquals("2", action.getVariable(ActionRunLinter.VAR_ERRORS), "the metadata file too");

    action.setIncludeMetadata(false);
    action.execute(new Result(), 0);
    assertEquals("1", action.getVariable(ActionRunLinter.VAR_ERRORS));

    // hop lint agrees, by default and with the option off, whatever the pre-commit setting says.
    assertEquals(2, cliErrors("--fail-on", "NONE", project.toString()));
    assertEquals(1, cliErrors("--fail-on", "NONE", "--no-include-metadata", project.toString()));
  }

  /** A report under folders that do not exist yet creates them. */
  @Test
  public void theReportFolderIsCreated() throws Exception {
    Path report = dir.resolve("output").resolve("lint").resolve("report.sarif");
    ActionRunLinter action = action(project);
    action.setFailOn(LintSeverity.FailOn.NONE);
    action.setReportFile(report.toString());

    Result result = action.execute(new Result(), 0);

    assertEquals(0, result.getNrErrors());
    assertTrue(result.getResult());
    assertTrue(Files.exists(report), report.toString());
  }

  /** The errors hop lint reports, read from its JSON report. */
  private int cliErrors(String... args) throws Exception {
    Path report = dir.resolve("cli-errors.json");
    List<String> all = new ArrayList<>(List.of("-f", "JSON", "-o", report.toString()));
    all.addAll(List.of(args));
    hopLint(new StringBuilder(), all.toArray(String[]::new));
    return new ObjectMapper().readTree(report.toFile()).get("summary").get("errors").asInt();
  }

  /**
   * After a run that fails with an error, the next action must not read an earlier run's counts.
   */
  @Test
  public void anErrorClearsTheVariablesOfAnEarlierRun() throws Exception {
    ActionRunLinter action = action(project);
    action.setReportFile(dir.resolve("report.sarif").toString());
    action.execute(new Result(), 0);
    assertEquals("1", action.getVariable(ActionRunLinter.VAR_ERRORS));

    action.setTarget(dir.resolve("no-such-folder").toString());
    Result result = action.execute(new Result(), 0);

    assertEquals(1, result.getNrErrors());
    for (String name :
        List.of(
            ActionRunLinter.VAR_ERRORS,
            ActionRunLinter.VAR_WARNINGS,
            ActionRunLinter.VAR_INFOS,
            ActionRunLinter.VAR_REPORT_FILE)) {
      assertEquals("", action.getVariable(name), name);
    }
  }

  /**
   * Switching the linter off is for Hop Gui. An explicit run that checked nothing would pass a
   * project full of errors.
   */
  @Test
  public void anExplicitRunLintsWhenTheLinterIsSwitchedOff() throws Exception {
    LinterConfigPlugin config = LinterConfigPlugin.getInstance();
    config.setLinterEnabled(false);
    config.saveToHopConfig();
    try {
      ActionRunLinter action = action(project);
      Result result = action.execute(new Result(), 0);
      assertFalse(result.getResult());
      assertEquals("1", action.getVariable(ActionRunLinter.VAR_ERRORS));

      assertEquals(1, cliErrors("--fail-on", "NONE", project.toString()));
    } finally {
      config = LinterConfigPlugin.getInstance();
      config.setLinterEnabled(true);
      config.saveToHopConfig();
    }
  }

  // ------------------------------------------------------------------ the target

  @Test
  public void anEmptyTargetLintsTheCurrentProject() {
    ActionRunLinter action = action(project);
    action.setTarget("");
    action.setVariable("PROJECT_HOME", project.toString());

    action.execute(new Result(), 0);

    assertEquals("1", action.getVariable(ActionRunLinter.VAR_ERRORS));
  }

  @Test
  public void anEmptyTargetOutsideAProjectFails() {
    ActionRunLinter action = action(project);
    action.setTarget("");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  public void aTargetThatIsNotLocalFails() {
    ActionRunLinter action = action(project);
    action.setTarget("ram:///project");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
  }

  // ------------------------------------------------------------------ configuration

  /** hop lint --config on a folder was dropped: the run reloaded the project's own hop-lint.yml. */
  @Test
  public void aConfigFileReplacesTheProjectsOnAFolder() throws Exception {
    Path config = dir.resolve("other-lint.yml");
    Files.writeString(config, "rules:\n  STRUCT-004:\n    enabled: false\n");
    ActionRunLinter action = action(project);
    action.setConfigFile(config.toString());

    action.execute(new Result(), 0);
    assertEquals("0", action.getVariable(ActionRunLinter.VAR_WARNINGS));

    StringBuilder err = new StringBuilder();
    Path cliReport = dir.resolve("cli.json");
    hopLint(
        err, "-c", config.toString(), "-f", "JSON", "-o", cliReport.toString(), project.toString());
    JsonNode cli = new ObjectMapper().readTree(cliReport.toFile());
    assertEquals(0, cli.get("summary").get("warnings").asInt(), cli + "\n" + err);
  }

  /**
   * A broken hop-lint.yml used to give the default rules on a folder, and a pass. It fails, with
   * the parser's message, in the action and in hop lint alike.
   */
  @Test
  public void aBrokenProjectConfigurationFailsWithItsOwnMessage() throws Exception {
    Path clean = dir.resolve("clean");
    Files.createDirectories(clean);
    writePipeline(clean.resolve("only.hpl"));
    Files.writeString(clean.resolve("hop-lint.yml"), "rules: [unterminated\n");

    ActionRunLinter action = action(clean);
    action.setFailOn(LintSeverity.FailOn.NONE);
    Result result = action.execute(new Result(), 0);
    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());

    StringBuilder err = new StringBuilder();
    assertEquals(1, hopLint(err, "--fail-on", "NONE", clean.toString()));
    assertTrue(err.toString().contains("Invalid lint configuration in"), err.toString());

    // The git hook is as strict: it used to check the commit against the default rules.
    Path staged = dir.resolve("staged.txt");
    Files.writeString(staged, clean.resolve("only.hpl") + "\n");
    StringBuilder hookErr = new StringBuilder();
    assertEquals(
        1,
        hopLint(hookErr, "--pre-commit", "--staged-file", staged.toString()),
        hookErr.toString());
    assertTrue(hookErr.toString().contains("Invalid lint configuration in"), hookErr.toString());

    LintRun run = new LintRun();
    run.setTarget(clean.toFile());
    LintConfigurationException e =
        assertThrows(
            LintConfigurationException.class,
            () -> run.execute(new MemoryMetadataProvider(), Variables.getADefaultVariableSpace()));
    assertTrue(e.getMessage().contains(clean.resolve("hop-lint.yml").toString()), e.getMessage());
    assertTrue(e.getMessage().contains("line"), "the parser says where: " + e.getMessage());
  }
}
