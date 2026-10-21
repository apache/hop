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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * {@code hop lint} paths, thresholds, system findings and the staged files the git hook passes.
 *
 * @see <a href="https://github.com/apache/hop/issues/9071">#9071</a>
 */
public class LintCommandOptionsTest {

  @TempDir private Path dir;

  private Path project;

  private String originDir;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  /** A project with a pipeline that cannot be read, which SYSTEM-001 reports. */
  @BeforeEach
  void createProject() throws Exception {
    project = dir.resolve("project");
    Files.createDirectories(project);
    writePipeline(project.resolve("customers.hpl"), "Customers");
    Files.writeString(project.resolve("broken.hpl"), "this is not a pipeline");
    originDir = System.getProperty("hop.origin.dir");
  }

  @AfterEach
  void restoreOrigin() {
    if (originDir == null) {
      System.clearProperty("hop.origin.dir");
    } else {
      System.setProperty("hop.origin.dir", originDir);
    }
    CustomRuleExecutor.setProjectIndex(null);
  }

  private static void writePipeline(Path file, String description) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName(file.getFileName().toString().replace(".hpl", ""));
    pipelineMeta.setDescription(description);
    Files.writeString(file, pipelineMeta.getXml(Variables.getADefaultVariableSpace()));
  }

  /** Runs hop lint; returns the exit code, with stdout and stderr in {@code output}. */
  private static int hopLint(StringBuilder output, String... args) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    PrintStream capture = new PrintStream(bytes, true, StandardCharsets.UTF_8);
    PrintStream systemOut = System.out;
    PrintStream systemErr = System.err;
    int exitCode;
    try {
      System.setOut(capture);
      System.setErr(capture);
      exitCode = new CommandLine(new LintCommand()).execute(args);
    } finally {
      System.setOut(systemOut);
      System.setErr(systemErr);
    }
    output.append(bytes.toString(StandardCharsets.UTF_8));
    return exitCode;
  }

  private static JsonNode report(Path file) throws Exception {
    return new ObjectMapper().readTree(Files.readString(file));
  }

  // ------------------------------------------------------------------ relative paths

  /**
   * The launcher runs Java from the Hop installation, so {@code hop lint .} linted the installation
   * and {@code -o lint.json} wrote the report there.
   */
  @Test
  public void relativePathsAreResolvedAgainstWhereHopLintWasRun() throws Exception {
    System.setProperty("hop.origin.dir", project.toString());

    StringBuilder output = new StringBuilder();
    int status = hopLint(output, "-f", "JSON", "-o", "lint.json", "--fail-on", "NONE", ".");

    assertEquals(0, status, output.toString());
    Path written = project.resolve("lint.json");
    assertTrue(Files.isRegularFile(written), "the report belongs in the project: " + output);
    assertEquals(1, report(written).get("summary").get("total").asInt(), output.toString());
  }

  @Test
  public void aRelativeFileTargetIsFound() throws Exception {
    System.setProperty("hop.origin.dir", project.toString());

    StringBuilder output = new StringBuilder();
    int status = hopLint(output, "customers.hpl");

    assertEquals(0, status, output.toString());
    assertFalse(output.toString().contains("does not exist"), output.toString());
  }

  @Test
  public void baselineFilesAreRelativeToo() throws Exception {
    System.setProperty("hop.origin.dir", project.toString());

    StringBuilder output = new StringBuilder();
    assertEquals(0, hopLint(output, "--write-baseline", "baseline.json", "."), output.toString());
    assertTrue(Files.isRegularFile(project.resolve("baseline.json")), output.toString());
    assertEquals(0, hopLint(output, "--baseline", "baseline.json", "."), output.toString());
  }

  // ------------------------------------------------------------------ --fail-on

  /** A typo used to read as ERROR, and a project with warnings passed. */
  @Test
  public void anUnknownFailOnIsRefused() {
    StringBuilder output = new StringBuilder();

    int status = hopLint(output, "--fail-on", "WARNNG", project.toString());

    assertEquals(1, status, output.toString());
    assertTrue(output.toString().contains("Unknown fail-on severity 'WARNNG'"), output.toString());
  }

  /** --fail-on INFO used to fail on errors only. SYSTEM-001 is lowered to INFO to have one. */
  @Test
  public void failOnInfoFailsOnAnInfoFinding() throws Exception {
    Files.writeString(
        project.resolve("hop-lint.yml"), "rules:\n  SYSTEM-001:\n    severity: INFO\n");

    StringBuilder output = new StringBuilder();
    assertEquals(0, hopLint(output, "--fail-on", "WARNING", project.toString()), output.toString());
    assertEquals(1, hopLint(output, "--fail-on", "INFO", project.toString()), output.toString());
  }

  // ------------------------------------------------------------------ system findings

  @Test
  public void aSystemFindingTakesTheProjectsSeverity() throws Exception {
    Files.writeString(
        project.resolve("hop-lint.yml"), "rules:\n  SYSTEM-001:\n    severity: WARNING\n");

    StringBuilder output = new StringBuilder();
    int status =
        hopLint(output, "-f", "JSON", "-o", dir.resolve("out.json").toString(), project.toString());

    assertEquals(0, status, "a warning does not fail on ERROR: " + output);
    JsonNode finding = report(dir.resolve("out.json")).get("findings").get(0);
    assertEquals("SYSTEM-001", finding.get("ruleId").asText(), finding.toString());
    assertEquals("WARNING", finding.get("severity").asText(), finding.toString());
    assertFalse(output.toString().contains("no installed rule pack defines"), output.toString());
  }

  @Test
  public void aSystemRuleCanBeSwitchedOff() throws Exception {
    Files.writeString(
        project.resolve("hop-lint.yml"), "rules:\n  SYSTEM-001:\n    enabled: false\n");

    StringBuilder output = new StringBuilder();

    assertEquals(0, hopLint(output, project.toString()), output.toString());
  }

  @Test
  public void systemRulesAreListed() {
    StringBuilder output = new StringBuilder();

    assertEquals(0, hopLint(output, "--list-rules"), output.toString());
    for (String ruleId : new String[] {"SYSTEM-001", "SYSTEM-002", "SYSTEM-003"}) {
      assertTrue(output.toString().contains(ruleId), ruleId + " is not listed: " + output);
    }
  }

  // ------------------------------------------------------------------ the staged version

  /**
   * The staged pipeline has no description, the one on disk has. LOCAL-001 asks for one, so only
   * linting the staged copy blocks the commit.
   */
  @Test
  public void thePreCommitCheckLintsTheStagedCopy() throws Exception {
    Files.delete(project.resolve("broken.hpl"));
    Files.writeString(
        project.resolve("hop-lint.yml"),
        """
        rules:
          LOCAL-001:
            type: custom
            enabled: true
            severity: ERROR
            target: PIPELINE
            targetField: description
            condition: NOT_EMPTY
            name: Pipeline Description
        """);
    Path staged = dir.resolve("staged-copy");
    writePipeline(staged, "");
    Path list = dir.resolve("staged.txt");

    Files.writeString(list, project.resolve("customers.hpl") + "\t" + staged + "\n");
    StringBuilder output = new StringBuilder();
    assertEquals(
        1, hopLint(output, "--pre-commit", "--staged-file", list.toString()), output.toString());
    assertTrue(output.toString().contains("customers.hpl"), "reported as the file: " + output);
    assertFalse(output.toString().contains("staged-copy"), output.toString());

    // Without a staged copy the file on disk, with its description, is what is linted.
    Files.writeString(list, project.resolve("customers.hpl") + "\n");
    StringBuilder disk = new StringBuilder();
    assertEquals(
        0, hopLint(disk, "--pre-commit", "--staged-file", list.toString()), disk.toString());
  }
}
