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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The machine-readable reports are a contract with CI, so their shape is pinned here. */
public class LintReportWriterTest {

  private static final Path PROJECT = Paths.get("/projects/sales").toAbsolutePath();

  private static List<LintResult> sampleResults() {
    return List.of(
        new LintResult(
            "DB-001",
            "Hardcoded Database Password",
            "ERROR",
            "Use a variable",
            PROJECT.resolve("metadata/rdbms/SALES.json").toString(),
            LintSourceRef.metadata("SALES"),
            LintResult.Origin.LINT),
        new LintResult(
            "TRANS-002",
            "Orphaned Transform",
            "WARNING",
            "Never executes",
            PROJECT.resolve("pipelines/load.hpl").toString(),
            LintSourceRef.transform("Orphan"),
            LintResult.Origin.LINT),
        new LintResult(
            "TRANS-002",
            "Orphaned Transform",
            "WARNING",
            "Never executes",
            PROJECT.resolve("pipelines/other.hpl").toString(),
            LintSourceRef.transform("Loner"),
            LintResult.Origin.LINT));
  }

  @Test
  public void sarifIsValidAndDeclaresEachRuleOnce() throws Exception {
    String sarif =
        LintReportWriter.render(LintReportFormat.SARIF, sampleResults(), "1.2.3", PROJECT);
    JsonNode root = new ObjectMapper().readTree(sarif);

    assertEquals("2.1.0", root.get("version").asText());
    JsonNode run = root.get("runs").get(0);
    assertEquals("1.2.3", run.get("tool").get("driver").get("version").asText());

    // Two distinct rule ids across three findings.
    assertEquals(2, run.get("tool").get("driver").get("rules").size());
    assertEquals(3, run.get("results").size());

    assertEquals("error", run.get("results").get(0).get("level").asText());
    assertEquals("warning", run.get("results").get(1).get("level").asText());
  }

  /** CI annotates a diff by path, so absolute build-machine paths have to be relativised. */
  @Test
  public void sarifPathsAreRelativeToTheLintTarget() throws Exception {
    String sarif =
        LintReportWriter.render(LintReportFormat.SARIF, sampleResults(), "1.0.0", PROJECT);
    JsonNode results = new ObjectMapper().readTree(sarif).get("runs").get(0).get("results");

    String uri =
        results
            .get(0)
            .get("locations")
            .get(0)
            .get("physicalLocation")
            .get("artifactLocation")
            .get("uri")
            .asText();

    assertEquals("metadata/rdbms/SALES.json", uri);
    assertFalse(sarif.contains(PROJECT.toString()), "absolute paths leaked into the report");
  }

  /**
   * A lint rule has no line number to report, so every finding in a file would otherwise be
   * indistinguishable on a pull request. The transform or action name carries that information.
   */
  @Test
  public void sarifMessageNamesTheTransform() throws Exception {
    String sarif =
        LintReportWriter.render(LintReportFormat.SARIF, sampleResults(), "1.0.0", PROJECT);
    JsonNode results = new ObjectMapper().readTree(sarif).get("runs").get(0).get("results");

    assertTrue(results.get(1).get("message").get("text").asText().startsWith("Orphan: "));
  }

  @Test
  public void jsonCarriesSummaryAndFindings() throws Exception {
    String json = LintReportWriter.render(LintReportFormat.JSON, sampleResults(), "1.0.0", PROJECT);
    JsonNode root = new ObjectMapper().readTree(json);

    assertEquals(3, root.get("summary").get("total").asInt());
    assertEquals(1, root.get("summary").get("errors").asInt());
    assertEquals(2, root.get("summary").get("warnings").asInt());
    assertEquals(3, root.get("findings").size());
    assertEquals("DB-001", root.get("findings").get(0).get("ruleId").asText());
  }

  /** An empty run still has to produce a parseable document, or the CI step breaks on success. */
  @Test
  public void emptyRunStillProducesValidDocuments() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    JsonNode sarif =
        mapper.readTree(
            LintReportWriter.render(LintReportFormat.SARIF, List.of(), "1.0.0", PROJECT));
    assertEquals(0, sarif.get("runs").get(0).get("results").size());

    JsonNode json =
        mapper.readTree(
            LintReportWriter.render(LintReportFormat.JSON, List.of(), "1.0.0", PROJECT));
    assertEquals(0, json.get("summary").get("total").asInt());
  }

  @Test
  public void formatParsingIsCaseInsensitiveAndRejectsUnknownValues() {
    assertEquals(LintReportFormat.SARIF, LintReportFormat.parse("SARIF"));
    assertEquals(LintReportFormat.JSON, LintReportFormat.parse(" json "));
    assertEquals(LintReportFormat.TEXT, LintReportFormat.parse(null));
    assertThrows(IllegalArgumentException.class, () -> LintReportFormat.parse("xml"));
  }
}
