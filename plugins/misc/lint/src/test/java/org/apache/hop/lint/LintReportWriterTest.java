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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

  private static List<LintResult> taggedResults() {
    Map<String, List<String>> tags = new LinkedHashMap<>();
    tags.put("category", List.of("secrets"));
    tags.put("policy", List.of("SEC-POL-4", "SEC-POL-7"));
    LintRuleDetails details =
        new LintRuleDetails(
            "Passwords come from a variable", "https://example.com/rules/SEC-002", tags);
    return List.of(
        new LintResult(
                "SEC-002",
                "Hardcoded Secret",
                "ERROR",
                "Use a variable",
                PROJECT.resolve("pipelines/load.hpl").toString(),
                LintSourceRef.transform("REST"),
                LintResult.Origin.LINT)
            .withRuleDetails(details),
        sampleResults().get(1));
  }

  /** GitHub code scanning and Azure DevOps read a flat list of key:value strings. */
  @Test
  public void sarifWritesTagsDescriptionAndHelpUriOnTheRule() throws Exception {
    String sarif =
        LintReportWriter.render(LintReportFormat.SARIF, taggedResults(), "1.0.0", PROJECT);
    JsonNode rules =
        new ObjectMapper()
            .readTree(sarif)
            .get("runs")
            .get(0)
            .get("tool")
            .get("driver")
            .get("rules");

    JsonNode tagged = rules.get(0);
    assertEquals(
        "Passwords come from a variable", tagged.get("fullDescription").get("text").asText());
    assertEquals("https://example.com/rules/SEC-002", tagged.get("helpUri").asText());

    JsonNode flat = tagged.get("properties").get("tags");
    assertEquals(3, flat.size());
    assertEquals("category:secrets", flat.get(0).asText());
    assertEquals("policy:SEC-POL-4", flat.get(1).asText());
    assertEquals("policy:SEC-POL-7", flat.get(2).asText());

    JsonNode hopTags = tagged.get("properties").get("hopTags");
    assertEquals("secrets", hopTags.get("category").get(0).asText());
    assertEquals(2, hopTags.get("policy").size());

    // A rule without tags, description or help link gets none of those keys.
    JsonNode plain = rules.get(1);
    assertFalse(plain.has("properties"));
    assertFalse(plain.has("fullDescription"));
    assertFalse(plain.has("helpUri"));
  }

  private static JsonNode sarifRules(List<LintResult> results) throws Exception {
    return new ObjectMapper()
        .readTree(LintReportWriter.render(LintReportFormat.SARIF, results, "1.0.0", PROJECT))
        .get("runs")
        .get(0)
        .get("tool")
        .get("driver")
        .get("rules");
  }

  /**
   * The rule is described by a finding that carries its details, even when one without them came
   * first, so the order of the results cannot hide the tags.
   */
  @Test
  public void sarifDescribesTheRuleFromAFindingThatCarriesItsDetails() throws Exception {
    LintResult tagged = taggedResults().get(0);
    LintResult bare =
        new LintResult(
            "SEC-002",
            "Hardcoded Secret",
            "ERROR",
            "Use a variable",
            PROJECT.resolve("pipelines/other.hpl").toString());

    JsonNode rules = sarifRules(List.of(bare, tagged));

    assertEquals(1, rules.size());
    assertEquals("category:secrets", rules.get(0).get("properties").get("tags").get(0).asText());
    JsonNode results =
        new ObjectMapper()
            .readTree(
                LintReportWriter.render(
                    LintReportFormat.SARIF, List.of(bare, tagged), "1.0.0", PROJECT))
            .get("runs")
            .get(0)
            .get("results");
    assertEquals(0, results.get(0).get("ruleIndex").asInt());
    assertEquals(0, results.get(1).get("ruleIndex").asInt());
  }

  /** The SARIF schema requires a rule's tags to be unique. */
  @Test
  public void sarifTagsAreUnique() throws Exception {
    LintResult finding =
        sampleResults()
            .get(0)
            .withRuleDetails(
                new LintRuleDetails("", null, Map.of("policy", List.of("SEC-POL-4", "SEC-POL-4"))));

    JsonNode flat = sarifRules(List.of(finding)).get(0).get("properties").get("tags");

    assertEquals(1, flat.size());
    assertEquals("policy:SEC-POL-4", flat.get(0).asText());
  }

  @Test
  public void jsonWritesTheRuleTagsOnEachFinding() throws Exception {
    String json = LintReportWriter.render(LintReportFormat.JSON, taggedResults(), "1.0.0", PROJECT);
    JsonNode findings = new ObjectMapper().readTree(json).get("findings");

    JsonNode tags = findings.get(0).get("tags");
    assertEquals("secrets", tags.get("category").get(0).asText());
    assertEquals("SEC-POL-7", tags.get("policy").get(1).asText());
    assertEquals(0, findings.get(1).get("tags").size(), "an untagged rule writes an empty object");
  }

  @Test
  public void formatParsingIsCaseInsensitiveAndRejectsUnknownValues() {
    assertEquals(LintReportFormat.SARIF, LintReportFormat.parse("SARIF"));
    assertEquals(LintReportFormat.JSON, LintReportFormat.parse(" json "));
    assertEquals(LintReportFormat.TEXT, LintReportFormat.parse(null));
    assertThrows(IllegalArgumentException.class, () -> LintReportFormat.parse("xml"));
  }
}
