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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.lint.registry.RulePackInfo;
import org.apache.hop.lint.registry.RulePackOwner;
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
  void sarifIsValidAndDeclaresEachRuleOnce() throws Exception {
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
  void sarifPathsAreRelativeToTheLintTarget() throws Exception {
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
  void sarifMessageNamesTheTransform() throws Exception {
    String sarif =
        LintReportWriter.render(LintReportFormat.SARIF, sampleResults(), "1.0.0", PROJECT);
    JsonNode results = new ObjectMapper().readTree(sarif).get("runs").get(0).get("results");

    assertTrue(results.get(1).get("message").get("text").asText().startsWith("Orphan: "));
  }

  @Test
  void jsonCarriesSummaryAndFindings() throws Exception {
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
  void emptyRunStillProducesValidDocuments() throws Exception {
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
  void sarifWritesTagsDescriptionAndHelpUriOnTheRule() throws Exception {
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
  void sarifDescribesTheRuleFromAFindingThatCarriesItsDetails() throws Exception {
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

  /**
   * Hop's own verify remarks carry the transform they were reported on as their rule name. The rule
   * was described by the first one, so code-scanning tools labelled every finding with it.
   *
   * @see <a href="https://github.com/apache/hop/issues/9500">#9500</a>
   */
  @Test
  public void sarifNativeRuleIsNotDescribedByATransformName() throws Exception {
    JsonNode rules =
        sarifRules(
            List.of(
                nativeRemark("CONNECTION_NOT_ASSIGNED", "Load customers"),
                nativeRemark("CONNECTION_NOT_ASSIGNED", "Load orders")));

    assertEquals(1, rules.size());
    String text = rules.get(0).get("shortDescription").get("text").asText();
    assertEquals("CONNECTION_NOT_ASSIGNED", text);
  }

  /** A remark classified by a native rule is described by that rule's name. */
  @Test
  public void sarifNativeRuleTakesTheClassifyingRulesName() throws Exception {
    LintRuleDetails details =
        new LintRuleDetails("Hop Verify Remark", "Every remark Hop's checks produce", null, null);
    JsonNode rules =
        sarifRules(
            List.of(
                nativeRemark("HOP-CHECK", "Load customers").withRuleDetails(details),
                nativeRemark("HOP-CHECK", "Load orders").withRuleDetails(details)));

    assertEquals("Hop Verify Remark", rules.get(0).get("shortDescription").get("text").asText());
  }

  private static LintResult nativeRemark(String ruleId, String transformName) {
    return new LintResult(
        ruleId,
        transformName,
        "WARNING",
        "No connection is set",
        PROJECT.resolve("pipelines/load.hpl").toString(),
        LintSourceRef.transform(transformName),
        LintResult.Origin.HOP_NATIVE);
  }

  /** The SARIF schema requires a rule's tags to be unique. */
  @Test
  void sarifTagsAreUnique() throws Exception {
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
  void jsonWritesTheRuleTagsOnEachFinding() throws Exception {
    String json = LintReportWriter.render(LintReportFormat.JSON, taggedResults(), "1.0.0", PROJECT);
    JsonNode findings = new ObjectMapper().readTree(json).get("findings");

    JsonNode tags = findings.get(0).get("tags");
    assertEquals("secrets", tags.get("category").get(0).asText());
    assertEquals("SEC-POL-7", tags.get("policy").get(1).asText());
    assertEquals(0, findings.get(1).get("tags").size(), "an untagged rule writes an empty object");
  }

  @Test
  void formatParsingIsCaseInsensitiveAndRejectsUnknownValues() {
    assertEquals(LintReportFormat.SARIF, LintReportFormat.parse("SARIF"));
    assertEquals(LintReportFormat.JSON, LintReportFormat.parse(" json "));
    assertEquals(LintReportFormat.TEXT, LintReportFormat.parse(null));
    assertThrows(IllegalArgumentException.class, () -> LintReportFormat.parse("xml"));
  }

  // ------------------------------------------------------------------ rules and invocation

  private static CustomLintRule enabledRule(String id, String name, String severity) {
    CustomLintRule rule = new CustomLintRule();
    rule.setId(id);
    rule.setName(name);
    rule.setSeverity(severity);
    rule.setEnabled(true);
    return rule;
  }

  private static LintInvocation invocation(int exitCode) {
    Map<String, Object> configuration = new LinkedHashMap<>();
    configuration.put("configFile", "hop-lint.yml");
    configuration.put("failOn", "ERROR");
    configuration.put("maxWarnings", 5);
    return new LintInvocation(
        true,
        exitCode,
        List.of(
            enabledRule("DB-001", "Hardcoded Database Password", "ERROR"),
            enabledRule("SEC-002", "Hardcoded Secret in Transform", "ERROR"),
            enabledRule("TRANS-002", "Orphaned Transform", "WARNING")),
        List.of(
            new RulePackInfo(
                "hop-core", "Apache Hop Core Lint Rules", "2.21.0", RulePackOwner.APACHE),
            new RulePackInfo("acme", "Acme Rules", null, RulePackOwner.VENDOR)),
        configuration);
  }

  private static JsonNode sarifRun(List<LintResult> results, LintInvocation invocation)
      throws Exception {
    return new ObjectMapper()
        .readTree(
            LintReportWriter.render(LintReportFormat.SARIF, results, "1.0.0", PROJECT, invocation))
        .get("runs")
        .get(0);
  }

  /**
   * A rule that ran and found nothing was missing from the SARIF, so it read the same as one that
   * was switched off or not installed.
   *
   * @see <a href="https://github.com/apache/hop/issues/9500">#9500</a>
   */
  @Test
  void sarifDeclaresEveryEnabledRuleWithoutFindings() throws Exception {
    JsonNode run = sarifRun(List.of(), invocation(0));

    JsonNode rules = run.get("tool").get("driver").get("rules");
    assertEquals(3, rules.size());
    assertEquals("DB-001", rules.get(0).get("id").asText());
    assertEquals(
        "Hardcoded Database Password", rules.get(0).get("shortDescription").get("text").asText());
    assertEquals("warning", rules.get(2).get("defaultConfiguration").get("level").asText());
    assertEquals(0, run.get("results").size());
  }

  /** The findings point at the declared rules; an id only a finding carries comes after them. */
  @Test
  void sarifFindingsReferenceTheDeclaredRules() throws Exception {
    List<LintResult> results = new ArrayList<>(sampleResults());
    results.add(nativeRemark("CONNECTION_NOT_ASSIGNED", "Load customers"));

    JsonNode run = sarifRun(results, invocation(1));

    JsonNode rules = run.get("tool").get("driver").get("rules");
    assertEquals(4, rules.size());
    assertEquals("CONNECTION_NOT_ASSIGNED", rules.get(3).get("id").asText());
    JsonNode findings = run.get("results");
    assertEquals(0, findings.get(0).get("ruleIndex").asInt(), "DB-001");
    assertEquals(2, findings.get(1).get("ruleIndex").asInt(), "TRANS-002");
    assertEquals(3, findings.get(3).get("ruleIndex").asInt(), "CONNECTION_NOT_ASSIGNED");
  }

  @Test
  void sarifRecordsTheInvocation() throws Exception {
    JsonNode invocation = sarifRun(List.of(), invocation(1)).get("invocations").get(0);

    assertTrue(invocation.get("executionSuccessful").asBoolean());
    assertEquals(1, invocation.get("exitCode").asInt());
    JsonNode properties = invocation.get("properties");
    assertEquals("hop-lint.yml", properties.get("configFile").asText());
    assertEquals("ERROR", properties.get("failOn").asText());
    assertEquals(5, properties.get("maxWarnings").asInt());
    JsonNode packs = properties.get("rulePacks");
    assertEquals("hop-core", packs.get(0).get("id").asText());
    assertEquals("2.21.0", packs.get(0).get("version").asText());
    assertEquals("APACHE", packs.get(0).get("owner").asText());
    assertFalse(packs.get(1).has("version"), "a pack without a version gets none");
  }

  /**
   * A rule is declared from the run's rules when the run is passed, and from its findings when not.
   * Both described it, but from different sources: a WARNING rule that could not run, which reports
   * an ERROR, and a rule without a name came out differently, which tools comparing reports showed
   * as a change.
   */
  @Test
  void sarifDescribesARuleTheSameFromTheRunOrFromItsFindings() throws Exception {
    CustomLintRule warning = enabledRule("OPT-002", "PostgreSQL without TLS", "WARNING");
    CustomLintRule unnamed = enabledRule("OPT-003", "", "INFO");
    List<LintResult> results =
        List.of(
            new LintResult(
                    "OPT-002",
                    "PostgreSQL without TLS",
                    "ERROR",
                    "Rule execution failed: the URL of connection 'CRM' cannot be built",
                    PROJECT.resolve("metadata/rdbms/CRM.json").toString(),
                    LintSourceRef.metadata("CRM"),
                    LintResult.Origin.LINT)
                .withRuleDetails(LintRuleDetails.of(warning)),
            new LintResult(
                    "OPT-003",
                    "",
                    "INFO",
                    "No application name",
                    PROJECT.resolve("metadata/rdbms/CRM.json").toString(),
                    LintSourceRef.metadata("CRM"),
                    LintResult.Origin.LINT)
                .withRuleDetails(LintRuleDetails.of(unnamed)));
    LintInvocation run =
        new LintInvocation(true, 1, List.of(warning, unnamed), List.of(), Map.of());

    JsonNode fromTheRun = sarifRun(results, run).get("tool").get("driver").get("rules");
    JsonNode fromTheFindings = sarifRun(results, null).get("tool").get("driver").get("rules");

    assertEquals(fromTheRun, fromTheFindings);
    assertEquals("warning", fromTheRun.get(0).get("defaultConfiguration").get("level").asText());
    assertEquals("OPT-003", fromTheRun.get(1).get("shortDescription").get("text").asText());
  }

  /** Findings that do not come from one run, such as the GUI's, have no invocation to record. */
  @Test
  void sarifWithoutAnInvocationDeclaresTheFindingsRulesOnly() throws Exception {
    JsonNode run = sarifRun(sampleResults(), null);

    assertEquals(2, run.get("tool").get("driver").get("rules").size());
    assertFalse(run.has("invocations"));
  }
}
