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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.hop.lint.registry.ProjectYamlOverlay;
import org.apache.hop.lint.registry.YamlRulePackParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exclusions and suppressions: what a project chooses not to check, and not to report. */
public class LintPolicyTest {

  private static final Path ROOT = Paths.get("/projects/sales").toAbsolutePath();

  private LintResult finding(String ruleId, String relativePath, String sourceName) {
    return new LintResult(
        ruleId,
        ruleId,
        "ERROR",
        "message",
        ROOT.resolve(relativePath).toString(),
        sourceName == null ? null : LintSourceRef.transform(sourceName),
        LintResult.Origin.LINT);
  }

  @Test
  public void globExcludesMatchProjectRelativePaths() {
    LintPolicy policy = new LintPolicy(List.of("generated/**", "tests/**"), List.of());

    assertTrue(policy.isExcluded(ROOT.resolve("generated/a.hpl").toString(), ROOT));
    assertTrue(policy.isExcluded(ROOT.resolve("tests/unit/b.hpl").toString(), ROOT));
    assertFalse(policy.isExcluded(ROOT.resolve("pipelines/c.hpl").toString(), ROOT));
  }

  /** A bare folder name is what people write first; treating it as a no-op would be a trap. */
  @Test
  public void bareFolderNameExcludesTheFolder() {
    LintPolicy policy = new LintPolicy(List.of("generated"), List.of());

    assertTrue(policy.isExcluded(ROOT.resolve("generated/a.hpl").toString(), ROOT));
    assertFalse(policy.isExcluded(ROOT.resolve("generated-reports/a.hpl").toString(), ROOT));
  }

  @Test
  public void suppressionNarrowsByPathAndSource() {
    LintPolicy policy =
        new LintPolicy(
            List.of(),
            List.of(
                new LintPolicy.Suppression("DB-001", "legacy/**", null, "pinned"),
                new LintPolicy.Suppression("TRANS-002", null, "Reserved", "deliberate")));

    List<LintResult> results =
        List.of(
            finding("DB-001", "legacy/old.json", null),
            finding("DB-001", "current/new.json", null),
            finding("TRANS-002", "pipelines/a.hpl", "Reserved"),
            finding("TRANS-002", "pipelines/a.hpl", "Something else"));

    List<LintResult> kept = policy.applySuppressions(results, ROOT);

    assertEquals(2, kept.size());
    assertEquals("current/new.json", relative(kept.get(0)));
    assertEquals("Something else", kept.get(1).getSource().getName());
  }

  @Test
  public void emptyPolicyChangesNothing() {
    List<LintResult> results = List.of(finding("DB-001", "a.json", null));

    assertEquals(results, LintPolicy.empty().applySuppressions(results, ROOT));
    assertFalse(LintPolicy.empty().isExcluded(ROOT.resolve("a.hpl").toString(), ROOT));
  }

  /**
   * A suppression with no rule id would silence everything, which is disabling the linter under
   * another name; one with no reason is a decision nobody can review later. Both are refused when
   * the configuration is read, rather than quietly applied.
   */
  @Test
  public void malformedSuppressionsAreRefused(@TempDir Path dir) throws Exception {
    File yaml = dir.resolve("hop-lint.yml").toFile();
    Files.writeString(
        yaml.toPath(),
        """
        suppress:
          - rule: NO-REASON-001
          - reason: "silence everything"
          - rule: GOOD-001
            reason: "Reviewed and accepted"
        """,
        StandardCharsets.UTF_8);

    ProjectYamlOverlay overlay = YamlRulePackParser.parseProjectYaml(yaml);
    List<LintPolicy.Suppression> suppressions = overlay.getPolicy().getSuppressions();

    assertEquals(1, suppressions.size());
    assertEquals("GOOD-001", suppressions.get(0).getRuleId());
    assertEquals("Reviewed and accepted", suppressions.get(0).getReason());
  }

  @Test
  public void excludeAndSuppressAreReadFromProjectYaml(@TempDir Path dir) throws Exception {
    File yaml = dir.resolve("hop-lint.yml").toFile();
    Files.writeString(
        yaml.toPath(),
        """
        exclude:
          - "generated/**"

        suppress:
          - rule: ACME-ENV-003
            path: "metadata/**"
            reason: "Legacy name, pinned until the migration"

        rules:
          TRANS-002:
            enabled: false
        """,
        StandardCharsets.UTF_8);

    ProjectYamlOverlay overlay = YamlRulePackParser.parseProjectYaml(yaml);

    assertEquals(List.of("generated/**"), overlay.getPolicy().getExcludes());
    assertEquals(1, overlay.getPolicy().getSuppressions().size());
    // The rules section still parses alongside them.
    assertTrue(overlay.getOverlays().containsKey("TRANS-002"));
  }

  /** A project file carrying only exclusions still has to yield a usable policy. */
  @Test
  public void policyIsReadEvenWithoutARulesSection(@TempDir Path dir) throws Exception {
    File yaml = dir.resolve("hop-lint.yml").toFile();
    Files.writeString(yaml.toPath(), "exclude:\n  - \"tests/**\"\n", StandardCharsets.UTF_8);

    ProjectYamlOverlay overlay = YamlRulePackParser.parseProjectYaml(yaml);

    assertEquals(List.of("tests/**"), overlay.getPolicy().getExcludes());
  }

  private String relative(LintResult result) {
    return LintPolicy.relativise(result.getFileName(), ROOT);
  }
}
