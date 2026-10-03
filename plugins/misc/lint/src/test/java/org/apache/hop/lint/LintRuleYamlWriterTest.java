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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.lint.registry.YamlRulePackParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Saving one rule from the rule manager.
 *
 * <p>The rule manager used to write the whole of hop-lint.yml from its list of rules. Toggling one
 * rule deleted the exclude and suppress sections with their reasons, every comment and the rule
 * order, so excluded files were linted again and accepted findings came back without a word.
 *
 * @see <a href="https://github.com/apache/hop/issues/8734">#8734</a>
 */
public class LintRuleYamlWriterTest {

  private static final String PROJECT_FILE =
      """
      # Lint policy for the CRM project, reviewed 2026-09-14
      pack:
        id: crm

      rules:
        # Too noisy on the generated pipelines
        DOC-001:
          enabled: false

        SEC-002:
          severity: WARNING  # until the vault migration is done

        CRM-001:
          type: custom
          enabled: true
          severity: ERROR
          target: PIPELINE
          targetField: name
          condition: MATCHES_PATTERN
          conditionValue: '^crm_.*'
          name: CRM pipeline names
          description: Pipelines are named crm_<subject>

      exclude:
        # Metadata injection templates
        - "templates/**"

      suppress:
        - rule: "SEC-002"
          path: "load/http.hpl"
          source: "HTTP client"
          reason: "Test endpoint without credentials"
      """;

  @TempDir private Path dir;

  private Path yaml() {
    return dir.resolve("hop-lint.yml");
  }

  private static Map<String, Object> entry(Object... keysAndValues) {
    Map<String, Object> entry = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      entry.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return entry;
  }

  @Test
  public void changingOneRuleLeavesEveryOtherLineAlone() throws Exception {
    Files.writeString(yaml(), PROJECT_FILE, StandardCharsets.UTF_8);

    LintPolicyYamlWriter.putRule(yaml(), "DOC-001", entry("enabled", true));

    String after = Files.readString(yaml(), StandardCharsets.UTF_8);
    assertEquals(
        PROJECT_FILE.replace("DOC-001:\n    enabled: false", "DOC-001:\n    enabled: true"), after);

    LintPolicy policy = YamlRulePackParser.parseProjectYaml(yaml().toFile()).getPolicy();
    assertEquals(List.of("templates/**"), policy.getExcludes());
    assertEquals(1, policy.getSuppressions().size());
  }

  @Test
  public void aRuleWithAnInlineCommentIsReplacedWhole() throws Exception {
    String after =
        LintPolicyYamlWriter.replaceRule(PROJECT_FILE, "SEC-002", entry("severity", "INFO"));

    assertEquals(
        PROJECT_FILE.replace(
            "severity: WARNING  # until the vault migration is done", "severity: INFO"),
        after);
  }

  @Test
  public void theCommentAboveTheNextRuleStaysWithIt() throws Exception {
    String file =
        """
        rules:
          DOC-001:
            enabled: false
          # Too noisy on the generated pipelines
          DOC-002:
            enabled: false
        """;

    String after = LintPolicyYamlWriter.replaceRule(file, "DOC-001", null);

    assertEquals(
        """
        rules:
          # Too noisy on the generated pipelines
          DOC-002:
            enabled: false
        """,
        after);
  }

  @Test
  public void aNewRuleIsAddedAtTheEndOfTheRules() throws Exception {
    String after =
        LintPolicyYamlWriter.replaceRule(PROJECT_FILE, "STRUCT-001", entry("conditionValue", "40"));

    assertTrue(
        after.contains("    description: Pipelines are named crm_<subject>\n  STRUCT-001:\n"),
        after);
    assertTrue(after.contains("  STRUCT-001:\n    conditionValue: '40'\n"), after);
  }

  @Test
  public void aComposedRuleIsWrittenAsBlockYaml() throws Exception {
    Map<String, Object> composed =
        entry(
            "type",
            "custom",
            "target",
            "TRANSFORM",
            "allOf",
            List.of(
                entry("targetField", "sql", "condition", "NOT_MATCHES_PATTERN"),
                entry("targetField", "limit", "condition", "NOT_EMPTY")));

    String after = LintPolicyYamlWriter.replaceRule(PROJECT_FILE, "SQL-002", composed);

    Map<?, ?> rules =
        (Map<?, ?>) ((Map<?, ?>) new org.yaml.snakeyaml.Yaml().load(after)).get("rules");
    assertEquals(composed, rules.get("SQL-002"));
    assertFalse(after.contains("{"), "flow style in a hand-edited file: " + after);
  }

  @Test
  public void theFileKeepsItsOwnIndentation() throws Exception {
    String file =
        """
        rules:
            DOC-001:
                enabled: false
        """;

    String after = LintPolicyYamlWriter.replaceRule(file, "DOC-002", entry("enabled", false));

    assertTrue(after.contains("\n    DOC-002:\n        enabled: false"), after);
  }

  @Test
  public void aQuotedOrDifferentlyCasedKeyIsTheSameRule() throws Exception {
    String file = """
        rules:
          'doc-001':
            enabled: false
        """;

    String after = LintPolicyYamlWriter.replaceRule(file, "DOC-001", entry("enabled", true));

    assertEquals("rules:\n  DOC-001:\n    enabled: true\n", after);
  }

  @Test
  public void removingTheLastRuleRemovesTheRulesKey() throws Exception {
    String file =
        """
        rules:
          DOC-001:
            enabled: false

        exclude:
          - "templates/**"
        """;

    String after = LintPolicyYamlWriter.replaceRule(file, "DOC-001", null);

    assertEquals("\nexclude:\n  - \"templates/**\"\n", after);
  }

  @Test
  public void aFileWithoutRulesGetsTheBlockAppended() throws Exception {
    String file = "exclude:\n  - \"templates/**\"\n";

    String after = LintPolicyYamlWriter.replaceRule(file, "DOC-001", entry("enabled", false));

    assertEquals(file + "\nrules:\n  DOC-001:\n    enabled: false\n", after);
  }

  @Test
  public void anEmptyInlineRulesMappingIsOpenedUp() throws Exception {
    String after =
        LintPolicyYamlWriter.replaceRule("rules: {}\n", "DOC-001", entry("enabled", false));

    assertEquals("rules:\n  DOC-001:\n    enabled: false\n", after);
  }

  @Test
  public void inlineRulesAreLeftToTheUser() {
    assertThrows(
        IOException.class,
        () ->
            LintPolicyYamlWriter.replaceRule(
                "rules: {DOC-001: {enabled: false}}\n", "DOC-002", entry("enabled", false)));
  }

  @Test
  public void removingARuleThatIsNotThereChangesNothing() throws Exception {
    Files.writeString(yaml(), PROJECT_FILE, StandardCharsets.UTF_8);

    assertFalse(LintPolicyYamlWriter.removeRule(yaml(), "NAMING-001"));
    assertEquals(PROJECT_FILE, Files.readString(yaml(), StandardCharsets.UTF_8));
  }
}
