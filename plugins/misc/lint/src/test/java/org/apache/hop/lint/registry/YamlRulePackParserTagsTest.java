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
package org.apache.hop.lint.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.lint.CustomLintRule;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/** Tags and help links on a rule, and the warning for keys the linter does not read. */
public class YamlRulePackParserTagsTest {

  private static final String PACK =
      """
      pack:
        id: acme
      rules:
        ACME-001:
          type: custom
          target: DATABASE_CONNECTION
          targetField: password
          condition: NO_HARDCODED
          helpUri: https://example.com/rules/ACME-001
          tags:
            category: secrets
            owner: platform-team
            policy: ["SEC-POL-4", "SEC-POL-7"]
            level: 3
        ACME-002:
          type: native
          tags:
            category: verify
        ACME-003:
          type: custom
          target: PIPELINE
          targetField: name
          condition: NOT_EMPTY
      """;

  private static Map<String, CustomLintRule> load(String yaml) throws Exception {
    Map<String, CustomLintRule> rules = new LinkedHashMap<>();
    for (CustomLintRule rule :
        YamlRulePackParser.loadFromStream(
            new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)),
            "acme",
            RulePackOwner.VENDOR)) {
      rules.put(rule.getId(), rule);
    }
    return rules;
  }

  @Test
  public void tagsAreReadAsListsInTheOrderTheyWereWritten() throws Exception {
    CustomLintRule rule = load(PACK).get("ACME-001");

    assertEquals(
        List.of("category", "owner", "policy", "level"), List.copyOf(rule.getTags().keySet()));
    assertEquals(List.of("secrets"), rule.getTags().get("category"));
    assertEquals(List.of("SEC-POL-4", "SEC-POL-7"), rule.getTags().get("policy"));
    assertEquals(List.of("3"), rule.getTags().get("level"));
    assertEquals("https://example.com/rules/ACME-001", rule.getHelpUri());
  }

  @Test
  public void nativeRulesCarryTagsToo() throws Exception {
    assertEquals(Map.of("category", List.of("verify")), load(PACK).get("ACME-002").getTags());
  }

  @Test
  public void aRuleWithoutTagsHasNone() throws Exception {
    CustomLintRule rule = load(PACK).get("ACME-003");

    assertTrue(rule.getTags().isEmpty());
    assertNull(rule.getHelpUri());
  }

  @Test
  public void copyKeepsTagsAndHelpUriWithoutSharingThem() throws Exception {
    CustomLintRule rule = load(PACK).get("ACME-001");
    CustomLintRule copy = rule.copy();

    assertEquals(rule.getTags(), copy.getTags());
    assertEquals(rule.getHelpUri(), copy.getHelpUri());
    copy.getTags().get("policy").add("SEC-POL-9");
    assertEquals(2, rule.getTags().get("policy").size());
  }

  /** A rule saved from the rule manager keeps its tags and help link. */
  @Test
  public void tagsSurviveARoundTripThroughTheProjectFile() throws Exception {
    CustomLintRule rule = load(PACK).get("ACME-001");
    rule.setPackOwner(RulePackOwner.PROJECT);

    Map<String, Object> entry = ProjectLintYamlExporter.entryFor(rule);
    assertEquals("secrets", ((Map<?, ?>) entry.get("tags")).get("category"));

    CustomLintRule reread =
        YamlRulePackParser.parseCustomRule("ACME-001", entry, "project", RulePackOwner.PROJECT);
    assertEquals(rule.getTags(), reread.getTags());
    assertEquals(rule.getHelpUri(), reread.getHelpUri());
  }

  /**
   * A project sets a tag per key: its owner replaces the pack's owner, the pack's other tags stay,
   * and a key with no values removes the pack's tag.
   */
  @Test
  public void aProjectOverrideReplacesTagsPerKey() throws Exception {
    CustomLintRule rule = load(PACK).get("ACME-001");
    Map<String, Object> override = new LinkedHashMap<>();
    Map<String, Object> tags = new LinkedHashMap<>();
    tags.put("owner", "data-team");
    tags.put("level", List.of());
    tags.put("team", List.of("crm", "sales"));
    override.put("tags", tags);
    override.put("helpUri", "https://wiki.example.com/ACME-001");

    ProjectYamlOverlay.ProjectRuleOverlay.fromMap("ACME-001", override, "in hop-lint.yml")
        .applyTo(rule);

    assertEquals(List.of("data-team"), rule.getTags().get("owner"));
    assertEquals(List.of("secrets"), rule.getTags().get("category"));
    assertEquals(List.of("SEC-POL-4", "SEC-POL-7"), rule.getTags().get("policy"));
    assertEquals(List.of("crm", "sales"), rule.getTags().get("team"));
    assertTrue(!rule.getTags().containsKey("level"), "an empty key removes the pack's tag");
    assertEquals("https://wiki.example.com/ACME-001", rule.getHelpUri());
  }

  /** A bare helpUri: removes the pack's link, rather than becoming the text "null". */
  @Test
  public void aBareHelpUriOverrideRemovesTheLink() throws Exception {
    CustomLintRule rule = load(PACK).get("ACME-001");
    Map<String, Object> override = new LinkedHashMap<>();
    override.put("helpUri", null);

    ProjectYamlOverlay.ProjectRuleOverlay.fromMap("ACME-001", override, "in hop-lint.yml")
        .applyTo(rule);

    assertNull(rule.getHelpUri());
  }

  @Test
  public void aValueGivenTwiceIsKeptOnce() {
    assertEquals(
        Map.of("policy", List.of("SEC-POL-4", "SEC-POL-7")),
        YamlRulePackParser.tagsValue(
            Map.of("policy", List.of("SEC-POL-4", "SEC-POL-7", "SEC-POL-4")),
            "ACME-001",
            YamlRulePackParser.inPack("acme")));
  }

  /**
   * The rule manager writes only the keys the project changed, so the pack's other tags follow it.
   */
  @Test
  public void theExporterWritesOnlyTheChangedTagKeys() {
    Map<String, List<String>> pack = new LinkedHashMap<>();
    pack.put("category", List.of("secrets"));
    pack.put("owner", List.of("platform-team"));
    pack.put("level", List.of("3"));
    Map<String, List<String>> desired = new LinkedHashMap<>();
    desired.put("category", List.of("secrets"));
    desired.put("owner", List.of("data-team"));
    desired.put("team", List.of("crm", "sales"));

    Map<String, Object> expected = new LinkedHashMap<>();
    expected.put("owner", "data-team");
    expected.put("team", List.of("crm", "sales"));
    expected.put("level", List.of());
    assertEquals(expected, ProjectLintYamlExporter.tagOverrides(desired, pack));
  }

  /** Tagging a core rule in the rule manager is an override, not a copy of the whole rule. */
  @Test
  public void taggingAPackRuleIsWrittenAsAnOverride() {
    CustomLintRule rule =
        RuleRegistry.getInstance().resolve(null).getRules().stream()
            .filter(r -> "DB-001".equals(r.generateRuleId()))
            .findFirst()
            .orElseThrow()
            .copy();
    rule.getTags().put("owner", List.of("data-team"));

    assertEquals(
        Map.of("tags", Map.of("owner", "data-team")), ProjectLintYamlExporter.entryFor(rule));
  }

  /** A nested mapping is not a tag. It is left out rather than failing the whole pack. */
  @Test
  public void malformedTagsAreLeftOut() {
    Map<String, Object> tags = new LinkedHashMap<>();
    tags.put("owner", "platform-team");
    tags.put("nested", Map.of("a", "b"));

    assertEquals(
        Map.of("owner", List.of("platform-team")),
        YamlRulePackParser.tagsValue(tags, "ACME-001", YamlRulePackParser.inPack("acme")));
    assertTrue(
        YamlRulePackParser.tagsValue("secrets", "ACME-001", YamlRulePackParser.inPack("acme"))
            .isEmpty());
  }

  /** A typo such as tag: for tags: would otherwise drop every tag without anyone noticing. */
  @Test
  public void anUnknownKeyIsReportedWithTheClosestKnownOne() {
    Map<String, Object> ruleData = new LinkedHashMap<>();
    ruleData.put("type", "custom");
    ruleData.put("tag", Map.of("owner", "platform-team"));

    List<String> warnings =
        YamlRulePackParser.unknownKeyWarnings(
            "ACME-001",
            YamlRulePackParser.inPack("acme"),
            ruleData,
            YamlRulePackParser.CUSTOM_RULE_KEYS);

    assertEquals(
        List.of(
            "Warning: rule 'ACME-001' in pack 'acme' has an unknown key 'tag', which is ignored."
                + " Did you mean tags?"),
        warnings);
  }

  @Test
  public void knownKeysProduceNoWarning() {
    Map<String, Object> ruleData = new LinkedHashMap<>();
    for (String key : YamlRulePackParser.CUSTOM_RULE_KEYS) {
      ruleData.put(key, "x");
    }

    assertTrue(
        YamlRulePackParser.unknownKeyWarnings(
                "ACME-001",
                YamlRulePackParser.inPack("acme"),
                ruleData,
                YamlRulePackParser.CUSTOM_RULE_KEYS)
            .isEmpty());
  }

  /**
   * A project override that misspells a key used to leave the pack's rule unchanged silently. Every
   * project has a hop-lint.yml, so the warning names the file by its path: the same typo in a
   * second project is a second warning, not a repeat of the first.
   */
  @Test
  public void anUnknownOverrideKeyNamesTheProjectFile() {
    Map<String, Object> ruleData = Map.of("severty", "ERROR");
    File crm = new File("/projects/crm/hop-lint.yml");
    File sales = new File("/projects/sales/hop-lint.yml");

    List<String> warnings =
        YamlRulePackParser.unknownKeyWarnings(
            "DB-001", YamlRulePackParser.inFile(crm), ruleData, YamlRulePackParser.OVERRIDE_KEYS);

    assertEquals(
        List.of(
            "Warning: rule 'DB-001' in "
                + crm.getAbsolutePath()
                + " has an unknown key 'severty', which is ignored. Did you mean severity?"),
        warnings);
    assertNotEquals(
        warnings,
        YamlRulePackParser.unknownKeyWarnings(
            "DB-001",
            YamlRulePackParser.inFile(sales),
            ruleData,
            YamlRulePackParser.OVERRIDE_KEYS));
  }

  /**
   * DB-001 carried checkPasswords and checkUsernames, which no code read, so they looked like
   * settings and changed nothing.
   *
   * @see <a href="https://github.com/apache/hop/issues/8591">#8591</a>
   */
  @Test
  public void anUnknownParameterIsReportedWithTheClosestKnownOne() {
    Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("checkPasswords", true);
    parameters.put("fieldPattern", List.of("password"));
    parameters.put("blockingTransforms", List.of("SortRows"));

    List<String> warnings =
        YamlRulePackParser.unknownParameterWarnings(
            "DB-001", YamlRulePackParser.inPack("acme"), parameters);

    assertEquals(
        List.of(
            "Warning: rule 'DB-001' in pack 'acme' has an unknown parameter 'checkPasswords',"
                + " which is ignored.",
            "Warning: rule 'DB-001' in pack 'acme' has an unknown parameter 'fieldPattern',"
                + " which is ignored. Did you mean fieldPatterns?"),
        warnings);
  }

  @Test
  public void noParametersProduceNoWarning() {
    assertTrue(
        YamlRulePackParser.unknownParameterWarnings("DB-001", "in pack 'acme'", null).isEmpty());
    assertTrue(
        YamlRulePackParser.unknownParameterWarnings("DB-001", "in pack 'acme'", Map.of())
            .isEmpty());
  }

  /** Every key the core pack uses has to be a known key, or Hop warns about its own rules. */
  @Test
  public void theCorePackUsesOnlyKnownKeys() throws Exception {
    try (var in =
        YamlRulePackParser.class.getClassLoader().getResourceAsStream("hop-lint-core.yml")) {
      @SuppressWarnings("unchecked")
      Map<String, Object> rules =
          (Map<String, Object>) ((Map<String, Object>) new Yaml().load(in)).get("rules");
      for (Map.Entry<String, Object> entry : rules.entrySet()) {
        @SuppressWarnings("unchecked")
        Map<String, Object> ruleData = (Map<String, Object>) entry.getValue();
        var known =
            YamlRulePackParser.isNativeRuleDefinition(ruleData)
                ? YamlRulePackParser.NATIVE_RULE_KEYS
                : YamlRulePackParser.CUSTOM_RULE_KEYS;
        assertEquals(
            List.of(),
            YamlRulePackParser.unknownKeyWarnings(
                entry.getKey(), YamlRulePackParser.inPack("hop-core"), ruleData, known));
        assertEquals(
            List.of(),
            YamlRulePackParser.unknownParameterWarnings(
                entry.getKey(), YamlRulePackParser.inPack("hop-core"), ruleData.get("parameters")));
      }
    }
  }
}
