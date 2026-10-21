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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.lint.registry.RuleRegistry;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The core rule pack against the code behind each rule.
 *
 * @see <a href="https://github.com/apache/hop/issues/8591">#8591</a>
 */
public class CoreRulePackReviewTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  private static CustomLintRule coreRule(String id) {
    CustomLintRule rule =
        RuleRegistry.getInstance().resolve(null).getRules().stream()
            .filter(r -> id.equals(r.generateRuleId()))
            .findFirst()
            .orElseThrow()
            .copy();
    rule.setEnabled(true);
    return rule;
  }

  // ------------------------------------------------------------------ TRANS-002

  /** Hop starts a transform that has no hops, so the rule must not say it never runs. */
  @Test
  public void trans002DoesNotClaimTheTransformNeverRuns() {
    String description = coreRule("TRANS-002").getDescription();

    assertFalse(description.contains("never executes"), description);
    assertTrue(description.contains("still runs"), description);
  }

  // ------------------------------------------------------------------ SEC-002 nested settings

  /** One file a transform reads, with a passphrase of its own, as PGP Decrypt Files has. */
  public static class FileEntry {
    @HopMetadataProperty private String fileName = "/data/in.pgp";
    @HopMetadataProperty private String passphrase;

    FileEntry(String passphrase) {
      this.passphrase = passphrase;
    }
  }

  /** Settings grouped in an object, as Text File Output's fileSettings are. */
  public static class ConnectionSettings {
    @HopMetadataProperty private String host = "example.org";
    @HopMetadataProperty private String password = "letmein";
  }

  public static class NestedSecretsMeta extends BaseTransformMeta {
    @HopMetadataProperty private ConnectionSettings settings = new ConnectionSettings();

    @HopMetadataProperty
    private List<FileEntry> files =
        new ArrayList<>(List.of(new FileEntry("s3cr3t"), new FileEntry("${PGP_PASSPHRASE}")));

    /** Not stored in the file, so not followed: a reference rather than a setting. */
    private ConnectionSettings cached = new ConnectionSettings();
  }

  private static List<String> sec002Messages(BaseTransformMeta meta) {
    TransformMeta transformMeta = new TransformMeta("Decrypt", "a transform", meta);
    return CustomRuleExecutor.executeRule(coreRule("SEC-002"), transformMeta, "/tmp/p.hpl").stream()
        .map(LintResult::getMessage)
        .toList();
  }

  @Test
  public void sec002LooksInsideTheSettingsATransformStores() {
    List<String> messages = sec002Messages(new NestedSecretsMeta());

    assertEquals(2, messages.size(), "unexpected findings: " + messages);
    assertTrue(
        messages.stream().anyMatch(m -> m.contains("field 'settings.password'")), "" + messages);
    assertTrue(
        messages.stream().anyMatch(m -> m.contains("field 'files[0].passphrase'")), "" + messages);
  }

  /** A field whose name ends in two patterns, such as accessToken, is one finding, not two. */
  public static class AccessTokenMeta extends BaseTransformMeta {
    private String accessToken = "abc";
  }

  @Test
  public void aFieldMatchingTwoPatternsIsReportedOnce() {
    assertEquals(1, sec002Messages(new AccessTokenMeta()).size());
  }

  // ------------------------------------------------------------------ variable syntaxes

  @Test
  public void everyVariableSyntaxHopResolvesCountsAsAVariable() {
    assertTrue(CustomRuleExecutor.isVariable("${DB_PASSWORD}"));
    assertTrue(CustomRuleExecutor.isVariable("%%DB_PASSWORD%%"));
    assertTrue(CustomRuleExecutor.isVariable("#{vault:secret/data/db:password}"));
    assertTrue(CustomRuleExecutor.isVariable("prefix-${SUFFIX}"));

    assertFalse(CustomRuleExecutor.isVariable("letmein"));
    assertFalse(CustomRuleExecutor.isVariable("$[6C,65,74]"), "hex spells a literal value");
    assertFalse(CustomRuleExecutor.isVariable("100%"));
    assertFalse(CustomRuleExecutor.isVariable("pa}ss${"));
  }

  public static class WindowsVariableMeta extends BaseTransformMeta {
    private String password = "%%DB_PASSWORD%%";
    private String apiKey = "#{vault:secret/data/api:key}";
  }

  @Test
  public void sec002AcceptsWindowsStyleVariablesAndResolvers() {
    assertEquals(List.of(), sec002Messages(new WindowsVariableMeta()));
  }

  // ------------------------------------------------------------------ Encrypted values

  private static List<LintResult> db001(String password) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("CRM");
    databaseMeta.setPassword(password);
    return CustomRuleExecutor.executeRule(
        coreRule("DB-001"), databaseMeta, "/tmp/metadata/rdbms/CRM.json");
  }

  /**
   * Hop decrypts a password field when it loads the file, so the linter mostly sees the plain
   * value. Where it does see an Encrypted one, that is reported as well, and the rule says so.
   */
  @Test
  public void db001ReportsAnEncryptedPassword() {
    String encrypted = Encr.encryptPasswordIfNotUsingVariables("secret123");
    List<LintResult> results = db001(encrypted);

    assertEquals(1, results.size());
    String message = results.get(0).getMessage();
    assertFalse(
        message.contains(encrypted.substring(Encr.PASSWORD_ENCRYPTED_PREFIX.length())), message);
    assertTrue(coreRule("DB-001").getDescription().contains("encrypted or not"));
  }

  @Test
  public void db001AcceptsAWindowsStyleVariable() {
    assertEquals(List.of(), db001("%%DB_PASSWORD%%"));
  }
}
