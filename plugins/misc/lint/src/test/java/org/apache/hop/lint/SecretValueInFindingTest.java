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

import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.ActionMeta;
import org.junit.jupiter.api.Test;

/**
 * A finding never carries the value of a secret.
 *
 * <p>DB-001 reported a hardcoded database password as "(current value: secret123)", in CI logs,
 * JSON and SARIF reports and the GUI, so the rule meant to catch exposed passwords exposed them.
 *
 * @see <a href="https://github.com/apache/hop/issues/8730">#8730</a>
 */
public class SecretValueInFindingTest {

  private static DatabaseMeta connection(String hostname, String username, String password) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("CRM");
    databaseMeta.setHostname(hostname);
    databaseMeta.setUsername(username);
    databaseMeta.setPassword(password);
    return databaseMeta;
  }

  /** The core pack's DB-001, as it ships. */
  private static CustomLintRule hardcodedDatabasePassword() {
    CustomLintRule rule = new CustomLintRule();
    rule.setId("DB-001");
    rule.setEnabled(true);
    rule.setSeverity("ERROR");
    rule.setTarget(RuleTarget.DATABASE_CONNECTION);
    rule.setTargetField("password");
    rule.setCondition(RuleCondition.NO_HARDCODED);
    rule.setName("Hardcoded Database Password");
    rule.setDescription("Database connections must take the password from a variable");
    return rule;
  }

  private static String onlyMessage(CustomLintRule rule, DatabaseMeta databaseMeta) {
    List<LintResult> results =
        CustomRuleExecutor.executeRule(rule, databaseMeta, "/tmp/metadata/rdbms/CRM.json");
    assertEquals(1, results.size(), "expected exactly one finding");
    return results.get(0).getMessage();
  }

  @Test
  public void aHardcodedPasswordIsReportedWithoutItsValue() {
    String message =
        onlyMessage(hardcodedDatabasePassword(), connection("localhost", "crm", "secret123"));

    assertFalse(message.contains("secret123"), message);
  }

  @Test
  public void anEncryptedPasswordIsReportedWithoutEitherForm() {
    String encrypted = Encr.encryptPasswordIfNotUsingVariables("secret123");

    String message =
        onlyMessage(hardcodedDatabasePassword(), connection("localhost", "crm", encrypted));

    assertFalse(message.contains("secret123"), message);
    assertFalse(message.contains(encrypted), message);
  }

  @Test
  public void aComposedRuleHidesThePasswordClause() {
    CustomLintRule rule = new CustomLintRule();
    rule.setId("CUSTOM-001");
    rule.setEnabled(true);
    rule.setSeverity("WARNING");
    rule.setTarget(RuleTarget.DATABASE_CONNECTION);
    rule.setTargetField("password");
    rule.setCondition(RuleCondition.MATCHES_PATTERN);
    rule.setConditionValue("^\\$\\{.*\\}$");
    rule.setAdditionalClauses(List.of(new RuleClause("username", RuleCondition.NOT_EMPTY, null)));
    rule.setDescription("Use a variable for the password, and name a user");

    String message = onlyMessage(rule, connection("localhost", "", "secret123"));

    assertFalse(message.contains("secret123"), message);
    assertTrue(message.contains("actual: hidden"), message);
  }

  @Test
  public void aValueThatIsNoSecretIsStillShown() {
    CustomLintRule rule = new CustomLintRule();
    rule.setId("CUSTOM-002");
    rule.setEnabled(true);
    rule.setSeverity("WARNING");
    rule.setTarget(RuleTarget.DATABASE_CONNECTION);
    rule.setTargetField("hostname");
    rule.setCondition(RuleCondition.MATCHES_PATTERN);
    rule.setConditionValue("^db\\..*");
    rule.setDescription("Connections point at a db.* host");

    String message = onlyMessage(rule, connection("localhost", "crm", "secret123"));

    assertTrue(message.contains("current value: localhost"), message);
  }

  /**
   * A connection whose secrets Hop stores as passwords under names the default patterns miss, as
   * the S3, MinIO and Salesforce connections do.
   */
  @HopMetadata(key = "lint-test-storage-connection", name = "Storage connection")
  public static class StorageConnection extends HopMetadataBase {
    @HopMetadataProperty private String endpoint = "https://storage.example.com";

    @HopMetadataProperty(password = true)
    private String accessKey = "AKIAEXAMPLEKEY";

    @HopMetadataProperty(key = "oauth_jwt_private_key", password = true)
    private String oauthJwtPrivateKey = "-----BEGIN PRIVATE KEY-----";

    /** A third-party plugin that names its secret but never flags it as a password. */
    @HopMetadataProperty(key = "secret_key")
    private String signingKey = "s3cr3t-signing-key";

    public StorageConnection() {
      super("storage");
    }
  }

  private static CustomLintRule metadataRule(String field, RuleCondition condition, String value) {
    CustomLintRule rule = new CustomLintRule();
    rule.setId("CUSTOM-003");
    rule.setEnabled(true);
    rule.setSeverity("WARNING");
    rule.setTarget(RuleTarget.METADATA);
    rule.setTargetField(field);
    rule.setCondition(condition);
    rule.setConditionValue(value);
    rule.setDescription("Use a variable");
    return rule;
  }

  private static String onlyMessage(CustomLintRule rule, StorageConnection connection) {
    List<LintResult> results =
        CustomRuleExecutor.executeRule(rule, connection, "/tmp/metadata/storage/storage.json");
    assertEquals(1, results.size(), "expected exactly one finding");
    return results.get(0).getMessage();
  }

  @Test
  public void aPasswordPropertyIsHiddenWhateverItIsNamed() {
    String message =
        onlyMessage(
            metadataRule("accessKey", RuleCondition.MATCHES_PATTERN, "^\\$\\{.*\\}$"),
            new StorageConnection());

    assertFalse(message.contains("AKIAEXAMPLEKEY"), message);
  }

  @Test
  public void aPasswordPropertyNamedByItsKeyIsHidden() {
    String message =
        onlyMessage(
            metadataRule("oauth_jwt_private_key", RuleCondition.MATCHES_PATTERN, "^\\$\\{.*\\}$"),
            new StorageConnection());

    assertFalse(message.contains("BEGIN PRIVATE KEY"), message);
  }

  @Test
  public void aSnakeCaseSecretKeyIsHidden() {
    String message =
        onlyMessage(
            metadataRule("secret_key", RuleCondition.MATCHES_PATTERN, "^\\$\\{.*\\}$"),
            new StorageConnection());

    assertFalse(message.contains("s3cr3t-signing-key"), message);
  }

  @Test
  public void aComposedRuleHidesAPasswordPropertyClause() {
    CustomLintRule rule = metadataRule("endpoint", RuleCondition.NOT_EMPTY, null);
    rule.setCondition(RuleCondition.MATCHES_PATTERN);
    rule.setConditionValue("^\\$\\{.*\\}$");
    rule.setAdditionalClauses(
        List.of(new RuleClause("accessKey", RuleCondition.MATCHES_PATTERN, "^\\$\\{.*\\}$")));

    String message = onlyMessage(rule, new StorageConnection());

    assertFalse(message.contains("AKIAEXAMPLEKEY"), message);
    assertTrue(message.contains("actual: hidden"), message);
  }

  @Test
  public void aHardcodedValueThatIsNoSecretIsStillShown() {
    String message =
        onlyMessage(
            metadataRule("endpoint", RuleCondition.NO_HARDCODED, null), new StorageConnection());

    assertTrue(message.contains("current value: https://storage.example.com"), message);
  }

  /** A transform that stores a secret as a password under a name no pattern catches. */
  public static class StreamTransformMeta extends BaseTransformMeta<ITransform, ITransformData> {
    @HopMetadataProperty(key = "access_key", password = true)
    private String accessKey = "AKIATRANSFORMKEY";
  }

  /** The same for an action. */
  public static class UploadAction extends ActionBase {
    @HopMetadataProperty(password = true)
    private String accessKey = "AKIAACTIONKEY";

    @Override
    public Result execute(Result previousResult, int nr) {
      return previousResult;
    }
  }

  private static CustomLintRule rule(RuleTarget target, String field) {
    CustomLintRule rule = metadataRule(field, RuleCondition.MATCHES_PATTERN, "^\\$\\{.*\\}$");
    rule.setTarget(target);
    return rule;
  }

  private static String onlyMessage(CustomLintRule rule, Object hopObject) {
    List<LintResult> results = CustomRuleExecutor.executeRule(rule, hopObject, "/tmp/test.hpl");
    assertEquals(1, results.size(), "expected exactly one finding");
    return results.get(0).getMessage();
  }

  private static TransformMeta streamTransform() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("Stream");
    transformMeta.setTransformPluginId("StreamConsume");
    transformMeta.setTransform(new StreamTransformMeta());
    return transformMeta;
  }

  @Test
  public void aTransformPasswordPropertyIsHiddenByItsJavaName() {
    String message = onlyMessage(rule(RuleTarget.TRANSFORM, "accessKey"), streamTransform());

    assertFalse(message.contains("AKIATRANSFORMKEY"), message);
  }

  @Test
  public void aTransformPasswordPropertyIsHiddenByItsKey() {
    String message = onlyMessage(rule(RuleTarget.TRANSFORM, "access_key"), streamTransform());

    assertFalse(message.contains("AKIATRANSFORMKEY"), message);
  }

  @Test
  public void anActionPasswordPropertyIsHidden() {
    UploadAction action = new UploadAction();
    action.setPluginId("Upload");
    ActionMeta actionMeta = new ActionMeta(action);

    String message = onlyMessage(rule(RuleTarget.ACTION, "accessKey"), actionMeta);

    assertFalse(message.contains("AKIAACTIONKEY"), message);
  }
}
