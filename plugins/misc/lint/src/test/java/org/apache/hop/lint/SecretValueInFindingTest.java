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
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.encryption.Encr;
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
}
