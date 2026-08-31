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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Unit tests for CustomLintRule class */
public class CustomLintRuleTest {

  /**
   * A fresh rule is enabled at WARNING severity and already carries an identity, so that a rule
   * built in the rule-builder dialog can be stored before the user has filled anything in.
   */
  @Test
  public void testDefaultConstructor() {
    CustomLintRule rule = new CustomLintRule();

    assertNull(rule.getName());
    assertNull(rule.getDescription());
    assertNull(rule.getTarget());
    assertNull(rule.getTargetField());
    assertNull(rule.getCondition());
    assertNull(rule.getConditionValue());
    assertEquals("WARNING", rule.getSeverity());
    assertTrue(rule.isEnabled());
    assertNotNull(rule.getId());
    assertNotNull(rule.getAdditionalParameters());
    assertTrue(rule.getAdditionalParameters().isEmpty());
  }

  @Test
  public void testSettersAndGetters() {
    CustomLintRule rule = new CustomLintRule();

    rule.setName("Test Rule");
    rule.setDescription("Test Description");
    rule.setTarget(RuleTarget.PIPELINE);
    rule.setTargetField("name");
    rule.setCondition(RuleCondition.NOT_EMPTY);
    rule.setConditionValue("testValue");
    rule.setSeverity("ERROR");
    rule.setEnabled(true);

    assertEquals("Test Rule", rule.getName());
    assertEquals("Test Description", rule.getDescription());
    assertEquals(RuleTarget.PIPELINE, rule.getTarget());
    assertEquals("name", rule.getTargetField());
    assertEquals(RuleCondition.NOT_EMPTY, rule.getCondition());
    assertEquals("testValue", rule.getConditionValue());
    assertEquals("ERROR", rule.getSeverity());
    assertTrue(rule.isEnabled());
  }

  @Test
  /**
   * The rule id is the YAML key ("DB-001", "STRUCT-001", ...) — that is what the registry merges on
   * and what a project hop-lint.yml overlay refers to. When an id is set, it wins.
   */
  public void testGenerateRuleId() {
    CustomLintRule rule = new CustomLintRule();
    rule.setId("DB-001");
    rule.setName("Pipeline Name Required");
    rule.setTarget(RuleTarget.PIPELINE);
    rule.setTargetField("name");
    rule.setCondition(RuleCondition.NOT_EMPTY);

    assertEquals("DB-001", rule.generateRuleId());
  }

  @Test
  /** A rule that was never given an explicit id still has the constructor-assigned UUID. */
  public void testGenerateRuleIdWithNullValues() {
    CustomLintRule rule = new CustomLintRule();

    String ruleId = rule.generateRuleId();
    assertNotNull(ruleId);
    assertEquals(rule.getId(), ruleId);
  }

  @Test
  public void testGenerateRuleIdConsistency() {
    CustomLintRule rule = new CustomLintRule();
    rule.setName("Test Rule");
    rule.setTarget(RuleTarget.DATABASE_CONNECTION);
    rule.setTargetField("password");
    rule.setCondition(RuleCondition.NO_HARDCODED);

    String ruleId1 = rule.generateRuleId();
    String ruleId2 = rule.generateRuleId();

    assertEquals(ruleId1, ruleId2);
  }

  @Test
  public void testAdditionalParameters() {
    CustomLintRule rule = new CustomLintRule();

    rule.getAdditionalParameters().put("param1", "value1");
    rule.getAdditionalParameters().put("param2", 42);
    rule.getAdditionalParameters().put("param3", true);

    assertEquals(3, rule.getAdditionalParameters().size());
    assertEquals("value1", rule.getAdditionalParameters().get("param1"));
    assertEquals(42, rule.getAdditionalParameters().get("param2"));
    assertEquals(true, rule.getAdditionalParameters().get("param3"));
  }

  @Test
  public void testToString() {
    CustomLintRule rule = new CustomLintRule();
    rule.setName("Test Rule");
    rule.setDescription("Test Description");
    rule.setTarget(RuleTarget.PIPELINE);
    rule.setTargetField("name");
    rule.setCondition(RuleCondition.NOT_EMPTY);
    rule.setSeverity("ERROR");
    rule.setEnabled(true);

    // toString() is the label shown in the rule manager list, so it is the rule name.
    assertEquals("Test Rule", rule.toString());

    // The structural summary is what the rule builder shows underneath it.
    String summary = rule.getSummary();
    assertTrue(summary.contains("Pipeline"));
    assertTrue(summary.contains("name"));
    assertTrue(summary.contains("not empty"));
  }

  @Test
  /**
   * Rule identity is the rule id, not the rule's contents: two packs defining "DB-001" are the same
   * rule (the higher-priority pack wins), while two differently-keyed rules stay distinct even if
   * they check exactly the same thing.
   */
  public void testEqualsAndHashCode() {
    CustomLintRule rule1 = new CustomLintRule();
    rule1.setId("DB-001");
    rule1.setName("Test Rule");
    rule1.setTarget(RuleTarget.PIPELINE);
    rule1.setTargetField("name");

    CustomLintRule rule2 = new CustomLintRule();
    rule2.setId("DB-001");
    rule2.setName("Same Rule, Different Wording");
    rule2.setTarget(RuleTarget.PIPELINE);
    rule2.setTargetField("name");

    CustomLintRule rule3 = new CustomLintRule();
    rule3.setId("DB-002");
    rule3.setName("Test Rule");
    rule3.setTarget(RuleTarget.PIPELINE);
    rule3.setTargetField("name");

    assertEquals(rule1, rule2);
    assertNotEquals(rule1, rule3);
    assertNotEquals(rule2, rule3);

    assertEquals(rule1.hashCode(), rule2.hashCode());
    assertNotEquals(rule1.hashCode(), rule3.hashCode());

    // copy() preserves identity so registry merges stay stable.
    assertEquals(rule1, rule1.copy());
  }

  @Test
  public void testEqualsWithNull() {
    CustomLintRule rule = new CustomLintRule();
    rule.setName("Test Rule");

    assertNotEquals(rule, null);
    assertNotEquals(null, rule);
  }

  @Test
  public void testEqualsWithDifferentClass() {
    CustomLintRule rule = new CustomLintRule();
    rule.setName("Test Rule");

    assertNotEquals(rule, "Not a CustomLintRule");
    assertNotEquals(rule, new Object());
  }
}
