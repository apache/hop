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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit tests for LinterConfig class */
public class LinterConfigTest {

  @Test
  public void testDefaultConstructor() {
    LinterConfig config = new LinterConfig();

    assertNotNull(config.getRules());
    assertTrue(config.getRules().isEmpty());
    assertFalse(config.isEnabled());
    assertNull(config.getParameters());
  }

  @Test
  public void testSettersAndGetters() {
    LinterConfig config = new LinterConfig();

    // Test rules
    Map<String, RuleConfig> rules = new HashMap<>();
    RuleConfig rule1 = new RuleConfig();
    rule1.setEnabled(true);
    rules.put("TEST-001", rule1);
    config.setRules(rules);

    assertEquals(1, config.getRules().size());
    assertTrue(config.getRules().containsKey("TEST-001"));
    assertTrue(config.getRules().get("TEST-001").isEnabled());

    // Test enabled flag
    config.setEnabled(true);
    assertTrue(config.isEnabled());

    // Test parameters
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("maxTransforms", 20);
    config.setParameters(parameters);

    assertEquals(1, config.getParameters().size());
    assertEquals(20, config.getParameters().get("maxTransforms"));
  }

  @Test
  public void testIsRuleEnabled() {
    LinterConfig config = new LinterConfig();

    // Test with no rules
    assertFalse(config.isRuleEnabled("NONEXISTENT"));

    // Test with disabled rule
    Map<String, RuleConfig> rules = new HashMap<>();
    RuleConfig disabledRule = new RuleConfig();
    disabledRule.setEnabled(false);
    rules.put("DISABLED-001", disabledRule);
    config.setRules(rules);

    assertFalse(config.isRuleEnabled("DISABLED-001"));

    // Test with enabled rule
    RuleConfig enabledRule = new RuleConfig();
    enabledRule.setEnabled(true);
    rules.put("ENABLED-001", enabledRule);
    config.setRules(rules);

    assertTrue(config.isRuleEnabled("ENABLED-001"));
  }

  @Test
  public void testIsRuleEnabledWithNullRule() {
    LinterConfig config = new LinterConfig();

    Map<String, RuleConfig> rules = new HashMap<>();
    rules.put("NULL-RULE", null);
    config.setRules(rules);

    assertFalse(config.isRuleEnabled("NULL-RULE"));
  }

  @Test
  public void testGetRuleParameters() {
    LinterConfig config = new LinterConfig();

    Map<String, RuleConfig> rules = new HashMap<>();
    RuleConfig rule = new RuleConfig();
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("maxTransforms", 15);
    parameters.put("checkPasswords", true);
    rule.setParameters(parameters);
    rules.put("TEST-001", rule);
    config.setRules(rules);

    Map<String, Object> retrievedParams = config.getRuleParameters("TEST-001");
    assertNotNull(retrievedParams);
    assertEquals(15, retrievedParams.get("maxTransforms"));
    assertEquals(true, retrievedParams.get("checkPasswords"));

    // Test with non-existent rule
    assertNull(config.getRuleParameters("NONEXISTENT"));

    // Test with null rule
    rules.put("NULL-RULE", null);
    config.setRules(rules);
    assertNull(config.getRuleParameters("NULL-RULE"));
  }
}
