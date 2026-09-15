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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Unit tests for RuleCondition enum */
public class RuleConditionTest {

  @Test
  public void testRuleConditionValues() {
    RuleCondition[] values = RuleCondition.values();

    // The condition vocabulary grows over time; assert the core set is present rather than an
    // exact count, so adding a condition does not break this test.
    assertTrue(values.length >= 6);
    assertTrue(containsValue(values, RuleCondition.MAX_VALUE));
    assertTrue(containsValue(values, RuleCondition.NOT_EMPTY));
    assertTrue(containsValue(values, RuleCondition.NO_HARDCODED));
    assertTrue(containsValue(values, RuleCondition.MATCHES_PATTERN));
    assertTrue(containsValue(values, RuleCondition.MUST_BE_TRUE));
    assertTrue(containsValue(values, RuleCondition.MUST_BE_FALSE));
  }

  @Test
  public void testRuleConditionValueOf() {
    assertEquals(RuleCondition.MAX_VALUE, RuleCondition.valueOf("MAX_VALUE"));
    assertEquals(RuleCondition.NOT_EMPTY, RuleCondition.valueOf("NOT_EMPTY"));
    assertEquals(RuleCondition.NO_HARDCODED, RuleCondition.valueOf("NO_HARDCODED"));
    assertEquals(RuleCondition.MATCHES_PATTERN, RuleCondition.valueOf("MATCHES_PATTERN"));
    assertEquals(RuleCondition.MUST_BE_TRUE, RuleCondition.valueOf("MUST_BE_TRUE"));
    assertEquals(RuleCondition.MUST_BE_FALSE, RuleCondition.valueOf("MUST_BE_FALSE"));
  }

  @Test
  public void testInvalidRuleConditionValueOf() {
    assertThrows(IllegalArgumentException.class, () -> RuleCondition.valueOf("INVALID"));
  }

  /**
   * {@code toString()} deliberately returns the human-readable display name: the enum is bound
   * directly to combo boxes in the rule builder. YAML round-tripping uses {@code name()}.
   */
  @Test
  public void testRuleConditionToString() {
    assertEquals("Maximum Value", RuleCondition.MAX_VALUE.toString());
    assertEquals("Not Empty", RuleCondition.NOT_EMPTY.toString());
    assertEquals("No Hardcoded Values", RuleCondition.NO_HARDCODED.toString());
    assertEquals("Matches Pattern", RuleCondition.MATCHES_PATTERN.toString());
    assertEquals("Must Be True", RuleCondition.MUST_BE_TRUE.toString());
    assertEquals("Must Be False", RuleCondition.MUST_BE_FALSE.toString());
  }

  /** YAML stores {@code name()}, so that — not the ordinal — is the persistence contract. */
  @Test
  public void testRuleConditionNameIsStableForYaml() {
    assertEquals("MAX_VALUE", RuleCondition.MAX_VALUE.name());
    assertEquals("NOT_EMPTY", RuleCondition.NOT_EMPTY.name());
    assertEquals("NO_HARDCODED", RuleCondition.NO_HARDCODED.name());
    assertEquals("MATCHES_PATTERN", RuleCondition.MATCHES_PATTERN.name());
    assertEquals("MUST_BE_TRUE", RuleCondition.MUST_BE_TRUE.name());
    assertEquals("MUST_BE_FALSE", RuleCondition.MUST_BE_FALSE.name());
  }

  @Test
  public void testRuleConditionEquality() {
    assertEquals(RuleCondition.MAX_VALUE, RuleCondition.MAX_VALUE);
    assertNotEquals(RuleCondition.MAX_VALUE, RuleCondition.NOT_EMPTY);
    assertNotEquals(RuleCondition.NO_HARDCODED, RuleCondition.MATCHES_PATTERN);
  }

  @Test
  public void testRuleConditionHashCode() {
    assertEquals(RuleCondition.MAX_VALUE.hashCode(), RuleCondition.MAX_VALUE.hashCode());
    assertNotEquals(RuleCondition.MAX_VALUE.hashCode(), RuleCondition.NOT_EMPTY.hashCode());
  }

  private boolean containsValue(RuleCondition[] values, RuleCondition condition) {
    for (RuleCondition value : values) {
      if (value == condition) {
        return true;
      }
    }
    return false;
  }
}
