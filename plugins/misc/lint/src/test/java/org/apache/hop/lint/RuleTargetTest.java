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

/** Unit tests for RuleTarget enum */
public class RuleTargetTest {

  @Test
  public void testRuleTargetValues() {
    RuleTarget[] values = RuleTarget.values();

    // Membership, not an exact count: targets get added as the engine reaches further into a
    // project, and a count assertion turns that into an unrelated test failure.
    assertTrue(values.length >= 6);
    assertTrue(containsValue(values, RuleTarget.METADATA));
    assertTrue(containsValue(values, RuleTarget.PIPELINE));
    assertTrue(containsValue(values, RuleTarget.WORKFLOW));
    assertTrue(containsValue(values, RuleTarget.DATABASE_CONNECTION));
    assertTrue(containsValue(values, RuleTarget.TRANSFORM));
    assertTrue(containsValue(values, RuleTarget.ACTION));
    assertTrue(containsValue(values, RuleTarget.HOP));
  }

  @Test
  public void testRuleTargetValueOf() {
    assertEquals(RuleTarget.PIPELINE, RuleTarget.valueOf("PIPELINE"));
    assertEquals(RuleTarget.WORKFLOW, RuleTarget.valueOf("WORKFLOW"));
    assertEquals(RuleTarget.DATABASE_CONNECTION, RuleTarget.valueOf("DATABASE_CONNECTION"));
    assertEquals(RuleTarget.TRANSFORM, RuleTarget.valueOf("TRANSFORM"));
    assertEquals(RuleTarget.ACTION, RuleTarget.valueOf("ACTION"));
    assertEquals(RuleTarget.HOP, RuleTarget.valueOf("HOP"));
  }

  @Test
  public void testInvalidRuleTargetValueOf() {
    assertThrows(IllegalArgumentException.class, () -> RuleTarget.valueOf("INVALID"));
  }

  /**
   * {@code toString()} deliberately returns the human-readable display name: the enum is bound
   * directly to combo boxes in the rule builder. YAML round-tripping uses {@code name()}.
   */
  @Test
  public void testRuleTargetToString() {
    assertEquals("Pipeline", RuleTarget.PIPELINE.toString());
    assertEquals("Workflow", RuleTarget.WORKFLOW.toString());
    assertEquals("Database Connection", RuleTarget.DATABASE_CONNECTION.toString());
    assertEquals("Transform", RuleTarget.TRANSFORM.toString());
    assertEquals("Action", RuleTarget.ACTION.toString());
    assertEquals("Hop", RuleTarget.HOP.toString());
  }

  /** YAML stores {@code name()}, so that is the persistence contract. */
  @Test
  public void testRuleTargetNameIsStableForYaml() {
    assertEquals("PIPELINE", RuleTarget.PIPELINE.name());
    assertEquals("DATABASE_CONNECTION", RuleTarget.DATABASE_CONNECTION.name());
    assertEquals("HOP", RuleTarget.HOP.name());
  }

  @Test
  public void testRuleTargetOrdinal() {
    assertEquals(0, RuleTarget.PIPELINE.ordinal());
    assertEquals(1, RuleTarget.WORKFLOW.ordinal());
    assertEquals(2, RuleTarget.DATABASE_CONNECTION.ordinal());
    assertEquals(3, RuleTarget.TRANSFORM.ordinal());
    assertEquals(4, RuleTarget.ACTION.ordinal());
    assertEquals(5, RuleTarget.HOP.ordinal());
  }

  @Test
  public void testRuleTargetEquality() {
    assertEquals(RuleTarget.PIPELINE, RuleTarget.PIPELINE);
    assertNotEquals(RuleTarget.PIPELINE, RuleTarget.WORKFLOW);
    assertNotEquals(RuleTarget.DATABASE_CONNECTION, RuleTarget.TRANSFORM);
  }

  @Test
  public void testRuleTargetHashCode() {
    assertEquals(RuleTarget.PIPELINE.hashCode(), RuleTarget.PIPELINE.hashCode());
    assertNotEquals(RuleTarget.PIPELINE.hashCode(), RuleTarget.WORKFLOW.hashCode());
  }

  private boolean containsValue(RuleTarget[] values, RuleTarget target) {
    for (RuleTarget value : values) {
      if (value == target) {
        return true;
      }
    }
    return false;
  }
}
