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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.lint.registry.RulePackIds;
import org.apache.hop.lint.registry.RulePackOwner;
import org.junit.jupiter.api.Test;

public class CustomLintRulePackTest {

  @Test
  public void projectRulesAreEditable() {
    CustomLintRule rule = new CustomLintRule();
    rule.setPackId(RulePackIds.PROJECT);
    rule.setPackOwner(RulePackOwner.PROJECT);
    assertTrue(rule.isProjectEditable());
  }

  @Test
  public void vendorRulesAreNotEditable() {
    CustomLintRule rule = new CustomLintRule();
    rule.setPackId("vendor");
    rule.setPackOwner(RulePackOwner.VENDOR);
    assertFalse(rule.isProjectEditable());
    assertTrue(rule.isVendorPack());
  }

  @Test
  public void apacheRulesAreNotEditable() {
    CustomLintRule rule = new CustomLintRule();
    rule.setPackId(RulePackIds.HOP_CORE);
    rule.setPackOwner(RulePackOwner.APACHE);
    assertFalse(rule.isProjectEditable());
    assertFalse(rule.isVendorPack());
  }
}
