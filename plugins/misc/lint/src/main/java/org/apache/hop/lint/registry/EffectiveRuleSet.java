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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.lint.CustomLintRule;
import org.apache.hop.lint.LintPolicy;
import org.apache.hop.lint.LinterConfig;

/** Immutable effective rule set for a single lint run (packs merged + project overlay). */
public final class EffectiveRuleSet {

  private final List<CustomLintRule> rules;
  private final LinterConfig config;
  private final LintPolicy policy;

  public EffectiveRuleSet(List<CustomLintRule> rules, LinterConfig config) {
    this(rules, config, LintPolicy.empty());
  }

  public EffectiveRuleSet(List<CustomLintRule> rules, LinterConfig config, LintPolicy policy) {
    this.rules =
        rules != null
            ? Collections.unmodifiableList(new ArrayList<>(rules))
            : Collections.emptyList();
    this.config = config != null ? config : new LinterConfig();
    this.policy = policy != null ? policy : LintPolicy.empty();
  }

  /** What the project excludes from linting, and which findings it has accepted. */
  public LintPolicy getPolicy() {
    return policy;
  }

  public List<CustomLintRule> getRules() {
    return rules;
  }

  public LinterConfig getConfig() {
    return config;
  }

  public List<CustomLintRule> getEnabledRules() {
    List<CustomLintRule> enabled = new ArrayList<>();
    for (CustomLintRule rule : rules) {
      if (rule.isEnabled()) {
        enabled.add(rule);
      }
    }
    return enabled;
  }
}
