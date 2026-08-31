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

import java.util.Collections;
import java.util.List;
import org.apache.hop.lint.CustomLintRule;

/**
 * SPI for discoverable lint rule packs. Prefer YAML via {@link LintRulePack#resource()}; override
 * {@link #loadRules()} only when YAML is insufficient.
 */
public interface IHopLintRulePack {

  String getPackId();

  String getDisplayName();

  RulePackOwner getOwner();

  int getPriority();

  List<CustomLintRule> loadRules();

  /**
   * Rule ids from other packs that this pack intends to replace.
   *
   * <p>Rule ids share one namespace, and a higher-priority pack used to win silently — so a
   * third-party pack could ship its own {@code DB-001} and quietly replace Apache's
   * hardcoded-password rule with something weaker, with nothing to show it had happened. A
   * collision that is not declared here is refused, and the incumbent rule stays.
   *
   * <p>Declared in the pack's YAML:
   *
   * <pre>
   * pack:
   *   id: acme
   *   overrides:
   *     - DB-001
   * </pre>
   */
  default List<String> getOverrides() {
    return Collections.emptyList();
  }
}
