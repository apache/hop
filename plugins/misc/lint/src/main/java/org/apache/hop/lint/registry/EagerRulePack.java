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
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.lint.CustomLintRule;

/**
 * A rule pack whose rules have already been read into memory.
 *
 * <p>Packs discovered inside a jar are loaded through a classloader opened just for that jar. The
 * rules have to be read while it is still open, because the pack's own {@code loadRules()} reads a
 * resource from it. This snapshot lets the caller close the loader immediately instead of keeping
 * one open per jar for the lifetime of the process.
 */
final class EagerRulePack implements IHopLintRulePack {

  private final String packId;
  private final String displayName;
  private final RulePackOwner owner;
  private final int priority;
  private final List<CustomLintRule> rules;
  private final List<String> overrides;

  private EagerRulePack(
      String packId,
      String displayName,
      RulePackOwner owner,
      int priority,
      List<CustomLintRule> rules,
      List<String> overrides) {
    this.packId = packId;
    this.displayName = displayName;
    this.owner = owner;
    this.priority = priority;
    this.rules = rules;
    this.overrides = overrides;
  }

  /** Read a discovered pack's rules now, so its classloader can be released. */
  static EagerRulePack of(IHopLintRulePack pack) {
    List<CustomLintRule> loaded;
    try {
      loaded = List.copyOf(pack.loadRules());
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Failed to read rules from pack " + pack.getPackId() + ": " + e.getMessage(), e);
      loaded = Collections.emptyList();
    }
    return new EagerRulePack(
        pack.getPackId(),
        pack.getDisplayName(),
        pack.getOwner(),
        pack.getPriority(),
        loaded,
        pack.getOverrides());
  }

  @Override
  public String getPackId() {
    return packId;
  }

  @Override
  public String getDisplayName() {
    return displayName;
  }

  @Override
  public RulePackOwner getOwner() {
    return owner;
  }

  @Override
  public int getPriority() {
    return priority;
  }

  @Override
  public List<CustomLintRule> loadRules() {
    return rules;
  }

  @Override
  public List<String> getOverrides() {
    return overrides;
  }
}
