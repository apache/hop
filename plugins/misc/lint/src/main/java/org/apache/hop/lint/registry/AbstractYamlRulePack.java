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

import java.util.List;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lint.CustomLintRule;

/** Base class for thin Java registrars that point at a YAML rule pack on the classpath. */
public abstract class AbstractYamlRulePack implements IHopLintRulePack {

  private final String packId;
  private final String displayName;
  private final RulePackOwner owner;
  private final int priority;
  private final String resourcePath;

  /** The YAML's pack block, read once; null until then. */
  private YamlRulePackParser.PackMetadata metadata;

  private boolean metadataRead;

  protected AbstractYamlRulePack(
      String packId, String displayName, RulePackOwner owner, int priority, String resourcePath) {
    this.packId = packId;
    this.displayName = displayName;
    this.owner = owner;
    this.priority = priority;
    this.resourcePath = resourcePath;
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

  private synchronized YamlRulePackParser.PackMetadata packMetadata() {
    if (!metadataRead) {
      metadata =
          YamlRulePackParser.readPackMetadataWithAdjacentFallback(
              resourcePath, packId, owner, getClass());
      metadataRead = true;
    }
    return metadata;
  }

  /** The rules this pack replaces, as declared in the {@code overrides:} of its YAML pack block. */
  @Override
  public List<String> getOverrides() {
    YamlRulePackParser.PackMetadata pack = packMetadata();
    return pack != null ? List.copyOf(pack.overrides()) : List.of();
  }

  /** The {@code version:} of its YAML pack block, or else the version of the jar it ships in. */
  @Override
  public String getVersion() {
    YamlRulePackParser.PackMetadata pack = packMetadata();
    if (pack != null && !Utils.isEmpty(pack.version())) {
      return pack.version();
    }
    return getClass().getPackage() != null
        ? getClass().getPackage().getImplementationVersion()
        : null;
  }

  @Override
  public List<CustomLintRule> loadRules() {
    return YamlRulePackParser.loadRulesWithAdjacentFallback(
        resourcePath, packId, owner, getClass());
  }
}
