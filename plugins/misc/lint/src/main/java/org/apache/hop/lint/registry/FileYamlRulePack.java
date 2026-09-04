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

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hop.lint.CustomLintRule;

/** Rule pack backed by a YAML file discovered under $HOP_HOME/plugins. */
public class FileYamlRulePack implements IHopLintRulePack {

  private final File file;
  private final String packId;
  private final String displayName;
  private final RulePackOwner owner;
  private final int priority;
  private final List<String> overrides;

  public FileYamlRulePack(
      File file, String packId, String displayName, RulePackOwner owner, int priority) {
    this(file, packId, displayName, owner, priority, java.util.Collections.emptyList());
  }

  public FileYamlRulePack(
      File file,
      String packId,
      String displayName,
      RulePackOwner owner,
      int priority,
      List<String> overrides) {
    this.file = file;
    this.packId = packId;
    this.displayName = displayName;
    this.owner = owner;
    this.priority = priority;
    this.overrides = overrides != null ? List.copyOf(overrides) : java.util.Collections.emptyList();
  }

  @Override
  public List<String> getOverrides() {
    return overrides;
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
    try {
      return YamlRulePackParser.loadFromFile(file, packId, owner);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load rule pack from " + file.getAbsolutePath(), e);
    }
  }
}
