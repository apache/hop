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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * What a finding carries about the rule that produced it, for the reports to write out.
 *
 * <p>None of it changes what the rule checks: it is the rule's description, where to read more
 * about it, and the tags its pack gave it.
 *
 * @param description the rule's description, empty when it has none
 * @param helpUri where to read more about the rule, or null
 * @param tags the rule's tags, each key holding one or more values
 */
public record LintRuleDetails(String description, String helpUri, Map<String, List<String>> tags) {

  public static final LintRuleDetails NONE = new LintRuleDetails("", null, Map.of());

  public LintRuleDetails {
    description = description != null ? description : "";
    tags = copyOf(tags);
  }

  /** The details of a rule, or {@link #NONE} when there is no rule. */
  public static LintRuleDetails of(CustomLintRule rule) {
    if (rule == null) {
      return NONE;
    }
    return new LintRuleDetails(rule.getDescription(), rule.getHelpUri(), rule.getTags());
  }

  /**
   * Only the tags, for a finding reported under an id other than the rule's own.
   *
   * <p>A remark from Hop's own checks with an error code of its own is reported under that code.
   * The tags of the native rule that classified it apply to it, but that rule's description and
   * help link are about the rule, not about the code.
   */
  public LintRuleDetails tagsOnly() {
    return hasTags() ? new LintRuleDetails("", null, tags) : NONE;
  }

  public boolean hasTags() {
    return !tags.isEmpty();
  }

  /**
   * The tags as one {@code key:value} string per value, the form SARIF consumers read and the rule
   * manager shows.
   */
  public static List<String> flatTags(Map<String, List<String>> tags) {
    List<String> flat = new ArrayList<>();
    if (tags != null) {
      tags.forEach((key, values) -> values.forEach(value -> flat.add(key + ":" + value)));
    }
    return flat;
  }

  private static Map<String, List<String>> copyOf(Map<String, List<String>> tags) {
    if (tags == null || tags.isEmpty()) {
      return Map.of();
    }
    // Insertion order is kept so the reports list tags as the pack wrote them. A value given twice
    // is kept once: SARIF requires the tags of a rule to be unique.
    Map<String, List<String>> copy = new LinkedHashMap<>();
    tags.forEach(
        (key, values) ->
            copy.put(key, values != null ? List.copyOf(new LinkedHashSet<>(values)) : List.of()));
    return Collections.unmodifiableMap(copy);
  }
}
