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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hop.core.util.Utils;

/** Removes duplicate lint findings, especially when native Hop verify and YAML rules overlap. */
public final class LintResultDeduplicator {

  private LintResultDeduplicator() {}

  public static List<LintResult> deduplicate(List<LintResult> results) {
    if (results == null || results.isEmpty()) {
      return List.of();
    }

    // Group first, decide second. The semantic buckets below match on the words in a message so
    // that Hop's own check and the lint rule saying the same thing collapse into one finding. That
    // only holds across origins: two lint rules which happen to share a word are two findings, and
    // collapsing them let one rule silently mask another.
    Map<String, List<LintResult>> grouped = new LinkedHashMap<>();
    for (LintResult result : results) {
      grouped.computeIfAbsent(dedupeKey(result), k -> new ArrayList<>()).add(result);
    }

    List<LintResult> deduplicated = new ArrayList<>();
    for (List<LintResult> group : grouped.values()) {
      deduplicated.addAll(reduceGroup(group));
    }
    return deduplicated;
  }

  /**
   * Reduce one bucket to the findings worth reporting.
   *
   * <p>Within a bucket, distinct lint rules are all kept: they are distinct findings however
   * similarly they are worded. Hop's own remarks are dropped only when a lint rule is already
   * reporting the same thing, which is what the buckets exist for.
   */
  private static List<LintResult> reduceGroup(List<LintResult> group) {
    if (group.size() == 1) {
      return group;
    }
    Map<String, LintResult> byRule = new LinkedHashMap<>();
    boolean hasLintResult = group.stream().anyMatch(r -> r.getOrigin() == LintResult.Origin.LINT);

    for (LintResult result : group) {
      if (hasLintResult && result.getOrigin() == LintResult.Origin.HOP_NATIVE) {
        continue;
      }
      String ruleKey = normalize(result.getRuleId()) + "|" + normalize(result.getMessage());
      LintResult existing = byRule.get(ruleKey);
      byRule.put(ruleKey, existing == null ? result : preferResult(existing, result));
    }
    return new ArrayList<>(byRule.values());
  }

  private static LintResult preferResult(LintResult left, LintResult right) {
    if (left.getOrigin() == LintResult.Origin.LINT
        && right.getOrigin() == LintResult.Origin.HOP_NATIVE) {
      return left;
    }
    if (right.getOrigin() == LintResult.Origin.LINT
        && left.getOrigin() == LintResult.Origin.HOP_NATIVE) {
      return right;
    }
    if ("ERROR".equalsIgnoreCase(right.getSeverity())
        && !"ERROR".equalsIgnoreCase(left.getSeverity())) {
      return right;
    }
    return left;
  }

  private static String dedupeKey(LintResult result) {
    String file = normalize(result.getFileName());
    String source = "";
    if (result.getSource() != null) {
      source = result.getSource().getKind() + ":" + normalize(result.getSource().getName());
    }

    String semantic = semanticBucket(result);
    return file + "|" + source + "|" + semantic;
  }

  private static String semanticBucket(LintResult result) {
    String ruleId = normalize(result.getRuleId());
    String message = normalize(result.getMessage());

    if (ruleId.contains("trans-002")
        || message.contains("orphan")
        || message.contains("not used")
        || message.contains("is not used")) {
      return "unused-transform";
    }
    if (message.contains("disabled hop")) {
      return "disabled-hop";
    }
    if (message.contains("description") && message.contains("empty")) {
      return "missing-description";
    }
    return ruleId + "|" + message;
  }

  private static String normalize(String value) {
    if (Utils.isEmpty(value)) {
      return "";
    }
    return value.trim().toLowerCase(Locale.ROOT);
  }
}
