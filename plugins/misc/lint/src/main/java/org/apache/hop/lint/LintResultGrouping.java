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
import java.util.Map;
import java.util.stream.Collectors;

/** Groups lint results for summary views. */
public final class LintResultGrouping {

  private LintResultGrouping() {}

  public static Map<String, List<LintResult>> bySeverity(List<LintResult> results) {
    return results.stream()
        .collect(
            Collectors.groupingBy(
                result -> result.getSeverity() != null ? result.getSeverity() : "OTHER",
                LinkedHashMap::new,
                Collectors.toList()));
  }

  public static Map<LintFileCategory, List<LintResult>> byCategory(List<LintResult> results) {
    Map<LintFileCategory, List<LintResult>> grouped = new LinkedHashMap<>();
    for (LintFileCategory category : LintFileCategory.values()) {
      grouped.put(category, new ArrayList<>());
    }

    for (LintResult result : results) {
      grouped.get(fromFileName(result)).add(result);
    }

    grouped.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    return grouped;
  }

  public static Map<String, List<LintResult>> byFile(List<LintResult> results) {
    Map<String, List<LintResult>> grouped = new LinkedHashMap<>();
    for (LintResult result : results) {
      String fileName = result.getFileName();
      if (fileName == null || fileName.isEmpty()) {
        fileName = "(unknown file)";
      }
      grouped.computeIfAbsent(fileName, key -> new ArrayList<>()).add(result);
    }

    return grouped.entrySet().stream()
        .sorted(Map.Entry.comparingByKey(String.CASE_INSENSITIVE_ORDER))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (left, right) -> left, LinkedHashMap::new));
  }

  public static Map<LintFileCategory, Map<String, List<LintResult>>> byCategoryAndFile(
      List<LintResult> results) {
    Map<LintFileCategory, Map<String, List<LintResult>>> grouped = new LinkedHashMap<>();

    for (LintResult result : results) {
      LintFileCategory category = fromFileName(result);
      String fileName = result.getFileName();
      if (fileName == null || fileName.isEmpty()) {
        fileName = "(unknown file)";
      }

      grouped
          .computeIfAbsent(category, key -> new LinkedHashMap<>())
          .computeIfAbsent(fileName, key -> new ArrayList<>())
          .add(result);
    }

    for (Map<String, List<LintResult>> files : grouped.values()) {
      Map<String, List<LintResult>> sorted =
          files.entrySet().stream()
              .sorted(Map.Entry.comparingByKey(String.CASE_INSENSITIVE_ORDER))
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      Map.Entry::getValue,
                      (left, right) -> left,
                      LinkedHashMap::new));
      files.clear();
      files.putAll(sorted);
    }

    return grouped;
  }

  public static int countBySeverity(List<LintResult> results, String severity) {
    return (int) results.stream().filter(result -> severity.equals(result.getSeverity())).count();
  }

  private static LintFileCategory fromFileName(LintResult result) {
    return LintFileCategory.fromFileName(result.getFileName());
  }
}
