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

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
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

  /** What the results view sorts the findings of a severity on. */
  public enum SortKey {
    FILE,
    RULE,
    MESSAGE
  }

  /**
   * The order of the findings within a severity: by the chosen column, then by the others, so the
   * same results always come out in the same order. Findings used to appear in the order they were
   * found, which moved a file's findings to the bottom each time it was linted again.
   */
  public static Comparator<LintResult> order(SortKey key, boolean ascending) {
    Comparator<LintResult> byFile =
        Comparator.comparing((LintResult r) -> fileName(r), String.CASE_INSENSITIVE_ORDER)
            .thenComparing(r -> text(r.getFileName()), String.CASE_INSENSITIVE_ORDER);
    Comparator<LintResult> byRule =
        Comparator.comparing((LintResult r) -> text(r.getRuleId()), String.CASE_INSENSITIVE_ORDER);
    Comparator<LintResult> byMessage =
        Comparator.comparing((LintResult r) -> text(r.getMessage()), String.CASE_INSENSITIVE_ORDER);

    Comparator<LintResult> order =
        switch (key) {
          case RULE -> byRule.thenComparing(byFile).thenComparing(byMessage);
          case MESSAGE -> byMessage.thenComparing(byFile).thenComparing(byRule);
          default -> byFile.thenComparing(byRule).thenComparing(byMessage);
        };
    return ascending ? order : order.reversed();
  }

  /** ERROR, WARNING and INFO in that order, whatever came first; anything else after them. */
  public static Comparator<String> severityOrder() {
    List<String> known = List.of("ERROR", "WARNING", "INFO");
    return Comparator.comparing(
            (String severity) -> {
              int index = known.indexOf(severity == null ? "" : severity.toUpperCase());
              return index < 0 ? known.size() : index;
            })
        .thenComparing(severity -> text(severity), String.CASE_INSENSITIVE_ORDER);
  }

  private static String fileName(LintResult result) {
    String path = text(result.getFileName());
    return path.isEmpty() ? path : new File(LintPathUtils.normalizePath(path)).getName();
  }

  private static String text(String value) {
    return value == null ? "" : value;
  }

  public static int countBySeverity(List<LintResult> results, String severity) {
    return (int) results.stream().filter(result -> severity.equals(result.getSeverity())).count();
  }

  private static LintFileCategory fromFileName(LintResult result) {
    return LintFileCategory.fromFileName(result.getFileName());
  }
}
