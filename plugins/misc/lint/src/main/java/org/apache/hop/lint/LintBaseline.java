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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A record of the findings a project has already accepted, so a run reports only what is new.
 *
 * <p>This is what makes the linter adoptable on a project that already exists. Pointed at a mature
 * codebase it will report thousands of findings; nobody triages that, so the linter gets switched
 * off. Recording today's findings as the baseline turns it into a ratchet instead: the build fails
 * on anything added from now on, and the backlog is paid down deliberately.
 *
 * <p>Findings are matched on rule, file and the transform or action they point at — deliberately
 * not on the message, which embeds values that change for the same underlying problem, and not on a
 * line number, which lint findings do not have. Counts are kept per fingerprint, so a second
 * orphaned transform in a file that already had one is still reported.
 */
public final class LintBaseline {

  private static final int FORMAT_VERSION = 1;

  private final Map<String, Integer> accepted;

  private LintBaseline(Map<String, Integer> accepted) {
    this.accepted = accepted;
  }

  public static LintBaseline empty() {
    return new LintBaseline(new LinkedHashMap<>());
  }

  public boolean isEmpty() {
    return accepted.isEmpty();
  }

  /** Total findings recorded, counting duplicates. */
  public int size() {
    return accepted.values().stream().mapToInt(Integer::intValue).sum();
  }

  public static LintBaseline read(Path file) throws IOException {
    JsonNode root = new ObjectMapper().readTree(Files.readString(file, StandardCharsets.UTF_8));
    JsonNode version = root.get("version");
    if (version != null && version.asInt() != FORMAT_VERSION) {
      throw new IOException(
          "Unsupported baseline format version "
              + version.asInt()
              + " in "
              + file
              + "; regenerate it with --write-baseline.");
    }

    Map<String, Integer> accepted = new LinkedHashMap<>();
    JsonNode findings = root.get("findings");
    if (findings != null && findings.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fields = findings.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        accepted.put(entry.getKey(), Math.max(0, entry.getValue().asInt()));
      }
    }
    return new LintBaseline(accepted);
  }

  public static void write(Path file, List<LintResult> results, Path projectRoot)
      throws IOException {
    // Sorted so the file is stable across runs and reviewable in a diff.
    Map<String, Integer> counts = new TreeMap<>();
    for (LintResult result : results) {
      counts.merge(fingerprint(result, projectRoot), 1, Integer::sum);
    }

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode root = mapper.createObjectNode();
    root.put("version", FORMAT_VERSION);
    root.put(
        "comment",
        "Findings accepted as pre-existing. Delete an entry to start failing on it again.");
    ObjectNode findings = root.putObject("findings");
    counts.forEach(findings::put);

    Files.writeString(
        file,
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n",
        StandardCharsets.UTF_8);
  }

  /**
   * Keep only the findings this baseline does not already account for.
   *
   * <p>Consumes the recorded counts as it goes, so three accepted occurrences of a finding hide
   * three occurrences and report the fourth.
   */
  public List<LintResult> filter(List<LintResult> results, Path projectRoot) {
    if (accepted.isEmpty()) {
      return results;
    }
    Map<String, Integer> remaining = new LinkedHashMap<>(accepted);
    List<LintResult> fresh = new ArrayList<>();
    for (LintResult result : results) {
      String fingerprint = fingerprint(result, projectRoot);
      Integer left = remaining.get(fingerprint);
      if (left != null && left > 0) {
        remaining.put(fingerprint, left - 1);
        continue;
      }
      fresh.add(result);
    }
    return fresh;
  }

  /**
   * How many baseline entries were not seen in this run — findings that have since been fixed, or
   * whose file was renamed. Reporting the number lets a team prune the file without hunting.
   */
  public int countStaleEntries(List<LintResult> results, Path projectRoot) {
    Map<String, Integer> remaining = new LinkedHashMap<>(accepted);
    for (LintResult result : results) {
      String fingerprint = fingerprint(result, projectRoot);
      Integer left = remaining.get(fingerprint);
      if (left != null && left > 0) {
        remaining.put(fingerprint, left - 1);
      }
    }
    return remaining.values().stream().mapToInt(Integer::intValue).sum();
  }

  static String fingerprint(LintResult result, Path projectRoot) {
    String file = LintPolicy.relativise(result.getFileName(), projectRoot);
    String source = result.getSource() != null ? result.getSource().getName() : "";
    return result.getRuleId() + "|" + file + "|" + (source != null ? source : "");
  }
}
