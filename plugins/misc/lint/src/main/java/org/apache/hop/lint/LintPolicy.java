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

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;

/**
 * The parts of a project's lint configuration that decide what gets checked and what gets reported,
 * as opposed to which rules exist.
 *
 * <p>Both exist because a linter with no way to say "not here" cannot be adopted on a project that
 * already exists. Excluding paths keeps generated or vendored files out of the run entirely;
 * suppressing lets a team accept a specific finding on the record, with a reason, instead of
 * disabling the rule everywhere.
 *
 * <pre>
 * exclude:
 *   - "tests/**"
 *   - "generated/**"
 *
 * suppress:
 *   - rule: ACME-ENV-001
 *     path: "legacy/**"
 *     reason: "Legacy connections are pinned until the 2026 migration"
 *   - rule: TRANS-002
 *     source: "Reserved for phase 2"
 *     reason: "Placeholder kept deliberately, agreed with the data team"
 * </pre>
 */
public final class LintPolicy {

  private static final LintPolicy EMPTY = new LintPolicy(List.of(), List.of());

  private final List<String> excludes;
  private final List<Suppression> suppressions;

  public LintPolicy(List<String> excludes, List<Suppression> suppressions) {
    this.excludes = excludes != null ? List.copyOf(excludes) : List.of();
    this.suppressions = suppressions != null ? List.copyOf(suppressions) : List.of();
  }

  public static LintPolicy empty() {
    return EMPTY;
  }

  public List<String> getExcludes() {
    return excludes;
  }

  public List<Suppression> getSuppressions() {
    return suppressions;
  }

  public boolean isEmpty() {
    return excludes.isEmpty() && suppressions.isEmpty();
  }

  /**
   * True when the file should not be linted at all.
   *
   * @param file the file being considered
   * @param projectRoot the directory the patterns are relative to, may be null
   */
  public boolean isExcluded(String file, Path projectRoot) {
    if (excludes.isEmpty() || Utils.isEmpty(file)) {
      return false;
    }
    String relative = relativise(file, projectRoot);
    for (String pattern : excludes) {
      if (matches(pattern, relative)) {
        return true;
      }
    }
    return false;
  }

  /** Drop the findings this project has accepted, keeping the rest in order. */
  public List<LintResult> applySuppressions(List<LintResult> results, Path projectRoot) {
    if (suppressions.isEmpty() || results.isEmpty()) {
      return results;
    }
    List<LintResult> kept = new ArrayList<>(results.size());
    for (LintResult result : results) {
      if (!isSuppressed(result, projectRoot)) {
        kept.add(result);
      }
    }
    return kept;
  }

  private boolean isSuppressed(LintResult result, Path projectRoot) {
    String relative = relativise(result.getFileName(), projectRoot);
    String sourceName = result.getSource() != null ? result.getSource().getName() : null;
    for (Suppression suppression : suppressions) {
      if (suppression.matches(result.getRuleId(), relative, sourceName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Patterns are written against project-relative paths, which is how a user thinks about them and
   * what keeps a configuration portable between machines and CI.
   */
  static String relativise(String file, Path projectRoot) {
    if (Utils.isEmpty(file)) {
      return "";
    }
    String normalised = file.replace('\\', '/');
    if (projectRoot == null) {
      return normalised;
    }
    try {
      Path path = Paths.get(file);
      if (path.isAbsolute() && path.startsWith(projectRoot)) {
        return projectRoot.relativize(path).toString().replace('\\', '/');
      }
    } catch (Exception ignored) {
      // Metadata findings carry labels rather than paths; match them as written.
    }
    return normalised;
  }

  private static boolean matches(String pattern, String relativePath) {
    if (Utils.isEmpty(pattern)) {
      return false;
    }
    try {
      PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
      if (matcher.matches(Paths.get(relativePath))) {
        return true;
      }
      // A bare directory name is the pattern people reach for first, so treat "generated" as
      // "generated/**" rather than silently matching nothing.
      return !pattern.contains("*")
          && (relativePath.equals(pattern) || relativePath.startsWith(pattern + "/"));
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Invalid exclude pattern '" + pattern + "': " + e.getMessage());
      return false;
    }
  }

  /** One accepted finding, or family of findings, recorded in the project configuration. */
  public static final class Suppression {

    private final String ruleId;
    private final String path;
    private final String source;
    private final String reason;

    public Suppression(String ruleId, String path, String source, String reason) {
      this.ruleId = ruleId;
      this.path = path;
      this.source = source;
      this.reason = reason;
    }

    public String getRuleId() {
      return ruleId;
    }

    public String getPath() {
      return path;
    }

    public String getSource() {
      return source;
    }

    public String getReason() {
      return reason;
    }

    /**
     * A suppression must name a rule; path and source narrow it further, and an omitted one matches
     * anything. Suppressing every rule everywhere would be indistinguishable from switching the
     * linter off, so an entry without a rule id is rejected when the configuration is read.
     */
    boolean matches(String candidateRuleId, String relativePath, String sourceName) {
      if (!ruleId.equalsIgnoreCase(candidateRuleId)) {
        return false;
      }
      if (!Utils.isEmpty(path) && !LintPolicy.matches(path, relativePath)) {
        return false;
      }
      return Utils.isEmpty(source) || source.equalsIgnoreCase(sourceName);
    }
  }

  @Override
  public String toString() {
    return "LintPolicy["
        + excludes.size()
        + " exclude(s), "
        + suppressions.size()
        + " suppression(s)]";
  }
}
