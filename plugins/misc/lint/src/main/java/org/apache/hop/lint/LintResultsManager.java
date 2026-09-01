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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;

/** Manages lint results and provides access for GUI components */
public class LintResultsManager {

  private static final ILogChannel log = LogChannel.GENERAL;
  private static LintResultsManager instance;

  // Store results by file path for quick lookup
  private final Map<String, List<LintResult>> resultsByFile = new ConcurrentHashMap<>();

  /**
   * Per-file, per-element-kind index of the findings, for the canvas painters.
   *
   * <p>A painter extension runs for every transform on every repaint. Copying the file's findings
   * and rebuilding this index each time made the cost of drawing a canvas proportional to the
   * number of transforms times the number of findings, on every frame. The index is built once and
   * thrown away whenever the results change.
   */
  private final Map<String, Map<String, List<LintResult>>> overlayIndexCache =
      new ConcurrentHashMap<>();

  // Store all results for global view
  private final List<LintResult> allResults = new ArrayList<>();

  // Track when results were last updated
  private long lastUpdateTime = 0;

  // Listeners for result changes
  private final List<LintResultsListener> listeners = new ArrayList<>();

  private LintResultsManager() {
    // Singleton
  }

  public static synchronized LintResultsManager getInstance() {
    if (instance == null) {
      instance = new LintResultsManager();
    }
    return instance;
  }

  /**
   * Update results from a linting run
   *
   * @param results New lint results
   */
  public synchronized void updateResults(List<LintResult> results) {
    log.logBasic("Updating lint results: " + results.size() + " issues found");

    // Clear previous results
    resultsByFile.clear();
    allResults.clear();

    // Group results by file
    for (LintResult result : results) {
      String fileName = result.getFileName();

      // Normalize file path
      String normalizedPath = LintPathUtils.normalizePath(fileName);

      resultsByFile.computeIfAbsent(normalizedPath, k -> new ArrayList<>()).add(result);
      allResults.add(result);
    }

    lastUpdateTime = System.currentTimeMillis();
    overlayIndexCache.clear();

    // Notify listeners
    notifyListeners();
  }

  /** Update results for a single file without clearing other file results. */
  public synchronized void updateResultsForFile(String filePath, List<LintResult> results) {
    String normalizedPath = LintPathUtils.normalizePath(filePath);

    // Capture the previous results for this file so we only notify (and trigger canvas/Explorer
    // refreshes) when something actually changed. Without this, every redraw re-fires the graph
    // update extension, which re-lints, which redraws again -- an endless refresh loop that
    // resets the Explorer tree and eventually exhausts SWT resources.
    List<LintResult> previous = new ArrayList<>();
    for (Map.Entry<String, List<LintResult>> entry : resultsByFile.entrySet()) {
      if (LintPathUtils.pathsMatch(entry.getKey(), normalizedPath)) {
        previous.addAll(entry.getValue());
      }
    }

    resultsByFile
        .entrySet()
        .removeIf(entry -> LintPathUtils.pathsMatch(entry.getKey(), normalizedPath));
    resultsByFile.put(normalizedPath, new ArrayList<>(results));
    allResults.removeIf(r -> LintPathUtils.pathsMatch(filePath, r.getFileName()));
    allResults.addAll(results);
    lastUpdateTime = System.currentTimeMillis();
    overlayIndexCache.clear();

    if (!sameResults(previous, results)) {
      notifyListeners();
    }
  }

  /** True when two result lists are equivalent (same issues, order-independent). */
  private static boolean sameResults(List<LintResult> a, List<LintResult> b) {
    if (a.size() != b.size()) {
      return false;
    }
    List<String> sa = new ArrayList<>(a.size());
    List<String> sb = new ArrayList<>(b.size());
    for (LintResult r : a) {
      sa.add(signatureOf(r));
    }
    for (LintResult r : b) {
      sb.add(signatureOf(r));
    }
    java.util.Collections.sort(sa);
    java.util.Collections.sort(sb);
    return sa.equals(sb);
  }

  private static String signatureOf(LintResult r) {
    String source =
        r.getSource() != null ? r.getSource().getKind() + ":" + r.getSource().getName() : "";
    return r.getRuleId() + "|" + r.getSeverity() + "|" + r.getMessage() + "|" + source;
  }

  /**
   * Get results for a specific file
   *
   * @param filePath Path to the file
   * @return List of results for the file, or empty list if none
   */
  public List<LintResult> getResultsForFile(String filePath) {
    String normalizedPath = LintPathUtils.normalizePath(filePath);
    List<LintResult> direct = resultsByFile.get(normalizedPath);
    if (direct != null) {
      return new ArrayList<>(direct);
    }

    List<LintResult> matched = new ArrayList<>();
    for (Map.Entry<String, List<LintResult>> entry : resultsByFile.entrySet()) {
      if (LintPathUtils.pathsMatch(entry.getKey(), normalizedPath)) {
        matched.addAll(entry.getValue());
      }
    }
    return matched;
  }

  /** Snapshot of per-file lint results keyed by normalized path. */
  public synchronized Map<String, List<LintResult>> getAllResultsByFile() {
    return new HashMap<>(resultsByFile);
  }

  /**
   * Check if a file has any lint issues
   *
   * @param filePath Path to the file
   * @return true if file has issues, false otherwise
   */
  public boolean hasIssues(String filePath) {
    return !getResultsForFile(filePath).isEmpty();
  }

  /**
   * Get the highest severity for a file
   *
   * @param filePath Path to the file
   * @return "ERROR", "WARNING", or null if no issues
   */
  public String getHighestSeverity(String filePath) {
    List<LintResult> results = getResultsForFile(filePath);
    if (results.isEmpty()) {
      return null;
    }

    boolean hasError = results.stream().anyMatch(r -> "ERROR".equals(r.getSeverity()));
    return hasError ? "ERROR" : "WARNING";
  }

  /**
   * Get all results
   *
   * @return All lint results
   */
  public List<LintResult> getAllResults() {
    return new ArrayList<>(allResults);
  }

  /**
   * Get results grouped by severity
   *
   * @return Map of severity to results
   */
  public Map<String, List<LintResult>> getResultsBySeverity() {
    Map<String, List<LintResult>> grouped = new HashMap<>();
    for (LintResult result : allResults) {
      grouped.computeIfAbsent(result.getSeverity(), k -> new ArrayList<>()).add(result);
    }
    return grouped;
  }

  /**
   * Get count of issues by severity
   *
   * @return Map of severity to count
   */
  public Map<String, Integer> getIssueCounts() {
    Map<String, Integer> counts = new HashMap<>();
    for (LintResult result : allResults) {
      counts.merge(result.getSeverity(), 1, Integer::sum);
    }
    return counts;
  }

  /**
   * The findings for a file, indexed by the name of the transform or action they point at.
   *
   * <p>Built once per file and reused until the results change, because the canvas painters ask for
   * this on every element of every repaint.
   *
   * @param filePath the file being drawn
   * @param kind whether the canvas is drawing transforms or actions
   * @return findings by element name; never null, and not to be modified by the caller
   */
  public Map<String, List<LintResult>> getOverlayIndex(String filePath, LintSourceRef.Kind kind) {
    if (filePath == null || kind == null) {
      return Collections.emptyMap();
    }
    String key = LintPathUtils.normalizePath(filePath) + "\u0000" + kind.name();
    return overlayIndexCache.computeIfAbsent(
        key,
        k ->
            Collections.unmodifiableMap(
                LintCanvasOverlayHelper.indexByElementName(getResultsForFile(filePath), kind)));
  }

  /** Clear all results */
  public synchronized void clearResults() {
    resultsByFile.clear();
    allResults.clear();
    overlayIndexCache.clear();
    lastUpdateTime = 0;
    notifyListeners();
  }

  /**
   * Get the time when results were last updated
   *
   * @return Timestamp of last update
   */
  public long getLastUpdateTime() {
    return lastUpdateTime;
  }

  /**
   * Check if results are available
   *
   * @return true if results have been loaded
   */
  public boolean hasResults() {
    return !allResults.isEmpty() || lastUpdateTime > 0;
  }

  /**
   * Add a listener for result changes
   *
   * @param listener The listener to add
   */
  public void addListener(LintResultsListener listener) {
    listeners.add(listener);
  }

  /**
   * Remove a listener
   *
   * @param listener The listener to remove
   */
  public void removeListener(LintResultsListener listener) {
    listeners.remove(listener);
  }

  /** Notify all listeners of result changes */
  private void notifyListeners() {
    for (LintResultsListener listener : listeners) {
      try {
        listener.onResultsUpdated();
      } catch (Exception e) {
        log.logError("Error notifying lint results listener: " + e.getMessage(), e);
      }
    }
  }

  /** Interface for listening to result changes */
  public interface LintResultsListener {
    void onResultsUpdated();
  }
}
