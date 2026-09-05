/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.lint;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.util.Utils;
import org.yaml.snakeyaml.Yaml;

/**
 * Adds an exclusion or a suppression to a project's {@code hop-lint.yml} from the user interface.
 *
 * <p>The file is edited as text rather than parsed and rewritten. A project's lint configuration is
 * meant to be read and hand-edited — the documentation says as much — and round-tripping it through
 * a YAML dumper would return it stripped of every comment and with the keys in whatever order the
 * parser felt like. So the new entry is inserted into the block it belongs to, or a block is
 * appended when there is none, and the rest of the file is left exactly as the user wrote it.
 *
 * <p>The result is parsed before it is saved. Text editing is the right call here but it is also
 * the kind that can produce a file nobody can load, and losing a project's lint configuration to a
 * convenience feature would be a poor trade.
 */
public final class LintPolicyYamlWriter {

  private static final String EXCLUDE_KEY = "exclude";
  private static final String SUPPRESS_KEY = "suppress";

  private LintPolicyYamlWriter() {}

  /** Keep a file or folder out of linting entirely. The pattern is project-relative. */
  public static void addExclude(Path yamlFile, String pattern, String comment) throws IOException {
    if (isBlank(pattern)) {
      throw new IOException("An exclusion needs a path pattern");
    }
    List<String> entry = new ArrayList<>();
    if (!Utils.isEmpty(comment)) {
      entry.add("  # " + singleLine(comment));
    }
    entry.add("  - " + quote(pattern));
    write(yamlFile, EXCLUDE_KEY, entry, pattern);
  }

  /**
   * Accept a finding on the record.
   *
   * @param ruleId the rule to accept, required — a suppression without one silences everything
   * @param path project-relative path pattern to narrow it to, may be null
   * @param source the transform or action name to narrow it to, may be null
   * @param reason why, required, so the decision can be reviewed later
   */
  public static void addSuppression(
      Path yamlFile, String ruleId, String path, String source, String reason) throws IOException {
    if (isBlank(ruleId)) {
      throw new IOException("A suppression needs a rule id");
    }
    if (isBlank(reason)) {
      throw new IOException("A suppression needs a reason");
    }

    List<String> entry = new ArrayList<>();
    entry.add("  - rule: " + quote(ruleId));
    if (!Utils.isEmpty(path)) {
      entry.add("    path: " + quote(path));
    }
    if (!Utils.isEmpty(source)) {
      entry.add("    source: " + quote(source));
    }
    entry.add("    reason: " + quote(reason));
    write(yamlFile, SUPPRESS_KEY, entry, ruleId);
  }

  /**
   * Put an excluded file or folder back under linting.
   *
   * <p>Written as a toggle rather than a one-way door: the menu that excluded the file is the
   * obvious place to look for the way back, and hunting through a YAML file for the line you just
   * added is not a way back anybody enjoys.
   *
   * @return true when an entry was removed
   */
  public static boolean removeExclude(Path yamlFile, String pattern) throws IOException {
    if (!Files.exists(yamlFile) || isBlank(pattern)) {
      return false;
    }
    String original = Files.readString(yamlFile, StandardCharsets.UTF_8);
    List<String> lines = new ArrayList<>(List.of(original.split("\n", -1)));

    int keyLine = indexOfTopLevelKey(lines, EXCLUDE_KEY);
    if (keyLine < 0) {
      return false;
    }
    int blockEnd = endOfBlock(lines, keyLine);

    List<String> kept = new ArrayList<>(lines.subList(0, keyLine + 1));
    int removed = 0;
    int survivors = 0;
    for (List<String> item : listItems(lines, keyLine + 1, blockEnd)) {
      if (isExcludeOf(item, pattern)) {
        removed++;
      } else {
        survivors++;
        kept.addAll(item);
      }
    }
    if (removed == 0) {
      return false;
    }
    kept.addAll(lines.subList(blockEnd, lines.size()));
    if (survivors == 0) {
      kept.remove(keyLine);
    }

    String updated = String.join("\n", kept);
    try {
      new Yaml().load(updated);
    } catch (Exception e) {
      throw new IOException(
          "Removing the exclusion would have made hop-lint.yml unreadable: " + e.getMessage(), e);
    }
    Files.writeString(yamlFile, updated, StandardCharsets.UTF_8);
    return true;
  }

  /**
   * Whether a list item is the exclusion of this pattern.
   *
   * <p>The comment written above an entry belongs to it, and travels with it when it goes.
   */
  private static boolean isExcludeOf(List<String> item, String pattern) {
    for (String line : item) {
      String trimmed = line.trim();
      if (trimmed.isEmpty() || trimmed.startsWith("#")) {
        continue;
      }
      String value = trimmed.startsWith("- ") ? trimmed.substring(2).trim() : trimmed;
      return pattern.equals(unquote(value));
    }
    return false;
  }

  /**
   * Remove the suppressions recorded for one transform or action, whatever rule they name.
   *
   * <p>The counterpart of the dialog: taking a decision back has to be as easy as making it, or
   * people work around the linter instead of with it. Entries are matched on path and source, and
   * everything else in the file — other suppressions, rules, comments — is left alone.
   *
   * @return how many entries were removed
   */
  public static int removeSuppressionsFor(Path yamlFile, String path, String source)
      throws IOException {
    if (!Files.exists(yamlFile) || isBlank(source)) {
      return 0;
    }
    String original = Files.readString(yamlFile, StandardCharsets.UTF_8);
    List<String> lines = new ArrayList<>(List.of(original.split("\n", -1)));

    int keyLine = indexOfTopLevelKey(lines, SUPPRESS_KEY);
    if (keyLine < 0) {
      return 0;
    }
    int blockEnd = endOfBlock(lines, keyLine);

    List<String> kept = new ArrayList<>(lines.subList(0, keyLine + 1));
    int removed = 0;
    int survivors = 0;
    for (List<String> item : listItems(lines, keyLine + 1, blockEnd)) {
      if (matchesEntry(item, path, source)) {
        removed++;
      } else {
        survivors++;
        kept.addAll(item);
      }
    }
    if (removed == 0) {
      return 0;
    }
    kept.addAll(lines.subList(blockEnd, lines.size()));

    // A suppress: key with nothing under it is valid YAML that reads as an oversight, so the
    // last entry takes the key with it.
    if (survivors == 0) {
      kept.remove(keyLine);
    }

    String updated = String.join("\n", kept);
    try {
      new Yaml().load(updated);
    } catch (Exception e) {
      throw new IOException(
          "Removing the suppression would have made hop-lint.yml unreadable: " + e.getMessage(), e);
    }
    Files.writeString(yamlFile, updated, StandardCharsets.UTF_8);
    return removed;
  }

  /**
   * Split a block into its list items, each starting at a line whose first token is "-".
   *
   * <p>A comment sitting above an entry describes it — that is where the reason for an exclusion is
   * written — so it is part of that item and goes when the item goes. A comment with no entry under
   * it belongs to nobody and becomes an item of its own, which nothing ever matches.
   */
  private static List<List<String>> listItems(List<String> lines, int from, int to) {
    List<List<String>> items = new ArrayList<>();
    List<String> pending = new ArrayList<>();
    List<String> current = null;

    for (int i = from; i < to; i++) {
      String line = lines.get(i);
      String trimmed = line.trim();

      if (trimmed.startsWith("- ")) {
        current = new ArrayList<>(pending);
        pending.clear();
        current.add(line);
        items.add(current);
      } else if (trimmed.isEmpty() || trimmed.startsWith("#")) {
        // Held back: it belongs to the entry below it, if there is one.
        pending.add(line);
      } else if (current == null) {
        current = new ArrayList<>(pending);
        pending.clear();
        current.add(line);
        items.add(current);
      } else {
        current.addAll(pending);
        pending.clear();
        current.add(line);
      }
    }
    if (!pending.isEmpty()) {
      items.add(pending);
    }
    return items;
  }

  /** Whether a written entry is the one for this file and element. */
  private static boolean matchesEntry(List<String> item, String path, String source) {
    String entryPath = null;
    String entrySource = null;
    boolean isEntry = false;
    for (String line : item) {
      String trimmed = line.trim();
      if (trimmed.startsWith("#")) {
        continue;
      }
      String withoutDash = trimmed.startsWith("- ") ? trimmed.substring(2).trim() : trimmed;
      if (withoutDash.startsWith("rule:")) {
        // Whatever order the keys were written in: this is a suppression entry.
        isEntry = true;
      }
      if (withoutDash.startsWith("path:")) {
        entryPath = unquote(withoutDash.substring("path:".length()).trim());
      } else if (withoutDash.startsWith("source:")) {
        entrySource = unquote(withoutDash.substring("source:".length()).trim());
      }
    }
    return isEntry
        && source.equals(entrySource)
        && (isBlank(path) ? entryPath == null : path.equals(entryPath));
  }

  private static String unquote(String value) {
    String trimmed = value.trim();
    if (trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
      return trimmed.substring(1, trimmed.length() - 1).replace("\\\"", "\"").replace("\\\\", "\\");
    }
    return trimmed;
  }

  private static void write(Path yamlFile, String key, List<String> entryLines, String expected)
      throws IOException {
    String original =
        Files.exists(yamlFile) ? Files.readString(yamlFile, StandardCharsets.UTF_8) : "";
    String updated = insert(original, key, entryLines);

    verify(updated, key, expected);

    if (yamlFile.getParent() != null) {
      Files.createDirectories(yamlFile.getParent());
    }
    Files.writeString(yamlFile, updated, StandardCharsets.UTF_8);
  }

  static String insert(String original, String key, List<String> entryLines) {
    List<String> lines = new ArrayList<>(List.of(original.split("\n", -1)));
    int keyLine = indexOfTopLevelKey(lines, key);

    if (keyLine < 0) {
      StringBuilder appended = new StringBuilder(original);
      if (!original.isEmpty() && !original.endsWith("\n")) {
        appended.append("\n");
      }
      if (!original.isBlank()) {
        appended.append("\n");
      }
      appended.append(key).append(":\n");
      entryLines.forEach(line -> appended.append(line).append("\n"));
      return appended.toString();
    }

    lines.addAll(endOfBlock(lines, keyLine), entryLines);
    return String.join("\n", lines);
  }

  /** The line holding {@code key:} at the start of a line, or -1. */
  private static int indexOfTopLevelKey(List<String> lines, String key) {
    for (int i = 0; i < lines.size(); i++) {
      String line = lines.get(i);
      if (line.startsWith(key + ":") && line.substring(key.length() + 1).trim().isEmpty()) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Where the block belonging to the key ends: the first line that starts a new top-level key.
   * Trailing blank lines and comments belong to whatever comes next, so they stay below the entry.
   */
  private static int endOfBlock(List<String> lines, int keyLine) {
    int lastContent = keyLine;
    for (int i = keyLine + 1; i < lines.size(); i++) {
      String line = lines.get(i);
      if (line.isBlank()) {
        continue;
      }
      boolean partOfBlock = line.startsWith(" ") || line.startsWith("\t") || line.startsWith("-");
      if (!partOfBlock) {
        break;
      }
      if (!line.trim().startsWith("#")) {
        lastContent = i;
      }
    }
    return lastContent + 1;
  }

  /** Refuse to save a file that no longer parses, or that lost the entry we just added. */
  private static void verify(String updated, String key, String expected) throws IOException {
    Object parsed;
    try {
      parsed = new Yaml().load(updated);
    } catch (Exception e) {
      throw new IOException(
          "Editing hop-lint.yml would have made it unreadable: " + e.getMessage(), e);
    }
    if (!(parsed instanceof Map)) {
      throw new IOException("hop-lint.yml is not a YAML mapping, add the entry by hand");
    }
    Object block = ((Map<?, ?>) parsed).get(key);
    if (!(block instanceof List) || !((List<?>) block).toString().contains(expected)) {
      throw new IOException("The " + key + " entry did not survive the edit, add it by hand");
    }
  }

  /** Values are quoted rather than written bare: a pattern like {@code *.hpl} is not valid YAML. */
  private static String quote(String value) {
    return "\"" + singleLine(value).replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  private static String singleLine(String value) {
    return value.replace("\r", " ").replace("\n", " ").trim();
  }
}
