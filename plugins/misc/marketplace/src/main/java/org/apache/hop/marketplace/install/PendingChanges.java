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

package org.apache.hop.marketplace.install;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;

/**
 * File changes the marketplace could not make while Hop was running, because a file was in use.
 * Windows keeps the jars of a running JVM locked, so a jar on the classpath can be neither replaced
 * nor deleted until Hop stops.
 *
 * <p>The changes are kept in a plain text file, {@value #PENDING_FILE}, one per line:
 *
 * <pre>
 * copy|plugins/.marketplace/pending-files/&lt;name&gt;.pending|lib/core/example-2.0.jar
 * delete|plugins/tech/example/example-1.0.jar
 * </pre>
 *
 * <p>Paths are relative to the Hop installation (the folder that holds {@code plugins/}) and use
 * {@code /}. Lines starting with {@code #} are comments. Every Hop launcher applies the list before
 * it starts Java ({@code lib/apply-pending-changes.sh} and {@code .cmd}): this cannot be done from
 * Java, because by then the jars are loaded. A change that still fails stays in the list for the
 * next start.
 *
 * <p>The copy sources end in {@value #SOURCE_SUFFIX}, not {@code .jar}: the plugin registry scans
 * every folder under {@code plugins/}, dot-folders included, and must not load them.
 */
public final class PendingChanges {

  public static final String PENDING_FILE = PluginInstaller.RECEIPTS_DIR + "/pending";
  public static final String SOURCES_DIR = PluginInstaller.RECEIPTS_DIR + "/pending-files";
  public static final String SOURCE_SUFFIX = ".pending";

  private static final String SEPARATOR = "|";
  private static final String UNSAFE_CHARACTERS = "\\:|%!^\"*?";

  /** Where a pending change may write. The launchers check the same prefixes. */
  private static final List<String> TARGET_PREFIXES =
      List.of("plugins/", "lib/", "webapps/ROOT/WEB-INF/lib/");

  private static final List<String> HEADER =
      List.of(
          "# Plugin file changes the Hop marketplace could not make while Hop was running.",
          "# The Hop launchers apply them before Java starts (lib/apply-pending-changes.sh/.cmd).",
          "# copy|<source>|<target> or delete|<target>, relative to the Hop installation.");

  @Getter
  public enum Action {
    COPY("copy"),
    DELETE("delete");

    private final String code;

    Action(String code) {
      this.code = code;
    }
  }

  /**
   * One pending change.
   *
   * @param source for a copy, the file to copy; null for a delete
   * @param target the file to replace, create or delete
   */
  public record Change(Action action, String source, String target) {

    String toLine() {
      return action == Action.COPY
          ? action.getCode() + SEPARATOR + source + SEPARATOR + target
          : action.getCode() + SEPARATOR + target;
    }

    /** The line as a change, or null when it is a comment, blank or not a valid change. */
    static Change parse(String line) {
      if (StringUtils.isBlank(line) || line.startsWith("#")) {
        return null;
      }
      String[] parts = line.split("\\|", -1);
      if (parts.length == 2 && Action.DELETE.getCode().equals(parts[0])) {
        return isValidTarget(parts[1]) ? new Change(Action.DELETE, null, parts[1]) : null;
      }
      if (parts.length == 3 && Action.COPY.getCode().equals(parts[0])) {
        return isValidSource(parts[1]) && isValidTarget(parts[2])
            ? new Change(Action.COPY, parts[1], parts[2])
            : null;
      }
      return null;
    }
  }

  private final Path hopHome;

  public PendingChanges(Path hopHome) {
    this.hopHome = hopHome.toAbsolutePath().normalize();
  }

  /** The valid changes in the list, in the order the launchers apply them. */
  public List<Change> read() throws HopException {
    List<Change> changes = new ArrayList<>();
    for (String line : readLines()) {
      Change change = Change.parse(line);
      if (change != null) {
        changes.add(change);
      }
    }
    return changes;
  }

  public boolean isEmpty() throws HopException {
    return read().isEmpty();
  }

  /**
   * Copy {@code file} over {@code target} the next time Hop starts. The file is copied into the
   * pending folder now, so it does not have to stay where it is. Replaces any earlier pending
   * change for the same target.
   */
  public synchronized void queueCopy(Path file, Path target) throws HopException {
    String relativeTarget = relativeTarget(target);
    Path sources = hopHome.resolve(SOURCES_DIR);
    String source;
    try {
      Files.createDirectories(sources);
      Path copy = newSourceFile(sources, target.getFileName().toString());
      Files.copy(file, copy, StandardCopyOption.REPLACE_EXISTING);
      source = SOURCES_DIR + "/" + copy.getFileName();
    } catch (IOException e) {
      throw new HopException("Unable to keep a copy of " + file + " for the next Hop start", e);
    }
    queue(new Change(Action.COPY, source, relativeTarget));
  }

  /**
   * Delete {@code target} the next time Hop starts. Replaces any earlier pending change for the
   * same target.
   */
  public synchronized void queueDelete(Path target) throws HopException {
    queue(new Change(Action.DELETE, null, relativeTarget(target)));
  }

  /**
   * Drop the pending changes for {@code target}, for example when a later install could write the
   * file after all. Nothing happens for a file the list cannot hold.
   */
  public synchronized void forget(Path target) throws HopException {
    Path absolute = target.toAbsolutePath().normalize();
    if (!absolute.startsWith(hopHome) || !Files.isRegularFile(hopHome.resolve(PENDING_FILE))) {
      return;
    }
    String relativeTarget = hopHome.relativize(absolute).toString().replace('\\', '/');
    List<Change> changes = read();
    List<Change> kept = changes.stream().filter(c -> !c.target().equals(relativeTarget)).toList();
    if (kept.size() != changes.size()) {
      write(kept);
      deleteUnusedSources(changes, kept);
    }
  }

  /** What a pending change does to its target, for the restart message. */
  @Getter
  public enum Effect {
    ADD("Add"),
    REPLACE("Replace"),
    REMOVE("Remove");

    private final String label;

    Effect(String label) {
      this.label = label;
    }
  }

  public record PendingEffect(Effect effect, String target) {}

  /** What the next Hop start changes, and where. */
  public List<PendingEffect> effects() throws HopException {
    List<PendingEffect> effects = new ArrayList<>();
    for (Change change : read()) {
      Effect effect;
      if (change.action() == Action.DELETE) {
        effect = Effect.REMOVE;
      } else {
        effect = Files.exists(hopHome.resolve(change.target())) ? Effect.REPLACE : Effect.ADD;
      }
      effects.add(new PendingEffect(effect, change.target()));
    }
    return effects;
  }

  /** {@link #effects()} as English lines, for the command line. */
  public List<String> describe() throws HopException {
    return effects().stream().map(e -> e.effect().getLabel() + " " + e.target()).toList();
  }

  private void queue(Change change) throws HopException {
    List<Change> changes = read();
    List<Change> kept = new ArrayList<>();
    for (Change existing : changes) {
      if (!existing.target().equals(change.target())) {
        kept.add(existing);
      }
    }
    kept.add(change);
    write(kept);
    deleteUnusedSources(changes, kept);
  }

  /**
   * The target relative to the Hop installation, or an exception when a launcher cannot write it.
   */
  String relativeTarget(Path target) throws HopException {
    Path absolute = target.toAbsolutePath().normalize();
    if (!absolute.startsWith(hopHome)) {
      throw new HopException(
          absolute + " is outside the Hop installation " + hopHome + ": change it by hand.");
    }
    String relative = hopHome.relativize(absolute).toString().replace('\\', '/');
    if (!isValidTarget(relative)) {
      throw new HopException(
          relative + " is not a plugin or library file the Hop launchers can change.");
    }
    return relative;
  }

  /**
   * Relative, no {@code ..}, under plugins/ (but not the marketplace folder) or a library folder.
   */
  static boolean isValidTarget(String path) {
    if (!isSafeRelativePath(path)) {
      return false;
    }
    if (path.startsWith(PluginInstaller.RECEIPTS_DIR + "/")) {
      return false;
    }
    return TARGET_PREFIXES.stream().anyMatch(path::startsWith);
  }

  static boolean isValidSource(String path) {
    if (!isSafeRelativePath(path) || !path.startsWith(SOURCES_DIR + "/")) {
      return false;
    }
    String name = path.substring(SOURCES_DIR.length() + 1);
    return !name.contains("/") && name.endsWith(SOURCE_SUFFIX);
  }

  /**
   * The launchers parse these paths with plain shell and batch string handling, so the rules are
   * strict: no absolute paths, drive letters, backslashes, wildcards, characters cmd treats
   * specially, {@code ..}, control characters or non-ASCII characters. The list is UTF-8, and cmd
   * reads it in the console code page, so a non-ASCII name would point at another file.
   */
  private static boolean isSafeRelativePath(String path) {
    if (StringUtils.isBlank(path) || path.startsWith("/") || path.endsWith("/")) {
      return false;
    }
    for (char c : path.toCharArray()) {
      if (c < ' ' || c > '~' || UNSAFE_CHARACTERS.indexOf(c) >= 0) {
        return false;
      }
    }
    for (String segment : path.split("/", -1)) {
      if (segment.isEmpty() || segment.equals(".") || segment.contains("..")) {
        return false;
      }
    }
    return true;
  }

  private static Path newSourceFile(Path sources, String targetName) throws IOException {
    while (true) {
      Path candidate =
          sources.resolve(
              UUID.randomUUID().toString().substring(0, 8)
                  + "-"
                  + safeFileName(targetName)
                  + SOURCE_SUFFIX);
      try {
        return Files.createFile(candidate);
      } catch (FileAlreadyExistsException e) {
        // Try another name.
      }
    }
  }

  private static String safeFileName(String name) {
    return name.replaceAll("[^A-Za-z0-9._-]", "_");
  }

  private List<String> readLines() throws HopException {
    Path file = hopHome.resolve(PENDING_FILE);
    if (!Files.isRegularFile(file)) {
      return List.of();
    }
    try {
      return Files.readAllLines(file, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new HopException("Unable to read the pending marketplace changes in " + file, e);
    }
  }

  /**
   * Writes the list through a temporary file and a rename, so a launcher never reads half a list.
   * An empty list removes the file.
   */
  private void write(List<Change> changes) throws HopException {
    Path file = hopHome.resolve(PENDING_FILE);
    try {
      if (changes.isEmpty()) {
        Files.deleteIfExists(file);
        return;
      }
      Files.createDirectories(file.getParent());
      Path temporary = file.resolveSibling(file.getFileName() + ".tmp");
      StringBuilder content = new StringBuilder();
      for (String line : HEADER) {
        content.append(line).append('\n');
      }
      for (Change change : changes) {
        // Always \n: the shell launchers read the list as well.
        content.append(change.toLine()).append('\n');
      }
      Files.writeString(temporary, content, StandardCharsets.UTF_8);
      try {
        Files.move(
            temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (IOException e) {
      throw new HopException("Unable to write the pending marketplace changes to " + file, e);
    }
  }

  private void deleteUnusedSources(List<Change> before, List<Change> after) {
    Set<String> used = new HashSet<>();
    for (Change change : after) {
      if (change.source() != null) {
        used.add(change.source());
      }
    }
    for (Change change : before) {
      if (change.source() != null && !used.contains(change.source())) {
        try {
          Files.deleteIfExists(hopHome.resolve(change.source()));
        } catch (IOException e) {
          // A leftover copy does no harm: the launchers never read files the list does not name.
        }
      }
    }
  }
}
