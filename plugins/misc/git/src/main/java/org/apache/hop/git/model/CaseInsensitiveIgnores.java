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

package org.apache.hop.git.model;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hop.core.logging.LogChannel;
import org.eclipse.jgit.ignore.FastIgnoreRule;
import org.eclipse.jgit.ignore.IgnoreNode;
import org.eclipse.jgit.lib.ConfigConstants;
import org.eclipse.jgit.lib.Repository;

/**
 * Ignore rules matched without regard to case, for repositories on a case insensitive file system.
 *
 * <p>JGit matches .gitignore patterns case sensitively whatever <code>core.ignorecase</code> says,
 * so on macOS and Windows it reports files as untracked which git itself ignores: a rule like
 * <code>output/</code> does not catch a folder named <code>Output</code>. This runs the rules of
 * the repository again with both the patterns and the paths folded to lower case, the way git
 * matches them when <code>core.ignorecase</code> is on.
 */
class CaseInsensitiveIgnores {

  /** JGit has no constant for it: git writes this when the file system ignores case. */
  static final String CONFIG_KEY_IGNORECASE = "ignorecase";

  private final File workTree;

  /** The rules that apply to the whole repository: .git/info/exclude and core.excludesFile. */
  private final IgnoreNode repositoryNode;

  /** The .gitignore of a directory, relative to the work tree, "" being the root. */
  private final Map<String, IgnoreNode> directoryNodes = new HashMap<>();

  /**
   * Whether the ignore rules of this repository have to be matched without regard to case. Only
   * repositories that say they are on a case insensitive file system need it: elsewhere git matches
   * case sensitively, exactly like JGit.
   */
  static boolean appliesTo(Repository repository) {
    return repository
        .getConfig()
        .getBoolean(ConfigConstants.CONFIG_CORE_SECTION, CONFIG_KEY_IGNORECASE, false);
  }

  CaseInsensitiveIgnores(Repository repository) {
    this.workTree = repository.getWorkTree();

    List<FastIgnoreRule> rules = new ArrayList<>();
    rules.addAll(readRules(new File(repository.getDirectory(), "info/exclude")));
    String excludesFile =
        repository
            .getConfig()
            .getString(
                ConfigConstants.CONFIG_CORE_SECTION, null, ConfigConstants.CONFIG_KEY_EXCLUDESFILE);
    if (excludesFile != null && !excludesFile.isBlank()) {
      rules.addAll(readRules(new File(replaceUserHome(excludesFile))));
    }
    this.repositoryNode = new IgnoreNode(rules);
  }

  /**
   * Whether git ignores this file. A file is ignored when it matches a rule itself, or when any of
   * the directories above it is ignored: git never descends into an ignored directory, so nothing
   * inside it can be brought back by a later rule.
   *
   * @param path the path of the file, relative to the work tree, with forward slashes
   */
  boolean isIgnored(String path) {
    for (int slash = path.indexOf('/'); slash > 0; slash = path.indexOf('/', slash + 1)) {
      if (Boolean.TRUE.equals(checkPath(path.substring(0, slash), true))) {
        return true;
      }
    }
    return Boolean.TRUE.equals(checkPath(path, false));
  }

  /**
   * Ask the .gitignore files whether this path is ignored, closest one first: the rules of a
   * directory win from the ones above it. Null means that no rule had anything to say about it.
   */
  private Boolean checkPath(String path, boolean isDirectory) {
    String foldedPath = path.toLowerCase(Locale.ROOT);

    int slash = foldedPath.lastIndexOf('/');
    while (slash >= 0) {
      String directory = foldedPath.substring(0, slash);
      Boolean ignored =
          nodeFor(directory).checkIgnored(foldedPath.substring(slash + 1), isDirectory);
      if (ignored != null) {
        return ignored;
      }
      slash = foldedPath.lastIndexOf('/', slash - 1);
    }

    Boolean ignored = nodeFor("").checkIgnored(foldedPath, isDirectory);
    return ignored != null ? ignored : repositoryNode.checkIgnored(foldedPath, isDirectory);
  }

  private IgnoreNode nodeFor(String directory) {
    return directoryNodes.computeIfAbsent(
        directory,
        dir ->
            new IgnoreNode(
                readRules(
                    new File(dir.isEmpty() ? workTree : new File(workTree, dir), ".gitignore"))));
  }

  /** The rules of one ignore file, folded to lower case so they match paths of any case. */
  private List<FastIgnoreRule> readRules(File ignoreFile) {
    List<FastIgnoreRule> rules = new ArrayList<>();
    if (!ignoreFile.isFile()) {
      return rules;
    }
    try {
      for (String line : Files.readAllLines(ignoreFile.toPath(), StandardCharsets.UTF_8)) {
        String pattern = line.trim();
        if (!pattern.isEmpty() && !pattern.startsWith("#")) {
          rules.add(new FastIgnoreRule(pattern.toLowerCase(Locale.ROOT)));
        }
      }
    } catch (Exception e) {
      // An ignore file we can't read simply holds no rules, the same as JGit does
      LogChannel.UI.logDebug("Unable to read ignore file '" + ignoreFile + "': " + e.getMessage());
    }
    return rules;
  }

  private static String replaceUserHome(String path) {
    if (path.startsWith("~/")) {
      return System.getProperty("user.home") + path.substring(1);
    }
    return path;
  }
}
