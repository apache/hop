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

package org.apache.hop.projects.util;

import java.util.Locale;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Rewrites absolute paths to use the most specific matching path-like variable from the active
 * variable space (for example {@code ${PROJECT_HOME}/file.csv} or {@code
 * ${SOURCE_FILES}/input.csv}).
 *
 * <p>Used by Hop GUI file/directory browse extension points as a best-practice aid.
 */
public final class PathVariableReplacer {

  private static final Set<String> SYSTEM_NAME_PREFIXES =
      Set.of(
          "java.", "sun.", "user.", "os.", "file.", "jdk.", "http.", "awt.", "path.", "line.",
          "ftp.");

  private static final Set<String> NON_PATH_VARIABLE_NAMES =
      Set.of(
          Defaults.VARIABLE_HOP_PROJECT_NAME,
          Defaults.VARIABLE_HOP_ENVIRONMENT_NAME,
          ProjectsUtil.VARIABLE_PARENT_PROJECT_NAME);

  private PathVariableReplacer() {
    // Utility class
  }

  /**
   * If {@code path} is under one or more path-like variable values in {@code variables}, rewrite
   * the longest matching prefix to {@code ${NAME}} or {@code ${NAME}/remainder}.
   *
   * @param variables active variables (may be null)
   * @param path selected file or directory path (may be null/empty)
   * @return rewritten path, or the original path when no replacement applies
   */
  public static String replacePathWithVariable(IVariables variables, String path) {
    if (variables == null || StringUtils.isEmpty(path)) {
      return path;
    }

    try {
      String absolutePath = normalizeAbsolutePath(path);
      if (StringUtils.isEmpty(absolutePath)) {
        return path;
      }

      PathMatch best = null;
      for (String name : variables.getVariableNames()) {
        if (!isCandidateVariableName(name)) {
          continue;
        }
        String rawValue = variables.getVariable(name);
        if (StringUtils.isEmpty(rawValue)) {
          continue;
        }
        String resolved = variables.resolve(rawValue);
        if (!isCandidatePathValue(resolved)) {
          continue;
        }

        String absoluteRoot;
        try {
          absoluteRoot = normalizeAbsolutePath(resolved);
        } catch (Exception e) {
          continue;
        }
        if (!isUsablePathRoot(absoluteRoot)) {
          continue;
        }

        if (absolutePath.equals(absoluteRoot)) {
          PathMatch match = new PathMatch(name, absoluteRoot, "");
          best = betterMatch(best, match);
        } else if (absolutePath.startsWith(absoluteRoot + "/")) {
          String remainder = absolutePath.substring(absoluteRoot.length() + 1);
          PathMatch match = new PathMatch(name, absoluteRoot, remainder);
          best = betterMatch(best, match);
        }
      }

      if (best == null) {
        return path;
      }
      if (StringUtils.isEmpty(best.remainder)) {
        return "${" + best.variableName + "}";
      }
      return "${" + best.variableName + "}/" + best.remainder;
    } catch (Exception e) {
      return path;
    }
  }

  /**
   * Whether a variable name is eligible to be used as a path replacement root.
   *
   * @param name variable name
   * @return true if the name may be considered
   */
  public static boolean isCandidateVariableName(String name) {
    if (StringUtils.isBlank(name)) {
      return false;
    }
    if (name.startsWith(Const.INTERNAL_VARIABLE_PREFIX)) {
      return false;
    }
    if (NON_PATH_VARIABLE_NAMES.contains(name)) {
      return false;
    }
    String lower = name.toLowerCase(Locale.ROOT);
    for (String prefix : SYSTEM_NAME_PREFIXES) {
      if (lower.startsWith(prefix)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Whether a resolved variable value looks like a usable path root before VFS normalization.
   *
   * @param value resolved variable value
   * @return true if the value may be a path
   */
  public static boolean isCandidatePathValue(String value) {
    if (StringUtils.isBlank(value)) {
      return false;
    }
    // Incomplete resolution or multi-folder lists (e.g. HOP_METADATA_FOLDER parent,child)
    if (value.contains("${") || value.contains(",")) {
      return false;
    }
    // Absolute local path, Windows drive path, UNC, or VFS URI
    if (value.startsWith("/") || value.startsWith("\\")) {
      return true;
    }
    if (value.length() >= 3
        && Character.isLetter(value.charAt(0))
        && value.charAt(1) == ':'
        && (value.charAt(2) == '/' || value.charAt(2) == '\\')) {
      return true;
    }
    // VFS / URL style: scheme://
    int schemeSep = value.indexOf("://");
    if (schemeSep > 0) {
      return true;
    }
    // Relative paths and bare names are not used as replacement roots
    return false;
  }

  static boolean isUsablePathRoot(String absoluteRoot) {
    if (StringUtils.isEmpty(absoluteRoot)) {
      return false;
    }
    // Too broad: filesystem root only
    if ("/".equals(absoluteRoot) || absoluteRoot.matches("^[A-Za-z]:$")) {
      return false;
    }
    // Windows root like C:/ or C:\
    if (absoluteRoot.matches("^[A-Za-z]:[\\\\/]$")) {
      return false;
    }
    return true;
  }

  static String normalizeAbsolutePath(String path) throws Exception {
    FileObject file = HopVfs.getFileObject(path);
    String absolute = file.getName().getPath();
    // Strip trailing slash except for root
    while (absolute.length() > 1 && absolute.endsWith("/")) {
      absolute = absolute.substring(0, absolute.length() - 1);
    }
    return absolute;
  }

  private static PathMatch betterMatch(PathMatch current, PathMatch candidate) {
    if (current == null) {
      return candidate;
    }
    int lengthCompare = Integer.compare(candidate.rootLength(), current.rootLength());
    if (lengthCompare > 0) {
      return candidate;
    }
    if (lengthCompare < 0) {
      return current;
    }
    // Equal length: prefer PROJECT_HOME, then stable name order
    if (ProjectsUtil.VARIABLE_PROJECT_HOME.equals(candidate.variableName)
        && !ProjectsUtil.VARIABLE_PROJECT_HOME.equals(current.variableName)) {
      return candidate;
    }
    if (ProjectsUtil.VARIABLE_PROJECT_HOME.equals(current.variableName)
        && !ProjectsUtil.VARIABLE_PROJECT_HOME.equals(candidate.variableName)) {
      return current;
    }
    return candidate.variableName.compareTo(current.variableName) < 0 ? candidate : current;
  }

  private static final class PathMatch {
    final String variableName;
    final String absoluteRoot;
    final String remainder;

    PathMatch(String variableName, String absoluteRoot, String remainder) {
      this.variableName = variableName;
      this.absoluteRoot = absoluteRoot;
      this.remainder = remainder;
    }

    int rootLength() {
      return absoluteRoot.length();
    }
  }
}
