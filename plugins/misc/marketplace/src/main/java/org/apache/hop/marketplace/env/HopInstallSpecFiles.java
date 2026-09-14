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

package org.apache.hop.marketplace.env;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/** Well-known names and VFS helpers for Hop install spec files. */
public final class HopInstallSpecFiles {

  public static final String DEFAULT_FILENAME = "hop-env.yaml";
  public static final String FULL_CLIENT_FILENAME = "full-client-env.yaml";

  public static final List<String> WELL_KNOWN_NAMES =
      List.of("hop-env.yaml", "hop-env.yml", "hop-env.json", FULL_CLIENT_FILENAME);

  /** A VFS scheme: letter, then letters/digits/+/-/. up to a colon. */
  private static final Pattern SCHEME = Pattern.compile("^[A-Za-z][A-Za-z0-9+.-]*:");

  private HopInstallSpecFiles() {}

  public static String resolve(String filename, IVariables variables) {
    if (filename == null) {
      return null;
    }
    String trimmed = filename.trim();
    return variables != null ? variables.resolve(trimmed) : trimmed;
  }

  /**
   * Resolve a spec file reference for lookup, anchoring a relative reference at the project home.
   *
   * <p>{@link #resolve(String, IVariables)} only expands variables. A reference that is still
   * relative afterwards is handed to VFS, which anchors it at {@code user.dir} — the Hop
   * installation directory for a launched Hop GUI — so {@code config/hop-env.yaml} configured on an
   * environment points into the install instead of the project (issue #8012). Anchor it at the
   * project home instead whenever one is known.
   *
   * @param filename the configured reference, may be null, blank, relative, or contain variables
   * @param variables used to expand variables, may be null
   * @param projectHome the project home to anchor at; when blank the {@code PROJECT_HOME} variable
   *     is used
   * @return the resolved reference, unchanged when it is absolute or no project home is known
   */
  public static String resolveInProject(String filename, IVariables variables, String projectHome) {
    String resolved = resolve(filename, variables);
    if (StringUtils.isBlank(resolved) || !isRelative(resolved)) {
      return resolved;
    }
    String home =
        StringUtils.isNotBlank(projectHome)
            ? resolve(projectHome, variables)
            : (variables != null
                ? resolve(variables.getVariable("PROJECT_HOME"), variables)
                : null);
    if (StringUtils.isBlank(home) || isRelative(home)) {
      return resolved;
    }
    String separator = home.endsWith("/") || home.endsWith("\\") ? "" : "/";
    return home + separator + resolved;
  }

  /**
   * Whether a reference still needs a base to be meaningful: no VFS scheme, no leading separator
   * and no Windows drive letter.
   */
  static boolean isRelative(String filename) {
    if (StringUtils.isBlank(filename)) {
      return false;
    }
    String name = filename.trim();
    if (name.startsWith("/") || name.startsWith("\\")) {
      return false;
    }
    // A single leading letter followed by a colon is a Windows drive, not a scheme.
    if (name.length() >= 2 && Character.isLetter(name.charAt(0)) && name.charAt(1) == ':') {
      return false;
    }
    return !SCHEME.matcher(name).find();
  }

  public static String baseName(String filename) {
    if (StringUtils.isBlank(filename)) {
      return "";
    }
    String name = filename.trim();
    int query = name.indexOf('?');
    if (query >= 0) {
      name = name.substring(0, query);
    }
    int slash = Math.max(name.lastIndexOf('/'), name.lastIndexOf('\\'));
    if (slash >= 0 && slash < name.length() - 1) {
      return name.substring(slash + 1);
    }
    if (slash >= 0) {
      return "";
    }
    return name;
  }

  public static boolean isWellKnown(String filename) {
    return WELL_KNOWN_NAMES.contains(baseName(filename));
  }

  public static boolean isFullClient(String filename) {
    return FULL_CLIENT_FILENAME.equalsIgnoreCase(baseName(filename));
  }

  public static boolean exists(String filename, IVariables variables) {
    if (StringUtils.isBlank(filename)) {
      return false;
    }
    try {
      String resolved = resolve(filename, variables);
      FileObject fileObject = HopVfs.getFileObject(resolved, variables);
      return fileObject != null && fileObject.exists() && fileObject.isFile();
    } catch (Exception e) {
      return false;
    }
  }

  public static String ensureSpecExtension(String filename) {
    if (StringUtils.isBlank(filename)) {
      return DEFAULT_FILENAME;
    }
    String name = baseName(filename).toLowerCase(Locale.ROOT);
    if (name.endsWith(".yaml") || name.endsWith(".yml") || name.endsWith(".json")) {
      return filename;
    }
    return filename + ".yaml";
  }

  public static String defaultSaveFolder(IVariables variables, Path hopHome) {
    String projectHome = variables != null ? variables.getVariable("PROJECT_HOME") : null;
    if (StringUtils.isNotBlank(projectHome)) {
      return resolve(projectHome, variables);
    }
    if (hopHome != null) {
      return hopHome.toString();
    }
    return ".";
  }
}
