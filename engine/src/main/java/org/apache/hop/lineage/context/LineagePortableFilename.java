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

package org.apache.hop.lineage.context;

import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;

/**
 * Derives a cross-environment logical path for pipeline/workflow filenames when Hop projects use
 * {@code ${PROJECT_HOME}/relative/path.hpl} (see {@code ProjectsUtil.VARIABLE_PROJECT_HOME} in the
 * projects plugin).
 *
 * <p>Returns {@code null} when {@code PROJECT_HOME} is unset, the path is not under the resolved
 * project home, or the value cannot be resolved.
 */
public final class LineagePortableFilename {

  /**
   * Hop variable name for the active project directory (same as {@code
   * ProjectsUtil.VARIABLE_PROJECT_HOME}).
   */
  public static final String VARIABLE_PROJECT_HOME = "PROJECT_HOME";

  private static final String TOKEN_PROJECT_HOME = "${PROJECT_HOME}";

  private LineagePortableFilename() {}

  /**
   * Portable key suitable for correlating the same artifact across machines (e.g. OpenLineage job
   * namespace), or {@code null} if not derivable.
   *
   * @param filenameFromMeta filename as stored on the engine or meta (may contain variables)
   * @param variables execution variables (must expose {@code PROJECT_HOME} when used)
   */
  public static String portableKey(String filenameFromMeta, IVariables variables) {
    if (Utils.isEmpty(filenameFromMeta) || variables == null) {
      return null;
    }
    String stored = filenameFromMeta.trim();
    if (stored.contains(TOKEN_PROJECT_HOME)) {
      return normalizeKeySlashes(stored);
    }
    String resolved;
    try {
      resolved = variables.resolve(stored);
    } catch (Exception ignored) {
      return null;
    }
    if (Utils.isEmpty(resolved)) {
      return null;
    }
    resolved = resolved.trim();
    String projectHome = variables.resolve(TOKEN_PROJECT_HOME);
    if (Utils.isEmpty(projectHome) || TOKEN_PROJECT_HOME.equals(projectHome)) {
      return null;
    }
    projectHome = stripTrailingSeparators(projectHome.trim());
    if (Utils.isEmpty(projectHome)) {
      return null;
    }
    if (resolved.startsWith(projectHome)) {
      String rel = resolved.substring(projectHome.length());
      rel = rel.replace('\\', '/');
      if (!rel.startsWith("/")) {
        rel = "/" + rel;
      }
      return TOKEN_PROJECT_HOME + rel;
    }
    return null;
  }

  private static String stripTrailingSeparators(String path) {
    String p = path;
    while (p.endsWith("/") || p.endsWith("\\")) {
      p = p.substring(0, p.length() - 1);
    }
    return p;
  }

  private static String normalizeKeySlashes(String key) {
    int idx = key.indexOf(TOKEN_PROJECT_HOME);
    if (idx < 0) {
      return key.replace('\\', '/');
    }
    String prefix = key.substring(0, idx + TOKEN_PROJECT_HOME.length());
    String suffix = key.substring(idx + TOKEN_PROJECT_HOME.length()).replace('\\', '/');
    return prefix + suffix;
  }
}
