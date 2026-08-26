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

  private HopInstallSpecFiles() {}

  public static String resolve(String filename, IVariables variables) {
    if (filename == null) {
      return null;
    }
    String trimmed = filename.trim();
    return variables != null ? variables.resolve(trimmed) : trimmed;
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
