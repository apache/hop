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

import java.io.File;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Normalizes Hop file paths so lint results match open editor filenames (local paths, VFS URIs).
 */
public final class LintPathUtils {

  private LintPathUtils() {}

  public static String normalizePath(String path) {
    if (Utils.isEmpty(path)) {
      return "";
    }

    String trimmed = path.trim();

    try {
      FileObject fileObject = HopVfs.getFileObject(trimmed);
      if (fileObject != null) {
        try {
          return fileObject.getName().getPath();
        } finally {
          fileObject.close();
        }
      }
    } catch (Exception ignored) {
      // Fall back to local file normalization below.
    }

    try {
      if (trimmed.startsWith("file:")) {
        java.net.URI uri = java.net.URI.create(trimmed);
        return new File(uri).getAbsolutePath();
      }
      return new File(trimmed).getAbsolutePath();
    } catch (Exception e) {
      return trimmed;
    }
  }

  public static boolean pathsMatch(String a, String b) {
    if (Utils.isEmpty(a) || Utils.isEmpty(b)) {
      return false;
    }

    String normalizedA = normalizePath(a);
    String normalizedB = normalizePath(b);

    if (normalizedA.equals(normalizedB)) {
      return true;
    }

    return normalizedA.equalsIgnoreCase(normalizedB);
  }

  /** Explorer passes the full VFS path as {@code path} and the basename as {@code name}. */
  public static String resolveExplorerFilePath(String path, String name) {
    if (Utils.isEmpty(path)) {
      return normalizePath(name);
    }

    String normalizedPath = normalizePath(path);
    if (!Utils.isEmpty(name)) {
      String lower = normalizedPath.toLowerCase();
      if (lower.endsWith(".hpl") || lower.endsWith(".hwf")) {
        return normalizedPath;
      }
      if (normalizedPath.endsWith("/" + name) || normalizedPath.endsWith("\\" + name)) {
        return normalizedPath;
      }
    }

    if (path.endsWith("/") || path.endsWith("\\")) {
      return normalizePath(path + name);
    }
    return normalizePath(path + "/" + name);
  }
}
