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

package org.apache.hop.ui.hopgui.perspective.explorer;

/**
 * Path helpers for the file explorer tree (separator normalization, equality). Package-private so
 * unit tests can cover Windows/Unix path friction without loading SWT.
 */
final class ExplorerPathUtils {

  private ExplorerPathUtils() {}

  /**
   * True when both paths refer to the same location after normalizing separators and trailing
   * slashes. Null-safe ({@code null} equals only {@code null}).
   */
  static boolean pathsEqual(String a, String b) {
    if (a == null || b == null) {
      return a == b;
    }
    if (a.equals(b)) {
      return true;
    }
    return normalizePath(a).equals(normalizePath(b));
  }

  /**
   * Normalize path separators to {@code /} and strip trailing slashes (except a single root {@code
   * /}).
   */
  static String normalizePath(String path) {
    if (path == null) {
      return null;
    }
    String n = path.replace('\\', '/');
    while (n.length() > 1 && n.endsWith("/")) {
      n = n.substring(0, n.length() - 1);
    }
    return n;
  }
}
