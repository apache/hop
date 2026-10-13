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

package org.apache.hop.ui.hopgui.vfs.explorer;

/** Path rules for the file explorer that do not touch VFS or SWT. */
public final class VfsLocations {

  private VfsLocations() {}

  /**
   * Folder to open for a selection in another tree. A folder is used as it is. A file opens its
   * parent.
   */
  public static String folderToBrowse(String path, boolean folder) {
    if (path == null || path.isBlank()) {
      return null;
    }
    if (folder) {
      return path;
    }
    return parentOf(path);
  }

  /** Parent folder of a file path, or {@code null} when the path has no parent segment. */
  public static String parentOf(String path) {
    if (path == null || path.isBlank()) {
      return null;
    }
    String stripped = stripTrailingSeparator(path);
    int slash = Math.max(stripped.lastIndexOf('/'), stripped.lastIndexOf('\\'));
    if (slash < 0) {
      return null;
    }
    if (slash == 0) {
      return stripped.substring(0, 1);
    }
    String parent = stripped.substring(0, slash);
    int scheme = parent.indexOf("://");
    if (scheme >= 0 && parent.length() == scheme + 2) {
      return parent + "/";
    }
    return parent;
  }

  /** True when both name the same folder, also when one is a plain path and the other a URI. */
  public static boolean sameLocation(String left, String right) {
    return normalize(left).equals(normalize(right));
  }

  /** True when {@code childUri} is strictly inside {@code parentUri}. */
  public static boolean isUnder(String parentUri, String childUri) {
    String parent = normalize(parentUri);
    String child = normalize(childUri);
    if (parent.isEmpty() || child.isEmpty() || parent.equals(child)) {
      return false;
    }
    return child.startsWith(parent.endsWith("/") ? parent : parent + "/");
  }

  /**
   * {@code location} written relative to a variable, for example {@code
   * ${PROJECT_HOME}/data/in.csv}.
   *
   * @param location the file or folder
   * @param variable the variable name, without {@code ${}}
   * @param value the resolved value of the variable
   * @return the relative form, or null when {@code location} is not {@code value} or inside it
   */
  public static String relativeTo(String location, String variable, String value) {
    if (location == null || location.isBlank() || value == null || value.isBlank()) {
      return null;
    }
    String expression = "${" + variable + "}";
    if (sameLocation(location, value)) {
      return expression;
    }
    if (!isUnder(value, location)) {
      return null;
    }
    String rest = normalize(location).substring(normalize(value).length());
    return expression + (rest.startsWith("/") ? rest : "/" + rest);
  }

  /**
   * A comparable form: a local path such as {@code /home/x} or {@code C:\\x} becomes the {@code
   * file://} URI VFS reports for it, separators become {@code /}, and a trailing separator is
   * dropped. Does not touch the file system.
   */
  static String normalize(String location) {
    String value = stripTrailingSeparator(location == null ? "" : location.trim());
    if (value.isEmpty()) {
      return "";
    }
    if (value.length() >= 2
        && Character.isLetter(value.charAt(0))
        && value.charAt(1) == ':'
        && (value.length() == 2 || value.charAt(2) == '\\' || value.charAt(2) == '/')) {
      return "file:///" + value.replace('\\', '/');
    }
    if (value.startsWith("/")) {
      return "file://" + value;
    }
    if (value.regionMatches(true, 0, "file:", 0, 5)) {
      return value.replace('\\', '/');
    }
    return value;
  }

  public static String stripTrailingSeparator(String value) {
    if (value == null) {
      return "";
    }
    String stripped = value;
    while (stripped.length() > 1 && (stripped.endsWith("/") || stripped.endsWith("\\"))) {
      stripped = stripped.substring(0, stripped.length() - 1);
    }
    return stripped;
  }
}
