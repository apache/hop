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

  /** True when {@code childUri} is strictly inside {@code parentUri}. */
  public static boolean isUnder(String parentUri, String childUri) {
    String parent = stripTrailingSeparator(parentUri);
    String child = stripTrailingSeparator(childUri);
    if (parent.isEmpty() || child.isEmpty() || parent.equals(child)) {
      return false;
    }
    return child.startsWith(parent + "/") || child.startsWith(parent + "\\");
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
