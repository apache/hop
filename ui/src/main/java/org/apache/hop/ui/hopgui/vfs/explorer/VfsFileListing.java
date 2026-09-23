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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.search.SearchMatcher;
import org.apache.hop.core.util.Utils;

/** Lists one folder and presents those rows. One {@code getChildren()} call, no content reads. */
public final class VfsFileListing {

  private VfsFileListing() {}

  public static List<VfsFileRow> childrenOf(FileObject folder) throws FileSystemException {
    FileObject[] children = folder.getChildren();
    if (children == null || children.length == 0) {
      return List.of();
    }
    List<VfsFileRow> rows = new ArrayList<>(children.length);
    for (FileObject child : children) {
      if (child == null) {
        continue;
      }
      try {
        rows.add(VfsFileDetails.read(child));
      } catch (Exception ignored) {
        // One unreadable child does not hide the rest of the folder.
      }
    }
    return rows;
  }

  /**
   * Folders for the left-hand tree, in case-insensitive name order. Hidden folders (names starting
   * with {@code .}) are omitted unless {@code showHiddenFolders} is set. Files are not included.
   */
  public static List<VfsFileRow> foldersForTree(List<VfsFileRow> rows, boolean showHiddenFolders) {
    List<VfsFileRow> folders = new ArrayList<>();
    if (rows != null) {
      for (VfsFileRow row : rows) {
        if (row == null || !row.isFolder()) {
          continue;
        }
        if (isHiddenName(row.getName()) && !showHiddenFolders) {
          continue;
        }
        folders.add(row);
      }
    }
    folders.sort((left, right) -> compare(left, right, VfsFileColumn.NAME, true));
    return folders;
  }

  /**
   * Drop hidden names and names that miss the filter, then sort. Folders stay ahead of files.
   * Hidden names start with {@code .}. Folders and files are hidden independently. The filter is
   * the file dialog's current-folder matcher.
   */
  public static List<VfsFileRow> visible(
      List<VfsFileRow> rows,
      boolean showHiddenFolders,
      boolean showHiddenFiles,
      String filter,
      VfsFileColumn column,
      boolean ascending) {
    SearchMatcher matcher =
        new SearchMatcher(filter == null ? "" : filter.trim(), false, false, true);
    boolean filtering = !Utils.isEmpty(filter);
    List<VfsFileRow> kept = new ArrayList<>();
    if (rows != null) {
      for (VfsFileRow row : rows) {
        if (row == null) {
          continue;
        }
        if (isHiddenName(row.getName())
            && ((row.isFolder() && !showHiddenFolders) || (!row.isFolder() && !showHiddenFiles))) {
          continue;
        }
        if (filtering && !matcher.matches(row.getName())) {
          continue;
        }
        kept.add(row);
      }
    }
    kept.sort((left, right) -> compare(left, right, column, ascending));
    return kept;
  }

  /**
   * Total order. A missing size or date does not compare equal to a different row: the name, URI
   * and row id break the tie. Folders stay first whichever way the column is sorted.
   */
  public static int compare(
      VfsFileRow left, VfsFileRow right, VfsFileColumn column, boolean ascending) {
    if (left == right) {
      return 0;
    }
    if (left == null) {
      return -1;
    }
    if (right == null) {
      return 1;
    }
    int folder = Boolean.compare(right.isFolder(), left.isFolder());
    if (folder != 0) {
      return folder;
    }
    int compared =
        switch (column == null ? VfsFileColumn.NAME : column) {
          case SIZE -> Long.compare(left.getSize(), right.getSize());
          case MODIFIED -> Long.compare(left.getLastModified(), right.getLastModified());
          case EXTENSION -> text(left.getExtension(), right.getExtension());
          case OWNER -> text(left.getOwner(), right.getOwner());
          case PERMISSIONS -> text(left.getPermissions(), right.getPermissions());
          default -> text(left.getName(), right.getName());
        };
    if (!ascending) {
      compared = -compared;
    }
    if (compared == 0 && column != VfsFileColumn.NAME) {
      compared = text(left.getName(), right.getName());
    }
    if (compared == 0) {
      compared = text(left.getUri(), right.getUri());
    }
    if (compared == 0) {
      compared = Integer.compare(left.getId(), right.getId());
    }
    return compared;
  }

  /** Keep {@code current} when {@code token} is no longer the listing this tab is waiting for. */
  public static List<VfsFileRow> publish(
      VfsListingGeneration generation,
      int token,
      List<VfsFileRow> current,
      List<VfsFileRow> incoming) {
    if (generation == null || !generation.isCurrent(token)) {
      return current == null ? List.of() : current;
    }
    return incoming == null ? List.of() : incoming;
  }

  /** A hidden name starts with {@code .}. */
  public static boolean isHiddenName(String name) {
    return name != null && name.startsWith(".");
  }

  private static int text(String left, String right) {
    String a = left == null ? "" : left;
    String b = right == null ? "" : right;
    int compared = a.compareToIgnoreCase(b);
    if (compared == 0) {
      compared = a.compareTo(b);
    }
    return compared;
  }
}
