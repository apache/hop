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

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Locale;
import lombok.Getter;
import org.apache.hop.i18n.BaseMessages;

/** How many rows are listed and selected, and the size of the files among them. */
@Getter
public final class VfsListingCounts {

  public static final Class<?> PKG = VfsFileExplorer.class;

  /** Nothing has been listed yet, so the status line omits the counts. */
  public static final VfsListingCounts NONE = new VfsListingCounts(-1, 0, 0, 0);

  private final int listed;
  private final long listedBytes;
  private final int selected;
  private final long selectedBytes;

  public VfsListingCounts(int listed, long listedBytes, int selected, long selectedBytes) {
    this.listed = listed;
    this.listedBytes = Math.max(0, listedBytes);
    this.selected = Math.max(0, selected);
    this.selectedBytes = Math.max(0, selectedBytes);
  }

  public static VfsListingCounts of(List<VfsFileRow> listed, List<VfsFileRow> selected) {
    List<VfsFileRow> rows = listed == null ? List.of() : listed;
    List<VfsFileRow> picked = selected == null ? List.of() : selected;
    return new VfsListingCounts(rows.size(), bytes(rows), picked.size(), bytes(picked));
  }

  /** Sum of known file sizes. Folders and missing sizes add nothing. */
  public static long bytes(List<VfsFileRow> rows) {
    long total = 0;
    if (rows == null) {
      return 0;
    }
    for (VfsFileRow row : rows) {
      if (row == null || row.isFolder() || row.getSize() < 0) {
        continue;
      }
      total += row.getSize();
    }
    return total;
  }

  /**
   * Compact size for the status line, without a space before the unit: {@code 0B}, {@code 120MB},
   * {@code 2.1GB}.
   */
  public static String formatSize(long size) {
    if (size < 0) {
      size = 0;
    }
    String[] units = {"B", "kB", "MB", "GB", "TB", "PB"};
    DecimalFormat format = new DecimalFormat("0.#", DecimalFormatSymbols.getInstance(Locale.ROOT));
    for (int i = 0; i < units.length; i++) {
      double unitSize = Math.pow(1024, i);
      double next = Math.pow(1024, i + 1);
      if (size < next || i == units.length - 1) {
        if (i == 0) {
          return size + units[i];
        }
        return format.format(size / unitSize) + units[i];
      }
    }
    return size + "B";
  }

  /** {@code - 200 files (2.1GB) - 10 files selected (120MB)}, or empty when nothing is listed. */
  public String statusSuffix() {
    if (listed < 0) {
      return "";
    }
    return " - "
        + phrase(listed, listedBytes, false)
        + " - "
        + phrase(selected, selectedBytes, true);
  }

  private static String phrase(int count, long bytes, boolean selected) {
    String size = formatSize(bytes);
    if (selected) {
      if (count == 1) {
        return BaseMessages.getString(PKG, "VfsFileExplorer.Status.Selected.One", size);
      }
      return BaseMessages.getString(PKG, "VfsFileExplorer.Status.Selected.Many", count, size);
    }
    if (count == 1) {
      return BaseMessages.getString(PKG, "VfsFileExplorer.Status.Listed.One", size);
    }
    return BaseMessages.getString(PKG, "VfsFileExplorer.Status.Listed.Many", count, size);
  }
}
