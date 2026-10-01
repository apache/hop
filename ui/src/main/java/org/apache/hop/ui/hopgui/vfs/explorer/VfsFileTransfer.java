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
import lombok.Getter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.apache.hop.core.vfs.HopVfs;

/** Copy and move of VFS files and folders. Refuses a copy onto itself or into its own child. */
public final class VfsFileTransfer {

  public enum Mode {
    COPY,
    MOVE
  }

  public enum Refusal {
    NONE,
    SAME,
    INTO_ITSELF,
    EXISTS
  }

  /** One file or folder held on the explorer clipboard. */
  @Getter
  public static final class Entry {
    private final String uri;
    private final String name;
    private final boolean folder;

    public Entry(String uri, String name, boolean folder) {
      this.uri = uri == null ? "" : uri;
      this.name = name == null ? "" : name;
      this.folder = folder;
    }
  }

  private VfsFileTransfer() {}

  /**
   * Drop a folder that sits inside another selected folder. The parent copy already includes it.
   */
  public static List<Entry> withoutNested(List<Entry> entries) {
    List<Entry> kept = new ArrayList<>();
    if (entries == null) {
      return kept;
    }
    for (Entry entry : entries) {
      if (entry == null) {
        continue;
      }
      boolean nested = false;
      for (Entry other : entries) {
        if (other != null
            && other != entry
            && other.isFolder()
            && VfsLocations.isUnder(other.getUri(), entry.getUri())) {
          nested = true;
          break;
        }
      }
      if (!nested) {
        kept.add(entry);
      }
    }
    return kept;
  }

  /**
   * Why this source cannot be placed in {@code destinationFolder} under its own base name. {@link
   * Refusal#EXISTS} can still proceed when the caller chooses to replace.
   */
  public static Refusal refusal(FileObject source, FileObject destinationFolder)
      throws FileSystemException {
    if (source == null || destinationFolder == null) {
      return Refusal.SAME;
    }
    FileObject destination = destinationFolder.resolveFile(source.getName().getBaseName());
    if (same(source, destination)) {
      return Refusal.SAME;
    }
    if (source.isFolder() && isInside(source, destinationFolder)) {
      return Refusal.INTO_ITSELF;
    }
    if (destination.exists()) {
      return Refusal.EXISTS;
    }
    return Refusal.NONE;
  }

  /**
   * Copy or move {@code source} into {@code destinationFolder}, keeping the base name. An existing
   * destination is replaced only when {@code overwrite} is true.
   */
  public static void transfer(
      FileObject source, FileObject destinationFolder, Mode mode, boolean overwrite)
      throws FileSystemException {
    Refusal refusal = refusal(source, destinationFolder);
    if (refusal == Refusal.SAME
        || refusal == Refusal.INTO_ITSELF
        || (refusal == Refusal.EXISTS && !overwrite)) {
      throw new IllegalStateException(refusal.name());
    }
    FileObject destination = destinationFolder.resolveFile(source.getName().getBaseName());
    if (refusal == Refusal.EXISTS) {
      delete(destination);
    }
    if (mode == Mode.MOVE) {
      source.moveTo(destination);
    } else {
      destination.copyFrom(source, Selectors.SELECT_ALL);
    }
  }

  public static void delete(FileObject file) throws FileSystemException {
    if (file == null || !file.exists()) {
      return;
    }
    if (file.isFolder()) {
      file.delete(Selectors.SELECT_ALL);
    } else {
      file.delete();
    }
  }

  public static boolean same(FileObject left, FileObject right) {
    if (left == null || right == null) {
      return false;
    }
    try {
      if (left.equals(right)) {
        return true;
      }
    } catch (Exception ignored) {
      // Fall through to the filename comparison.
    }
    try {
      return HopVfs.getFilename(left).equals(HopVfs.getFilename(right));
    } catch (Exception ignored) {
      return false;
    }
  }

  /** True when {@code candidate} is {@code folder} or a folder inside it. */
  public static boolean isInside(FileObject folder, FileObject candidate)
      throws FileSystemException {
    if (folder == null || candidate == null) {
      return false;
    }
    if (same(folder, candidate)) {
      return true;
    }
    return folder.getName().isDescendent(candidate.getName());
  }
}
