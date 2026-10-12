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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;
import lombok.Getter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Copy and move of VFS files and folders. Refuses a copy onto itself or into its own child. An
 * existing file is only replaced once its new content is completely written, and an existing folder
 * is merged rather than replaced.
 */
public final class VfsFileTransfer {

  public enum Mode {
    COPY,
    MOVE
  }

  public enum Refusal {
    NONE,
    SAME,
    INTO_ITSELF,
    EXISTS,
    /** A file where a folder of the same name exists, or the other way around. */
    OTHER_KIND
  }

  /** What to do with a file that already exists at the destination. */
  public enum Resolution {
    REPLACE,
    SKIP,
    CANCEL
  }

  /** Counts what a transfer wrote. Called on the worker thread. */
  public interface Progress {
    Progress NONE =
        new Progress() {
          @Override
          public void bytes(long count) {
            // Not counted.
          }

          @Override
          public void fileDone() {
            // Not counted.
          }
        };

    void bytes(long count);

    void fileDone();
  }

  /** Decides about one existing destination file. Called on the worker thread. */
  @FunctionalInterface
  public interface ConflictResolver {
    Resolution resolve(FileObject source, FileObject target);
  }

  /** One file or folder that could not be placed. Other entries are still transferred. */
  @Getter
  public static final class RefusedException extends RuntimeException {
    private final Refusal refusal;
    private final String name;

    public RefusedException(Refusal refusal, String name) {
      super(refusal + ": " + name);
      this.refusal = refusal;
      this.name = name;
    }
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

  static final String TEMP_PREFIX = ".hop-transfer-";

  private static final int BUFFER_SIZE = 64 * 1024;

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
   * Refusal#EXISTS} can still proceed: a file is replaced when the resolver says so, a folder is
   * merged.
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
      return destination.isFolder() == source.isFolder() ? Refusal.EXISTS : Refusal.OTHER_KIND;
    }
    return Refusal.NONE;
  }

  /**
   * Copy or move {@code source} into {@code destinationFolder}, keeping the base name. An existing
   * folder is merged. Each existing file goes to {@code resolver}.
   *
   * @throws RefusedException when the source cannot be placed at all
   * @throws CancellationException when {@code cancelled} turns true or the resolver cancels
   */
  public static void transfer(
      FileObject source,
      FileObject destinationFolder,
      Mode mode,
      ConflictResolver resolver,
      BooleanSupplier cancelled)
      throws IOException {
    transfer(source, destinationFolder, mode, resolver, cancelled, Progress.NONE);
  }

  /** As {@link #transfer(FileObject, FileObject, Mode, ConflictResolver, BooleanSupplier)}. */
  public static void transfer(
      FileObject source,
      FileObject destinationFolder,
      Mode mode,
      ConflictResolver resolver,
      BooleanSupplier cancelled,
      Progress progress)
      throws IOException {
    Refusal refusal = refusal(source, destinationFolder);
    String name = source == null ? "" : source.getName().getBaseName();
    if (refusal == Refusal.SAME
        || refusal == Refusal.INTO_ITSELF
        || refusal == Refusal.OTHER_KIND) {
      throw new RefusedException(refusal, name);
    }
    place(
        source,
        destinationFolder.resolveFile(name),
        mode,
        resolver,
        cancelled,
        progress == null ? Progress.NONE : progress);
  }

  private static void place(
      FileObject source,
      FileObject target,
      Mode mode,
      ConflictResolver resolver,
      BooleanSupplier cancelled,
      Progress progress)
      throws IOException {
    checkCancelled(cancelled);
    if (!target.exists()) {
      if (mode == Mode.MOVE && source.canRenameTo(target)) {
        source.moveTo(target);
        progress.fileDone();
      } else {
        copyTree(source, target, cancelled, progress);
        if (mode == Mode.MOVE) {
          delete(source);
        }
      }
      return;
    }
    if (source.isFolder() != target.isFolder()) {
      throw new RefusedException(Refusal.OTHER_KIND, source.getName().getBaseName());
    }
    if (!source.isFolder()) {
      Resolution resolution = resolver == null ? Resolution.SKIP : resolver.resolve(source, target);
      if (resolution == Resolution.CANCEL) {
        throw new CancellationException();
      }
      if (resolution == Resolution.REPLACE) {
        replaceFile(source, target, mode, cancelled, progress);
      }
      return;
    }
    for (FileObject child : source.getChildren()) {
      place(
          child,
          target.resolveFile(child.getName().getBaseName()),
          mode,
          resolver,
          cancelled,
          progress);
    }
    if (mode == Mode.MOVE && source.getChildren().length == 0) {
      // Skipped files stay behind, and so does the folder that holds them.
      source.delete();
    }
  }

  /**
   * Replace the existing file {@code target} with {@code source}. The new content goes to a hidden
   * file next to the target first, so a failed or stopped copy leaves the original untouched.
   */
  public static void replaceFile(
      FileObject source, FileObject target, Mode mode, BooleanSupplier cancelled)
      throws IOException {
    replaceFile(source, target, mode, cancelled, Progress.NONE);
  }

  /** As {@link #replaceFile(FileObject, FileObject, Mode, BooleanSupplier)}, counting bytes. */
  public static void replaceFile(
      FileObject source, FileObject target, Mode mode, BooleanSupplier cancelled, Progress progress)
      throws IOException {
    FileObject temp = target.getParent().resolveFile(TEMP_PREFIX + UUID.randomUUID() + ".tmp");
    // copyFile removes a partly written temp file itself.
    copyFile(source, temp, cancelled, progress == null ? Progress.NONE : progress);
    try {
      target.delete();
    } catch (FileSystemException | RuntimeException e) {
      deleteQuietly(temp);
      throw e;
    }
    // From here the new content is only in the temp file: it is kept if the rename fails.
    temp.moveTo(target);
    if (mode == Mode.MOVE) {
      source.delete();
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

  /**
   * Copy a file or a folder with everything in it to {@code target}, which does not exist yet.
   * Stops between chunks when {@code cancelled} turns true, and removes a partly written file.
   */
  public static void copy(
      FileObject source, FileObject target, BooleanSupplier cancelled, Progress progress)
      throws IOException {
    copyTree(source, target, cancelled, progress == null ? Progress.NONE : progress);
  }

  private static void copyTree(
      FileObject source, FileObject target, BooleanSupplier cancelled, Progress progress)
      throws IOException {
    checkCancelled(cancelled);
    if (!source.isFolder()) {
      copyFile(source, target, cancelled, progress);
      return;
    }
    target.createFolder();
    for (FileObject child : source.getChildren()) {
      copyTree(child, target.resolveFile(child.getName().getBaseName()), cancelled, progress);
    }
  }

  /**
   * Copy one file's content, checking {@code cancelled} between chunks so Stop does not wait for a
   * large file. A failed or stopped copy deletes what it wrote.
   */
  private static void copyFile(
      FileObject source, FileObject target, BooleanSupplier cancelled, Progress progress)
      throws IOException {
    try (InputStream in = HopVfs.getInputStream(source);
        OutputStream out = HopVfs.getOutputStream(target, false)) {
      byte[] buffer = new byte[BUFFER_SIZE];
      int read;
      while ((read = in.read(buffer)) >= 0) {
        checkCancelled(cancelled);
        out.write(buffer, 0, read);
        progress.bytes(read);
      }
    } catch (IOException | RuntimeException e) {
      deleteQuietly(target);
      throw e;
    }
    progress.fileDone();
  }

  private static void checkCancelled(BooleanSupplier cancelled) {
    if ((cancelled != null && cancelled.getAsBoolean()) || Thread.currentThread().isInterrupted()) {
      throw new CancellationException();
    }
  }

  private static void deleteQuietly(FileObject file) {
    try {
      if (file.exists()) {
        file.delete();
      }
    } catch (Exception ignored) {
      // Best effort: the original file is still in place.
    }
  }
}
