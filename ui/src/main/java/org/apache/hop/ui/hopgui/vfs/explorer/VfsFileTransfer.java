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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;
import lombok.Getter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;

/**
 * Copy and move of VFS files and folders. Refuses a copy onto itself or into its own child. An
 * existing file is only replaced once its new content is completely written, and an existing folder
 * is merged rather than replaced.
 *
 * <p>A link on the local disk (a symbolic link, or a directory junction on Windows) is deleted and
 * moved as a link: the files it points to stay where they are. A move to another local disk creates
 * the link again there. A copy follows a link to a folder and copies what is in it. A link that
 * leads back to a folder the transfer is already in is skipped and reported, and the rest is still
 * transferred. A folder is not copied or moved onto or into the folder it really is, also when a
 * link leads there.
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

    /**
     * {@code folder} was reached through a link and leads back to a folder this transfer is already
     * in, so it was left out. The rest is still transferred.
     */
    default void linkLoopSkipped(String folder) {
      // Not reported.
    }
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

  private static final Class<?> PKG = VfsFileExplorer.class;

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
   * Drop a path that sits inside another path of the list, and repeated paths. A path that holds
   * another one is a folder, so this needs no file system access: the folder transfer already
   * includes the rest.
   */
  public static List<String> withoutNestedPaths(List<String> paths) {
    List<String> kept = new ArrayList<>();
    if (paths == null) {
      return kept;
    }
    for (int i = 0; i < paths.size(); i++) {
      String path = paths.get(i);
      if (path == null || path.isBlank()) {
        continue;
      }
      boolean nested = false;
      for (int j = 0; j < paths.size() && !nested; j++) {
        String other = paths.get(j);
        if (j == i || other == null || other.isBlank()) {
          continue;
        }
        nested =
            VfsLocations.isUnder(other, path) || (j < i && VfsLocations.sameLocation(other, path));
      }
      if (!nested) {
        kept.add(path);
      }
    }
    return kept;
  }

  /** Opens the file of an entry. */
  @FunctionalInterface
  public interface FileResolver {
    FileObject resolve(String uri) throws Exception;
  }

  /** One entry of a batch that was not transferred: refused, or failed with an error. */
  @Getter
  public static final class Failure {
    private final String name;

    /** Why it was refused, or null when it failed with {@link #error}. */
    private final Refusal refusal;

    private final Exception error;

    Failure(String name, Refusal refusal, Exception error) {
      this.name = name;
      this.refusal = refusal;
      this.error = error;
    }
  }

  /** What a copy or move of several entries did. */
  @Getter
  public static final class BatchResult {
    private final List<Failure> failures = new ArrayList<>();

    /**
     * Entries whose source is still in place. After a move these are the ones that were skipped,
     * refused, failed or not reached; after a copy, all of them.
     */
    private final List<Entry> remaining = new ArrayList<>();

    private boolean cancelled;
  }

  /**
   * Copy or move every entry into {@code destinationFolder}. A refusal or an error on one entry is
   * recorded and the next entry is still transferred. A stop ends the batch.
   */
  public static BatchResult transferAll(
      List<Entry> entries,
      FileObject destinationFolder,
      Mode mode,
      ConflictResolver resolver,
      BooleanSupplier cancelled,
      Progress progress,
      FileResolver files) {
    BatchResult result = new BatchResult();
    for (Entry entry : entries) {
      if (isCancelled(cancelled)) {
        result.cancelled = true;
        break;
      }
      try {
        transfer(
            files.resolve(entry.getUri()), destinationFolder, mode, resolver, cancelled, progress);
      } catch (RefusedException e) {
        result.failures.add(new Failure(entry.getName(), e.getRefusal(), null));
      } catch (CancellationException e) {
        result.cancelled = true;
        break;
      } catch (Exception e) {
        result.failures.add(new Failure(entry.getName(), null, e));
      }
    }
    for (Entry entry : entries) {
      if (mode == Mode.COPY || stillInPlace(files, entry)) {
        result.remaining.add(entry);
      }
    }
    return result;
  }

  /** True when the source of {@code entry} exists, or when that cannot be told. */
  private static boolean stillInPlace(FileResolver files, Entry entry) {
    try {
      FileObject file = files.resolve(entry.getUri());
      file.refresh();
      return file.exists();
    } catch (Exception e) {
      return true;
    }
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
    if (source.isFolder()
        && (isInside(source, destinationFolder)
            || realRefusal(source, destinationFolder) != Refusal.NONE)) {
      return Refusal.INTO_ITSELF;
    }
    if (destination.exists()) {
      if (destination.isFolder() != source.isFolder()) {
        return Refusal.OTHER_KIND;
      }
      Refusal real = source.isFolder() ? realRefusal(source, destination) : Refusal.NONE;
      return real == Refusal.NONE ? Refusal.EXISTS : real;
    }
    return Refusal.NONE;
  }

  /**
   * Where two existing local folders really are, after following links. {@link Refusal#SAME} when
   * {@code target} is {@code source}, {@link Refusal#INTO_ITSELF} when it is inside it. Merging
   * into such a target would write every file onto itself, or into the folder being read.
   */
  private static Refusal realRefusal(FileObject source, FileObject target) {
    Path realSource = realPath(source);
    Path realTarget = realPath(target);
    if (realSource == null || realTarget == null) {
      return Refusal.NONE;
    }
    if (realTarget.equals(realSource)) {
      return Refusal.SAME;
    }
    return realTarget.startsWith(realSource) ? Refusal.INTO_ITSELF : Refusal.NONE;
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
        new Walk(resolver, cancelled, progress == null ? Progress.NONE : progress));
  }

  /** What one transfer shares between the folders it goes through. */
  private static final class Walk {
    private final ConflictResolver resolver;
    private final BooleanSupplier cancelled;
    private final Progress progress;

    /**
     * The real paths of the local folders this transfer is reading or writing, see {@link #enter}.
     */
    private final Set<Path> open = new HashSet<>();

    /**
     * While a link is moved into an existing folder: the files and folders it placed where nothing
     * was, so they can be removed again when that fails. Null otherwise.
     */
    private List<FileObject> created;

    /** Files left where they are: skipped in the prompt, or reached again through a link. */
    private int skipped;

    private Walk(ConflictResolver resolver, BooleanSupplier cancelled, Progress progress) {
      this.resolver = resolver;
      this.cancelled = cancelled;
      this.progress = progress;
    }
  }

  private static void place(FileObject source, FileObject target, Mode mode, Walk walk)
      throws IOException {
    checkCancelled(walk.cancelled);
    if (!target.exists()) {
      if (mode == Mode.MOVE && canRename(source, target)) {
        source.moveTo(target);
        walk.progress.fileDone();
      } else if (mode == Mode.MOVE) {
        moveByCopy(source, target, walk);
      } else {
        copyTree(source, target, false, walk);
      }
      if (walk.created != null) {
        walk.created.add(target);
      }
      return;
    }
    String name = source.getName().getBaseName();
    if (source.isFolder() != target.isFolder()) {
      throw new RefusedException(Refusal.OTHER_KIND, name);
    }
    if (!source.isFolder()) {
      Resolution resolution =
          walk.resolver == null ? Resolution.SKIP : walk.resolver.resolve(source, target);
      if (resolution == Resolution.CANCEL) {
        throw new CancellationException();
      }
      if (resolution == Resolution.REPLACE) {
        replaceFile(source, target, mode, walk.cancelled, walk.progress);
      } else {
        walk.skipped++;
      }
      return;
    }
    Refusal real = realRefusal(source, target);
    if (real != Refusal.NONE) {
      throw new RefusedException(real, name);
    }
    if (mode == Mode.MOVE && localLink(source) != null) {
      moveLinkInto(source, target, walk);
      return;
    }
    Path entered = enter(walk, source);
    if (entered == LOOP) {
      return;
    }
    try {
      for (FileObject child : source.getChildren()) {
        place(child, target.resolveFile(child.getName().getBaseName()), mode, walk);
      }
    } finally {
      leave(walk, entered);
    }
    if (mode == Mode.MOVE) {
      source.refresh();
      if (source.getChildren().length == 0) {
        // Skipped files stay behind, and so does the folder that holds them.
        source.delete();
      }
    }
  }

  /**
   * Move a link to a folder into the existing folder {@code target}. Moving the files themselves
   * would empty the folder the link points to, so they are copied and then the link is removed. The
   * link stays when a file was skipped. When the copy fails or is stopped, the files and folders it
   * created in {@code target} are removed again, so nothing is left twice.
   */
  private static void moveLinkInto(FileObject link, FileObject target, Walk walk)
      throws IOException {
    int skippedBefore = walk.skipped;
    Path entered = enter(walk, link);
    if (entered == LOOP) {
      return;
    }
    // The files are copied, so no other link is moved inside this one.
    List<FileObject> created = new ArrayList<>();
    walk.created = created;
    try {
      for (FileObject child : link.getChildren()) {
        place(child, target.resolveFile(child.getName().getBaseName()), Mode.COPY, walk);
      }
    } catch (IOException | RuntimeException e) {
      for (int i = created.size() - 1; i >= 0; i--) {
        try {
          delete(created.get(i));
        } catch (Exception ignored) {
          // The failure itself is what gets reported.
        }
      }
      throw e;
    } finally {
      leave(walk, entered);
      walk.created = null;
    }
    if (walk.skipped == skippedBefore) {
      delete(link);
    }
  }

  /**
   * True when a rename can move {@code source}. Commons VFS sees the whole local disk as one file
   * system, but a rename cannot move a file to another disk or partition.
   */
  private static boolean canRename(FileObject source, FileObject target) {
    if (!source.canRenameTo(target)) {
      return false;
    }
    Path from = localPath(source);
    Path to = localPath(target);
    if (from == null || to == null) {
      return true;
    }
    try {
      Path fromFolder = from.toAbsolutePath().getParent();
      Path toFolder = to.toAbsolutePath().getParent();
      if (fromFolder == null || toFolder == null) {
        return true;
      }
      return Files.getFileStore(fromFolder.toRealPath())
          .equals(Files.getFileStore(toFolder.toRealPath()));
    } catch (IOException | RuntimeException e) {
      // The rename reports what is wrong.
      return true;
    }
  }

  /**
   * Move by copying and then deleting the source. A link is created again as a link where it can
   * be, so the files it points to are not copied. When a link loop was skipped, the source stays.
   */
  private static void moveByCopy(FileObject source, FileObject target, Walk walk)
      throws IOException {
    int skippedBefore = walk.skipped;
    copyTree(source, target, true, walk);
    if (walk.skipped == skippedBefore) {
      delete(source);
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
    FileObject folder = target.getParent();
    String id = UUID.randomUUID().toString();
    FileObject temp = folder.resolveFile(TEMP_PREFIX + id + ".tmp");
    // copyFile removes a partly written temp file itself.
    copyFile(source, temp, cancelled, progress == null ? Progress.NONE : progress);
    // Keep the original under a hidden name until the new copy has the real name. On an object
    // store a rename is a copy and a delete, so it can fail half way.
    FileObject backup = folder.resolveFile(TEMP_PREFIX + id + ".bak");
    try {
      target.moveTo(backup);
    } catch (IOException | RuntimeException e) {
      deleteQuietly(temp);
      throw e;
    }
    try {
      temp.moveTo(target);
    } catch (IOException | RuntimeException e) {
      if (restore(backup, target)) {
        deleteQuietly(temp);
        throw e;
      }
      throw new IOException(
          BaseMessages.getString(
              PKG,
              "VfsFileExplorer.Transfer.Stranded",
              target.getName().getBaseName(),
              backup.getName().getBaseName(),
              temp.getName().getBaseName()),
          e);
    }
    deleteQuietly(backup);
    if (mode == Mode.MOVE) {
      source.delete();
    }
  }

  /**
   * Delete a file, or a folder with everything in it. A link is removed by itself, also when it is
   * inside the folder: the files it points to stay.
   */
  public static void delete(FileObject file) throws FileSystemException {
    if (file == null) {
      return;
    }
    Path path = localPath(file);
    if (path == null) {
      // Links are only known on the local disk. Elsewhere the provider removes the whole tree.
      if (file.exists()) {
        file.delete(Selectors.SELECT_ALL);
      }
      return;
    }
    // Commons VFS sees a link to a folder as that folder: Selectors.SELECT_ALL deletes what is in
    // it, and the link is not removed while that folder holds files. So the disk is walked
    // directly, without following links.
    try {
      deleteLocal(path);
    } catch (IOException e) {
      throw new FileSystemException("vfs.provider/delete.error", e, file.getName());
    } finally {
      file.refresh();
      FileObject parent = file.getParent();
      if (parent != null) {
        parent.refresh();
      }
    }
  }

  private static void deleteLocal(Path path) throws IOException {
    BasicFileAttributes attributes;
    try {
      attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    } catch (NoSuchFileException e) {
      return;
    }
    if (!attributes.isDirectory() || isLink(path, attributes)) {
      Files.delete(path);
      return;
    }
    Files.walkFileTree(
        path,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult preVisitDirectory(
              Path folder, BasicFileAttributes folderAttributes) throws IOException {
            // A Windows junction is a folder to the walk. Remove it, not what it points to.
            if (!folder.equals(path) && isLink(folder, folderAttributes)) {
              Files.delete(folder);
              return FileVisitResult.SKIP_SUBTREE;
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes fileAttributes)
              throws IOException {
            // Links are not followed, so a symbolic link to a folder arrives here, as itself.
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path folder, IOException e) throws IOException {
            if (e != null) {
              throw e;
            }
            Files.delete(folder);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  /** The path of a file on the local disk, or null when it is elsewhere. */
  private static Path localPath(FileObject file) {
    try {
      if (file == null || !"file".equals(file.getName().getScheme())) {
        return null;
      }
      String name = HopVfs.getFilename(file);
      // A network share keeps its URI.
      return name.startsWith("file:") ? null : Path.of(name);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * The path of a link on the local disk, or null when the file is not one. A link is a symbolic
   * link, or a directory junction on Windows.
   */
  static Path localLink(FileObject file) {
    Path path = localPath(file);
    if (path == null) {
      return null;
    }
    try {
      BasicFileAttributes attributes =
          Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
      return isLink(path, attributes) ? path : null;
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * True for a symbolic link, and for a Windows directory junction ({@code mklink /J}). Java does
   * not count a junction as a symbolic link: it is a folder that is also "other", a reparse point.
   * Other reparse points, such as a cloud storage placeholder, are folders in their own place, so a
   * junction is told apart by its real path being somewhere else.
   */
  private static boolean isLink(Path path, BasicFileAttributes attributes) {
    if (attributes.isSymbolicLink()) {
      return true;
    }
    if (!attributes.isOther() || !attributes.isDirectory()) {
      return false;
    }
    try {
      Path absolute = path.toAbsolutePath();
      Path folder = absolute.getParent();
      return folder != null
          && !absolute.toRealPath().equals(folder.toRealPath().resolve(absolute.getFileName()));
    } catch (IOException e) {
      // A junction whose folder is gone.
      return true;
    }
  }

  /** The real path of an existing local file, following links, or null. */
  private static Path realPath(FileObject file) {
    Path path = localPath(file);
    if (path == null) {
      return null;
    }
    try {
      return path.toRealPath();
    } catch (IOException e) {
      return null;
    }
  }

  /** What {@link #enter} returns for a folder the transfer is already in. */
  private static final Path LOOP = Path.of("");

  /**
   * Note that the transfer is in {@code folder}. Following links, a copy can reach a folder it is
   * already copying or writing into, and would then never end. Such a folder is skipped and
   * reported, and the rest is still transferred. Only local folders are checked: the links are on
   * the local disk.
   *
   * @return {@link #LOOP} when the transfer is already in this folder; otherwise the real path to
   *     pass to {@link #leave}, or null when the folder is not local
   */
  private static Path enter(Walk walk, FileObject folder) {
    Path real = realPath(folder);
    if (real == null) {
      // Not checked. Reading the folder reports what is wrong with it.
      return null;
    }
    if (!walk.open.add(real)) {
      walk.skipped++;
      walk.progress.linkLoopSkipped(HopVfs.getFilename(folder));
      return LOOP;
    }
    return real;
  }

  private static void leave(Walk walk, Path real) {
    if (real != null && real != LOOP) {
      walk.open.remove(real);
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

  /**
   * True when both objects name one file. {@link #same} compares the path as written, so on a
   * case-insensitive disk {@code data.csv} and {@code Data.csv} are not the same to it. A local
   * pair counts only when the names differ by letter case, neither path is a symbolic link, and the
   * disk reports one file. A link and the file it points to stay different, so renaming the link
   * does not replace that file.
   */
  static boolean sameFile(FileObject left, FileObject right) {
    if (same(left, right)) {
      return true;
    }
    try {
      String leftName = HopVfs.getFilename(left);
      String rightName = HopVfs.getFilename(right);
      if (!leftName.equalsIgnoreCase(rightName)
          || !HopVfs.isLocalFileSystem(leftName)
          || !HopVfs.isLocalFileSystem(rightName)) {
        return false;
      }
      Path leftPath = Path.of(leftName);
      Path rightPath = Path.of(rightName);
      return !Files.isSymbolicLink(leftPath)
          && !Files.isSymbolicLink(rightPath)
          && Files.isSameFile(leftPath, rightPath);
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
    copyTree(
        source,
        target,
        false,
        new Walk(null, cancelled, progress == null ? Progress.NONE : progress));
  }

  /**
   * Move {@code source} to {@code target}, which does not exist yet, by copying and then deleting
   * it, as a move to another disk does. A local link is created again as a link.
   */
  static void moveByCopy(
      FileObject source, FileObject target, BooleanSupplier cancelled, Progress progress)
      throws IOException {
    moveByCopy(
        source, target, new Walk(null, cancelled, progress == null ? Progress.NONE : progress));
  }

  /** Put the original back after the new copy could not take its name. */
  private static boolean restore(FileObject backup, FileObject target) {
    try {
      if (target.exists()) {
        target.delete();
      }
      backup.moveTo(target);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * A folder this creates is removed again when the copy fails or is stopped, with whatever was
   * already copied into it.
   *
   * @param keepLinks create a local link again as a link instead of copying what it points to, as
   *     for a move
   */
  private static void copyTree(FileObject source, FileObject target, boolean keepLinks, Walk walk)
      throws IOException {
    checkCancelled(walk.cancelled);
    if (keepLinks && copyLink(source, target)) {
      walk.progress.fileDone();
      return;
    }
    if (!source.isFolder()) {
      copyFile(source, target, walk.cancelled, walk.progress);
      return;
    }
    Path realSource = enter(walk, source);
    if (realSource == LOOP) {
      return;
    }
    Path realTarget = null;
    try {
      target.createFolder();
      try {
        // A link inside the source can lead into the folder being written. That folder is new, so
        // only a later source folder can match it.
        realTarget = realPath(target);
        if (realTarget != null && !walk.open.add(realTarget)) {
          realTarget = null;
        }
        for (FileObject child : source.getChildren()) {
          copyTree(child, target.resolveFile(child.getName().getBaseName()), keepLinks, walk);
        }
      } catch (IOException | RuntimeException e) {
        try {
          delete(target);
        } catch (Exception ignored) {
          // The failure itself is what gets reported.
        }
        throw e;
      }
    } finally {
      leave(walk, realTarget);
      leave(walk, realSource);
    }
  }

  /**
   * Create the local link {@code source} again at {@code target}, pointing where it points.
   *
   * @return false when {@code source} is not a local link, or the link cannot be created there: a
   *     junction, a target that is not on the local disk, or no right to create links (Windows
   *     without developer mode). The caller then copies what the link points to.
   */
  private static boolean copyLink(FileObject source, FileObject target) {
    Path link = localLink(source);
    Path to = localPath(target);
    if (link == null || to == null) {
      return false;
    }
    try {
      Files.createSymbolicLink(to, Files.readSymbolicLink(link));
    } catch (IOException | UnsupportedOperationException | SecurityException e) {
      return false;
    }
    try {
      target.refresh();
    } catch (FileSystemException e) {
      // Only the cached state; the link is in place.
    }
    return true;
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
    if (isCancelled(cancelled)) {
      throw new CancellationException();
    }
  }

  private static boolean isCancelled(BooleanSupplier cancelled) {
    return (cancelled != null && cancelled.getAsBoolean())
        || Thread.currentThread().isInterrupted();
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
