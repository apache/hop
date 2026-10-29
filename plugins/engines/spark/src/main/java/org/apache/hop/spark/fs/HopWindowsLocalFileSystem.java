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

package org.apache.hop.spark.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Shell;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.spark.sql.SparkSession;

/**
 * Local Hadoop file system for a Windows Spark session that has no {@code winutils.exe}.
 *
 * <p>Spark file output commits through {@code FileOutputCommitter}, which creates the output
 * directory with {@code FileSystem.mkdirs}. On Windows, Hadoop's {@code RawLocalFileSystem} then
 * runs {@code winutils chmod}. That binary is resolved from {@code HADOOP_HOME} or {@code
 * hadoop.home.dir}, and the call fails with {@code HADOOP_HOME and hadoop.home.dir are unset} when
 * neither is set. The file itself was already created by Java; only the permission change fails.
 *
 * <p>This class keeps the checksummed local file system and skips those permission changes. It also
 * lists directories with {@link File} instead of {@code FileUtil.list}. On Windows that helper
 * calls {@code NativeIO.Windows.access} without checking that the native library loaded, and the
 * resulting {@link UnsatisfiedLinkError} is not an {@link IOException}. Spark file output hits it
 * when {@code FileOutputCommitter} lists the output directory. A {@code file:///} path, including
 * one produced by resolving a VFS variable such as {@code PROJECT_HOME}, is this same local file
 * system.
 *
 * <p>Files keep the permissions of the Hop process. The class is installed only when Hop creates
 * the session, and only when Windows has neither winutils nor Hadoop's native I/O library. An
 * explicit {@code spark.hadoop.fs.file.impl} in the run configuration is applied afterwards and
 * wins. A reused session keeps the file system it was created with.
 */
public class HopWindowsLocalFileSystem extends LocalFileSystem {

  /** Spark conf key copied into Hadoop {@code fs.file.impl}. */
  public static final String SPARK_HADOOP_FILE_IMPL = "spark.hadoop.fs.file.impl";

  public HopWindowsLocalFileSystem() {
    this(new WinutilsFreeRawLocalFileSystem());
  }

  /** Visible to tests that supply a raw file system with the Java directory listing forced on. */
  HopWindowsLocalFileSystem(WinutilsFreeRawLocalFileSystem raw) {
    super(raw);
  }

  /**
   * True when a local Spark write would fail in {@code RawLocalFileSystem.setPermission} because
   * Windows has no winutils binary and no Hadoop native library.
   */
  public static boolean needed() {
    if (!Shell.WINDOWS) {
      return false;
    }
    return permissionShellMissing(true, Shell.hasWinutilsPath(), NativeIO.isAvailable());
  }

  /**
   * Whether a permission change would shell out to a missing winutils binary.
   *
   * @param windows true when the JVM is running on Windows
   * @param winutilsPresent true when HADOOP_HOME/bin/winutils.exe was found
   * @param nativeIo true when Hadoop's native library can chmod without winutils
   * @return true when Windows has neither winutils nor the native library
   */
  public static boolean permissionShellMissing(
      boolean windows, boolean winutilsPresent, boolean nativeIo) {
    return windows && !winutilsPresent && !nativeIo;
  }

  /**
   * Point a new Spark session at this file system when {@link #needed()} is true.
   *
   * <p>Call this before run-configuration entries are copied onto the builder. A later {@code
   * spark.hadoop.fs.file.impl} replaces this default.
   */
  public static void applyIfNeeded(SparkSession.Builder builder, ILogChannel log) {
    if (!needed()) {
      return;
    }
    builder.config(SPARK_HADOOP_FILE_IMPL, HopWindowsLocalFileSystem.class.getName());
    if (log != null) {
      log.logBasic(
          "HADOOP_HOME and hadoop.home.dir are unset. Local Spark paths will keep the"
              + " permissions of this process instead of calling winutils or Hadoop's native"
              + " Windows library.");
    }
  }

  /**
   * Raw local file system that does not shell out for chmod or for owner lookups, and does not call
   * the Windows native access check while listing a directory.
   *
   * <p>{@code mkdirs} and {@code create} call {@code setPermission} on this instance, not on the
   * checksum wrapper. {@code ChecksumFileSystem.listStatus} calls {@code listStatus} here. Both
   * overrides have to live on this class.
   */
  public static class WinutilsFreeRawLocalFileSystem extends RawLocalFileSystem {

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      if (!skipNativeDirectoryListing()) {
        return super.listStatus(f);
      }
      File localFile = pathToFile(f);
      if (!localFile.exists()) {
        throw new FileNotFoundException("File " + f + " does not exist");
      }
      if (!localFile.isDirectory()) {
        return new FileStatus[] {getFileStatus(f)};
      }
      String[] names = listLocalNames(localFile);
      FileStatus[] results = new FileStatus[names.length];
      int kept = 0;
      for (String name : names) {
        try {
          // The three-argument Path keeps a colon in a name from being read as a scheme.
          results[kept] = getFileStatus(new Path(f, new Path(null, null, name)));
          kept++;
        } catch (FileNotFoundException ignored) {
          // The directory changed between list() and getFileStatus().
        }
      }
      if (kept == names.length) {
        return results;
      }
      return Arrays.copyOf(results, kept);
    }

    /**
     * Whether directory listing should use {@link File} instead of {@code FileUtil.list}.
     *
     * <p>The default follows {@link HopWindowsLocalFileSystem#needed()}. Tests override it so the
     * Java listing can run on Linux, where {@code needed()} is false and {@code FileUtil.canRead}
     * does not call the Windows native method.
     */
    boolean skipNativeDirectoryListing() {
      return HopWindowsLocalFileSystem.needed();
    }

    /**
     * Names in a directory, using {@link File#canRead()} and {@link File#list()}.
     *
     * <p>{@code FileUtil.list} refuses the directory through {@code FileUtil.canRead}. On Windows
     * that method always calls {@code NativeIO.Windows.access}. When {@code hadoop.dll} is not
     * loaded the native method throws {@link UnsatisfiedLinkError}.
     */
    static String[] listLocalNames(File directory) throws IOException {
      if (!directory.canRead()) {
        throw new AccessDeniedException(directory.toString(), null, "Permission denied");
      }
      String[] names = directory.list();
      if (names == null) {
        throw new IOException("Invalid directory or I/O error occurred for dir: " + directory);
      }
      return names;
    }

    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
      if (HopWindowsLocalFileSystem.needed()) {
        return;
      }
      super.setPermission(path, permission);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      return localPermissions(super.getFileStatus(path));
    }

    @Override
    public FileStatus getFileLinkStatus(Path path) throws IOException {
      return localPermissions(super.getFileLinkStatus(path));
    }

    /**
     * Copy status onto a plain {@link FileStatus}. The deprecated Windows status loads owner and
     * mode by running winutils the first time those fields are read.
     */
    private static FileStatus localPermissions(FileStatus status) throws IOException {
      if (!HopWindowsLocalFileSystem.needed()) {
        return status;
      }
      Path symlink = status.isSymlink() ? status.getSymlink() : null;
      return new FileStatus(
          status.getLen(),
          status.isDirectory(),
          status.getReplication(),
          status.getBlockSize(),
          status.getModificationTime(),
          status.getAccessTime(),
          FsPermission.getFileDefault(),
          "",
          "",
          symlink,
          status.getPath());
    }
  }
}
