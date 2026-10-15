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

import java.io.IOException;
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
 * <p>This class keeps the checksummed local file system and skips those permission changes. Files
 * keep the permissions of the Hop process. It is installed only when Hop creates the session, and
 * only when Windows has neither winutils nor Hadoop's native I/O library. An explicit {@code
 * spark.hadoop.fs.file.impl} in the run configuration is applied afterwards and wins.
 */
public class HopWindowsLocalFileSystem extends LocalFileSystem {

  /** Spark conf key copied into Hadoop {@code fs.file.impl}. */
  public static final String SPARK_HADOOP_FILE_IMPL = "spark.hadoop.fs.file.impl";

  public HopWindowsLocalFileSystem() {
    super(new WinutilsFreeRawLocalFileSystem());
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
              + " permissions of this process instead of calling winutils.");
    }
  }

  /**
   * Raw local file system that does not shell out for chmod or for owner lookups.
   *
   * <p>{@code mkdirs} and {@code create} call {@code setPermission} on this instance, not on the
   * checksum wrapper, so the override has to live here.
   */
  public static class WinutilsFreeRawLocalFileSystem extends RawLocalFileSystem {

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
