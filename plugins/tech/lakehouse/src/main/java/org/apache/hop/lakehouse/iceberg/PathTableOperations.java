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

package org.apache.hop.lakehouse.iceberg;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.lakehouse.iceberg.io.HopVfsFileIO;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * Table operations for an Iceberg table that lives in a folder without a catalog, in the layout of
 * Hadoop catalogs and Spark path tables: {@code metadata/v<N>.metadata.json} plus a {@code
 * version-hint.text} that names the current version.
 *
 * <p>A commit writes the new metadata to a temporary file and then publishes it under the next
 * version's name with an operation that fails if that name is already taken. On a local file system
 * that is a hard link, which the operating system creates atomically, so two writers can't both
 * commit the same version, whether they run in one Hop instance or in several processes. Commits in
 * one JVM are also serialized per table location. Other file systems publish with a check followed
 * by a move, which isn't atomic across processes: like Iceberg's Hadoop tables on S3, a path table
 * there should have one writer at a time. Tables managed by a catalog should be written through the
 * catalog instead.
 *
 * <p>The version hint is best effort. Once the metadata file is in place the commit has happened,
 * so a failure to update the hint is only logged, and readers walk forward from the hint to the
 * newest version (see {@link IcebergTables#findCurrentMetadataFile(String)}).
 */
public class PathTableOperations implements TableOperations {

  /** Serializes commits to the same table location within this JVM. */
  private static final Map<String, ReentrantLock> LOCKS = new ConcurrentHashMap<>();

  private final String location;
  private final FileIO io = new HopVfsFileIO();
  private TableMetadata current;
  private int version = -1;

  public PathTableOperations(String location) {
    this.location =
        location.endsWith("/") ? location.substring(0, location.length() - 1) : location;
  }

  @Override
  public TableMetadata current() {
    if (current == null && version < 0) {
      refresh();
    }
    return current;
  }

  @Override
  public TableMetadata refresh() {
    String metadataFile;
    try {
      metadataFile = IcebergTables.findCurrentMetadataFile(location);
    } catch (Exception e) {
      throw new RuntimeIOException(
          new java.io.IOException("Unable to read Iceberg table at " + location, e));
    }
    if (metadataFile == null) {
      // No table yet: it is created by the first commit.
      version = 0;
      current = null;
    } else {
      version =
          (int)
              IcebergTables.metadataVersion(
                  metadataFile.substring(metadataFile.lastIndexOf('/') + 1));
      current = TableMetadataParser.read(io, metadataFile);
    }
    return current;
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    ReentrantLock lock = LOCKS.computeIfAbsent(location, ignored -> new ReentrantLock());
    lock.lock();
    try {
      commitLocked(base, metadata);
    } finally {
      lock.unlock();
    }
  }

  private void commitLocked(TableMetadata base, TableMetadata metadata) {
    TableMetadata latest = refresh();
    if (base != latest
        && (base == null
            || latest == null
            || !base.metadataFileLocation().equals(latest.metadataFileLocation()))) {
      throw new CommitFailedException(
          "Table %s was changed by another writer since it was read", location);
    }

    int next = version + 1;
    String target = metadataFileLocation("v" + next + ".metadata.json");
    String temp = metadataFileLocation(UUID.randomUUID() + ".metadata.json.tmp");
    TableMetadataParser.write(metadata, io.newOutputFile(temp));
    try {
      publish(temp, target, next);
    } finally {
      deleteQuietly(temp);
    }

    // The commit has happened. Nothing after this point may fail it.
    try {
      writeVersionHint(next);
    } catch (Exception e) {
      LogChannel.GENERAL.logBasic(
          "Committed version "
              + next
              + " of Iceberg table "
              + location
              + ", but couldn't update version-hint.text; readers find the new version anyway: "
              + e.getMessage());
    }
    // Read the new metadata lazily, on the next call to current() or refresh().
    current = null;
    version = -1;
  }

  /**
   * Publishes {@code temp} as {@code target}, failing with a {@link CommitFailedException} if
   * another writer committed that version first.
   */
  void publish(String temp, String target, int next) {
    try {
      FileObject tempFile = HopVfs.getFileObject(temp);
      FileObject targetFile = HopVfs.getFileObject(target);
      if (tempFile instanceof LocalFile && targetFile instanceof LocalFile) {
        Path tempPath = Paths.get(tempFile.getURI());
        Path targetPath = Paths.get(targetFile.getURI());
        try {
          // link(2) fails with EEXIST if the name is taken, atomically.
          Files.createLink(targetPath, tempPath);
        } catch (UnsupportedOperationException | FileSystemException linkNotSupported) {
          if (linkNotSupported instanceof FileAlreadyExistsException) {
            throw linkNotSupported;
          }
          // No hard links on this file system: a move that refuses an existing target.
          Files.move(tempPath, targetPath);
        }
        return;
      }
      if (targetFile.exists()) {
        throw new FileAlreadyExistsException(target);
      }
      tempFile.moveTo(targetFile);
    } catch (FileAlreadyExistsException e) {
      throw new CommitFailedException(
          "Version %d of table %s was committed by another writer", next, location);
    } catch (Exception e) {
      throw new RuntimeIOException(
          new java.io.IOException("Unable to commit version " + next + " of " + location, e));
    }
  }

  private static void deleteQuietly(String file) {
    try {
      FileObject object = HopVfs.getFileObject(file);
      if (object.exists()) {
        object.delete();
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logBasic(
          "Unable to delete temporary file " + file + ": " + e.getMessage());
    }
  }

  private void writeVersionHint(int newVersion) throws Exception {
    FileObject hint = HopVfs.getFileObject(metadataFileLocation("version-hint.text"));
    try (OutputStream out = HopVfs.getOutputStream(hint, false)) {
      out.write(Integer.toString(newVersion).getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * True when the table's metadata files follow the path-table naming, so a commit here is seen by
   * every other reader of the folder. Tables created by other catalogs use different names and must
   * be written through their catalog.
   */
  public boolean isPathTable() {
    TableMetadata metadata = current();
    if (metadata == null) {
      return true;
    }
    String file = metadata.metadataFileLocation();
    return file != null && file.substring(file.lastIndexOf('/') + 1).startsWith("v");
  }

  @Override
  public FileIO io() {
    return io;
  }

  @Override
  public String metadataFileLocation(String fileName) {
    return location + "/metadata/" + fileName;
  }

  @Override
  public LocationProvider locationProvider() {
    TableMetadata metadata = current();
    return LocationProviders.locationsFor(
        location, metadata == null ? java.util.Map.of() : metadata.properties());
  }

  public String location() {
    return location;
  }
}
