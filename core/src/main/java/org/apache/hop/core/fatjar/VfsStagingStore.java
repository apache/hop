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

package org.apache.hop.core.fatjar;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/**
 * A staging folder written through Hop VFS (any named connection: MinIO, S3, HDFS, Azure, ...),
 * which the cluster reads through its own URL for the same folder.
 */
public final class VfsStagingStore implements IStagingStore {

  private final IVariables variables;
  private final String folder;
  private final String clusterFolder;

  /**
   * @param stagingFolder Hop VFS folder to upload to
   * @param stagingClusterUrl the same folder as the cluster reads it, or empty for the same URL
   * @param missingFolderMessage the error when no staging folder is set
   */
  public VfsStagingStore(
      IVariables variables,
      String stagingFolder,
      String stagingClusterUrl,
      String missingFolderMessage)
      throws HopException {
    this.variables = variables;
    this.folder = trimSlash(variables.resolve(stagingFolder));
    if (StringUtils.isEmpty(folder)) {
      throw new HopException(missingFolderMessage);
    }
    String cluster = trimSlash(variables.resolve(stagingClusterUrl));
    this.clusterFolder = StringUtils.isEmpty(cluster) ? folder : cluster;
  }

  @Override
  public long size(String name) throws HopException {
    try {
      FileObject file = file(name);
      return file.exists() ? file.getContent().getSize() : -1L;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to read " + location(name), e);
    }
  }

  /**
   * Upload under a name of its own and give the file its final name once it is complete: a run that
   * stages the same content at the same time, or a driver that reads the file, never finds half a
   * file under the final name.
   */
  @Override
  public void upload(Path localFile, String name) throws HopException {
    FileObject partial = null;
    try {
      FileObject target = file(name);
      createFolder(target.getParent());
      FileObject upload = file(name + StagingCleanup.UPLOAD_INFIX + UUID.randomUUID());
      if (!upload.canRenameTo(target)) {
        // A store without renames (Google Cloud Storage) would copy the file through this client
        // to move it. Such object stores only show an object once it is complete.
        write(localFile, target);
        return;
      }
      partial = upload;
      write(localFile, partial);
      if (isStaged(target, localFile)) {
        // Another run staged the same content meanwhile, and its driver may be reading it
        return;
      }
      try {
        partial.moveTo(target);
        partial = null;
      } catch (FileSystemException e) {
        // On S3 a rename is a copy. The store can make the copy and answer in a way its client
        // can't read (#9760): the file under its final name is then complete, and the partial
        // one goes below.
        if (!isStaged(target, localFile)) {
          throw e;
        }
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to stage " + localFile + " to " + location(name), e);
    } finally {
      deleteQuietly(partial);
    }
  }

  /** The file is there under its final name, with the size of the local file. */
  private static boolean isStaged(FileObject target, Path localFile) throws IOException {
    target.refresh();
    return target.exists() && target.getContent().getSize() == Files.size(localFile);
  }

  private static void write(Path localFile, FileObject target) throws Exception {
    try (InputStream in = Files.newInputStream(localFile);
        OutputStream out = HopVfs.getOutputStream(target, false)) {
      in.transferTo(out);
    }
  }

  /** A partial upload left behind goes with its staged file in a later cleanup. */
  private static void deleteQuietly(FileObject file) {
    if (file == null) {
      return;
    }
    try {
      file.delete();
    } catch (Exception e) {
      // Removed by a cleanup together with its staged file
    }
  }

  @Override
  public boolean supportsCleanup() {
    return true;
  }

  @Override
  public List<StagedFile> list() throws HopException {
    List<StagedFile> files = new ArrayList<>();
    try {
      FileObject folderObject = HopVfs.getFileObject(folder, variables);
      requireKnownScheme(folder, folderObject);
      if (!folderObject.exists()) {
        return files;
      }
      for (FileObject child : folderObject.getChildren()) {
        if (child.getType() == FileType.FILE) {
          files.add(
              new StagedFile(
                  child.getName().getBaseName(),
                  child.getContent().getSize(),
                  child.getContent().getLastModifiedTime()));
        }
      }
      return files;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to list staging folder " + folder, e);
    }
  }

  @Override
  public void delete(String name) throws HopException {
    try {
      file(name).delete();
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to delete " + location(name), e);
    }
  }

  @Override
  public void writeText(String name, String text) throws HopException {
    try {
      FileObject target = file(name);
      createFolder(target.getParent());
      try (OutputStream out = HopVfs.getOutputStream(target, false)) {
        out.write(text.getBytes(StandardCharsets.UTF_8));
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to write " + location(name), e);
    }
  }

  @Override
  public String readText(String name) throws HopException {
    try {
      FileObject source = file(name);
      if (!source.exists()) {
        return null;
      }
      try (InputStream in = HopVfs.getInputStream(source)) {
        return new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to read " + location(name), e);
    }
  }

  /** The staging folder, resolved. */
  public String getFolder() {
    return folder;
  }

  @Override
  public String location(String name) {
    return folder + "/" + name;
  }

  @Override
  public String clusterUrl(String name) {
    return clusterFolder + "/" + name;
  }

  private FileObject file(String name) throws HopException {
    String target = location(name);
    FileObject file = HopVfs.getFileObject(target, variables);
    requireKnownScheme(target, file);
    return file;
  }

  /**
   * Hop VFS resolves a URL with a scheme it does not know as a relative local path: {@code
   * minio://bucket} would quietly become a folder named {@code minio:} on this disk, and the
   * cluster would never find the file.
   */
  void requireKnownScheme(String target, FileObject resolved) throws HopException {
    int colon = target.indexOf("://");
    if (colon <= 0) {
      return;
    }
    String scheme = target.substring(0, colon);
    String resolvedScheme = resolved.getName().getScheme();
    if (!scheme.equalsIgnoreCase(resolvedScheme)) {
      throw new HopException(
          "Hop VFS does not know the scheme '"
              + scheme
              + "' of staging folder "
              + folder
              + " (it resolved to "
              + resolved.getName().getURI()
              + "). Use the name of a VFS connection of this project (MinIO, S3, Azure, Google,"
              + " HDFS, Databricks, ...) or a scheme of an installed VFS plugin.");
    }
  }

  /** Local and HDFS folders must exist; object stores have no folders and may refuse this. */
  private static void createFolder(FileObject folderObject) {
    try {
      if (folderObject != null && !folderObject.exists()) {
        folderObject.createFolder();
      }
    } catch (Exception e) {
      // The upload reports a folder that is really missing
    }
  }

  public static String trimSlash(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    while (trimmed.endsWith("/") && !trimmed.endsWith("://")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }
}
