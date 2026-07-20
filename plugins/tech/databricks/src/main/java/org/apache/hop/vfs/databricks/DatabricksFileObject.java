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

package org.apache.hop.vfs.databricks;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileNotFoundException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.databricks.client.DatabricksFilesClient;
import org.apache.hop.databricks.client.DirectoryEntry;
import org.apache.hop.databricks.client.RestDatabricksFilesClient;
import org.apache.hop.databricks.client.WorkspaceFileMetadata;
import org.apache.hop.vfs.databricks.DatabricksListCache.ChildInfo;

/** File object backed by the Databricks Files API (UC Volumes / Workspace). */
public class DatabricksFileObject extends AbstractFileObject<DatabricksFileSystem> {

  private FileType type = FileType.IMAGINARY;
  private long size = -1L;
  private long lastModified = 0L;
  private List<DirectoryEntry> listedChildren;

  protected DatabricksFileObject(final AbstractFileName name, final DatabricksFileSystem fileSystem)
      throws FileSystemException {
    super(name, fileSystem);
  }

  private DatabricksFilesClient client() throws FileSystemException {
    return getAbstractFileSystem().getClient();
  }

  /**
   * Absolute workspace path for the Files API. When the VFS connection has a root path, logical
   * paths such as {@code /input} map under that root.
   */
  String workspacePath() {
    return DatabricksPathResolver.toWorkspacePath(
        getName().getPath(), getAbstractFileSystem().getRootPath());
  }

  @Override
  protected void doAttach() throws Exception {
    String path = workspacePath();
    if ("/".equals(path)) {
      // No root configured — virtual multi-volume root
      type = FileType.FOLDER;
      size = -1L;
      lastModified = 0L;
      return;
    }
    if (!RestDatabricksFilesClient.isFilesApiPath(path)) {
      if ("/Volumes".equals(path) || "/Workspace".equals(path)) {
        type = FileType.FOLDER;
        size = -1L;
        lastModified = 0L;
        return;
      }
      type = FileType.IMAGINARY;
      size = -1L;
      lastModified = 0L;
      return;
    }

    // Prefer metadata from a recent parent directory listing (includes last_modified).
    ChildInfo cached = getAbstractFileSystem().getListCache().get(path);
    if (cached != null) {
      type = cached.type;
      size = cached.size;
      lastModified = cached.lastModifiedEpochMs;
      if (type == FileType.FOLDER) {
        // Keep listedChildren null until doListChildren; type is enough for attach.
      }
      return;
    }

    // Configured scheme root is always a folder (the volume / workspace dir exists or is managed)
    if (getAbstractFileSystem().hasRootPath()
        && path.equals(getAbstractFileSystem().getRootPath())) {
      try {
        listedChildren = client().listDirectory(path);
        rememberListCache(path, listedChildren);
        type = FileType.FOLDER;
        size = -1L;
        lastModified = 0L;
      } catch (HopException e) {
        // Still treat as folder so create child works even if list fails (empty / permission)
        type = FileType.FOLDER;
        size = -1L;
        lastModified = 0L;
        listedChildren = null;
      }
      return;
    }

    DatabricksFilesClient client = client();
    WorkspaceFileMetadata meta = client.getFileMetadata(path);
    if (meta.exists()) {
      type = FileType.FILE;
      size = meta.sizeBytes();
      // HEAD does not reliably expose last_modified; leave 0 (unknown) unless list cache filled it.
      lastModified = 0L;
      return;
    }
    try {
      listedChildren = client.listDirectory(path);
      rememberListCache(path, listedChildren);
      type = FileType.FOLDER;
      size = -1L;
      lastModified = 0L;
    } catch (HopException e) {
      type = FileType.IMAGINARY;
      size = -1L;
      lastModified = 0L;
      listedChildren = null;
    }
  }

  private void rememberListCache(String parentPath, List<DirectoryEntry> entries) {
    if (entries == null || entries.isEmpty()) {
      return;
    }
    Map<String, ChildInfo> map = new LinkedHashMap<>();
    for (DirectoryEntry entry : entries) {
      if (entry.path() != null) {
        map.put(DatabricksListCache.normalizePath(entry.path()), ChildInfo.fromEntry(entry));
      }
    }
    getAbstractFileSystem().getListCache().put(parentPath, map);
  }

  @Override
  protected void doDetach() {
    type = FileType.IMAGINARY;
    size = -1L;
    lastModified = 0L;
    listedChildren = null;
  }

  @Override
  protected FileType doGetType() {
    return type;
  }

  @Override
  protected long doGetContentSize() {
    return size;
  }

  @Override
  protected long doGetLastModifiedTime() {
    return lastModified;
  }

  @Override
  protected String[] doListChildren() throws Exception {
    if (type != FileType.FOLDER) {
      return null;
    }
    String path = workspacePath();
    if ("/".equals(path)) {
      return new String[] {"Volumes/", "Workspace/"};
    }
    if ("/Volumes".equals(path) || "/Workspace".equals(path)) {
      return new String[0];
    }
    if (listedChildren == null) {
      listedChildren = client().listDirectory(path);
      rememberListCache(path, listedChildren);
    }
    return listedChildren.stream()
        .map(
            e -> {
              String n = e.name();
              if (e.directory() && n != null && !n.endsWith("/")) {
                return n + "/";
              }
              return n;
            })
        .toArray(String[]::new);
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    if (type != FileType.FILE) {
      throw new FileNotFoundException(getName());
    }
    return client().openInputStream(workspacePath());
  }

  @Override
  protected OutputStream doGetOutputStream(boolean append) throws Exception {
    if (append) {
      throw new FileSystemException(
          "vfs.provider/write-append-not-supported.error", workspacePath());
    }
    String path = workspacePath();
    if (!RestDatabricksFilesClient.isFilesApiPath(path)) {
      throw new FileSystemException(
          "Databricks VFS only supports /Volumes/… and /Workspace/… paths"
              + (getAbstractFileSystem().hasRootPath()
                  ? " (or paths under the connection root path)"
                  : "")
              + ", got workspace path: "
              + path
              + " from logical path: "
              + getName().getPath());
    }
    Path temp = Files.createTempFile("hop-dbx-vfs-", ".bin");
    OutputStream fileOut = Files.newOutputStream(temp);
    return new FilterOutputStream(fileOut) {
      private boolean closed;

      @Override
      public void close() throws IOException {
        if (closed) {
          return;
        }
        closed = true;
        try {
          super.close();
          try {
            client().upload(temp, path);
          } catch (FileSystemException e) {
            throw new IOException(e);
          } catch (HopException e) {
            throw new IOException(e);
          }
          try {
            type = FileType.FILE;
            size = Files.size(temp);
            lastModified = System.currentTimeMillis();
            getAbstractFileSystem().getListCache().invalidateParentOf(path);
          } catch (IOException ignored) {
            type = FileType.FILE;
            lastModified = System.currentTimeMillis();
          }
        } finally {
          try {
            Files.deleteIfExists(temp);
          } catch (IOException ignored) {
            // best effort
          }
        }
      }
    };
  }

  @Override
  protected void doCreateFolder() throws Exception {
    String path = workspacePath();
    // VFS createFile/getOutputStream walks parents. Volume roots and the configured scheme root
    // already exist (or cannot be created via Files API) — treat as present.
    if (isPreexistingWorkspaceRoot(path, getAbstractFileSystem().getRootPath())) {
      type = FileType.FOLDER;
      return;
    }
    if (!RestDatabricksFilesClient.isFilesApiPath(path)) {
      throw new FileSystemException(
          "Databricks VFS only supports creating folders under /Volumes/… or /Workspace/…, got: "
              + path
              + " (logical: "
              + getName().getPath()
              + (getAbstractFileSystem().hasRootPath()
                  ? ", root: " + getAbstractFileSystem().getRootPath()
                  : "")
              + ")");
    }
    client().createDirectory(path);
    type = FileType.FOLDER;
    listedChildren = null;
    lastModified = System.currentTimeMillis();
    getAbstractFileSystem().getListCache().invalidateParentOf(path);
  }

  /**
   * Paths that are not user-created directories via Files API: filesystem root, {@code /Volumes},
   * {@code /Workspace}, catalog/schema/volume levels, shallow Workspace roots, and the configured
   * scheme root itself.
   */
  static boolean isPreexistingWorkspaceRoot(String path, String schemeRoot) {
    if (StringUtils.isNotBlank(schemeRoot) && schemeRoot.equals(path)) {
      return true;
    }
    if (StringUtils.isBlank(path) || "/".equals(path)) {
      return true;
    }
    if ("/Volumes".equals(path) || "/Workspace".equals(path)) {
      return true;
    }
    // /Volumes/<catalog>/<schema>/<volume> — volume object must already exist
    if (path.startsWith("/Volumes/")) {
      // "", "Volumes", catalog, schema, volume → length 5
      return path.split("/", -1).length <= 5;
    }
    // /Workspace/Users and similar shallow trees are managed by the workspace
    if (path.startsWith("/Workspace/")) {
      return path.split("/", -1).length <= 3;
    }
    return false;
  }

  @Override
  protected void doDelete() throws Exception {
    String path = workspacePath();
    if (!RestDatabricksFilesClient.isFilesApiPath(path)) {
      throw new FileSystemException("Cannot delete path: " + path);
    }
    if (getAbstractFileSystem().hasRootPath()
        && path.equals(getAbstractFileSystem().getRootPath())) {
      throw new FileSystemException("Cannot delete the configured VFS root path: " + path);
    }
    if (type == FileType.FOLDER) {
      client().deleteDirectory(path, true);
    } else {
      client().deleteFile(path);
    }
    type = FileType.IMAGINARY;
    listedChildren = null;
    lastModified = 0L;
    getAbstractFileSystem().getListCache().invalidateParentOf(path);
  }

  /**
   * Files API has no rename endpoint: copy via download+upload then delete the source (same pattern
   * as MinIO object rename). Folders are not supported.
   */
  @Override
  protected void doRename(FileObject newFile) throws Exception {
    if (getType() == FileType.FOLDER) {
      throw new FileSystemException(
          "Databricks VFS does not support renaming folders (move files individually): "
              + workspacePath());
    }
    if (!(newFile instanceof DatabricksFileObject dest)) {
      throw new FileSystemException(
          "Cannot rename Databricks VFS file to a different file system: " + newFile);
    }
    String sourcePath = workspacePath();
    String destPath = dest.workspacePath();
    if (!RestDatabricksFilesClient.isFilesApiPath(sourcePath)
        || !RestDatabricksFilesClient.isFilesApiPath(destPath)) {
      throw new FileSystemException(
          "Rename requires Files API paths under /Volumes or /Workspace (source="
              + sourcePath
              + ", dest="
              + destPath
              + ")");
    }
    if (sourcePath.equals(destPath)) {
      return;
    }
    Path temp = Files.createTempFile("hop-dbx-rename-", ".bin");
    try {
      try (InputStream in = client().openInputStream(sourcePath)) {
        Files.copy(in, temp, StandardCopyOption.REPLACE_EXISTING);
      }
      client().upload(temp, destPath);
      client().deleteFile(sourcePath);
    } catch (HopException e) {
      throw new FileSystemException(
          "Failed to rename " + sourcePath + " to " + destPath + " via Files API", e);
    } finally {
      try {
        Files.deleteIfExists(temp);
      } catch (IOException ignored) {
        // best effort
      }
    }
    type = FileType.IMAGINARY;
    listedChildren = null;
  }
}
