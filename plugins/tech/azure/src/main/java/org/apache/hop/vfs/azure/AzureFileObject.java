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
 *
 */

package org.apache.hop.vfs.azure;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.Context;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.ListFileSystemsOptions;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.options.DataLakePathDeleteOptions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.hop.core.exception.HopException;

public class AzureFileObject extends AbstractFileObject<AzureFileSystem> {

  public class BlockBlobOutputStream extends OutputStream {

    private final OutputStream outputStream;
    long written = 0;

    public BlockBlobOutputStream(OutputStream outputStream) {
      this.outputStream = outputStream;
    }

    @Override
    public void write(int b) throws IOException {
      outputStream.write(b);
      written(1);
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    protected void written(int len) {
      written += len;
      lastModified = System.currentTimeMillis();
      size = written;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      outputStream.write(b, off, len);
      written(len);
    }

    @Override
    public void flush() throws IOException {
      super.flush();
    }

    @Override
    public void close() throws IOException {
      outputStream.close();
    }
  }

  private final DataLakeServiceClient service;
  private boolean attached = false;
  private long size;
  private long lastModified;
  private FileType type;
  private List<String> children = null;
  private DataLakeFileClient dataLakeFileClient;
  private String currentFilePath;
  private PathItem pathItem;
  private PathItem dirPathItem;
  private final String markerFileName = ".cvfs.temp";
  private OutputStream blobOutputStream;
  private String containerName;

  public AzureFileObject(
      AbstractFileName fileName, AzureFileSystem fileSystem, DataLakeServiceClient service)
      throws FileSystemException {
    super(fileName, fileSystem);
    this.service = service;
  }

  @Override
  protected void doAttach() throws HopException {
    if (attached) {
      return;
    }
    containerName = ((AzureFileName) getName()).getContainer();
    String fullPath = ((AzureFileName) getName()).getPath();
    ListPathsOptions lpo = new ListPathsOptions();
    children = new ArrayList<>();
    if (isFileSystemRoot(fullPath)) {
      service
          .listFileSystems()
          .iterator()
          .forEachRemaining(
              item -> {
                String containerName = item.getName();
                // Extract just the container name (remove any path after last /)
                String cleanName = StringUtils.substringAfterLast(containerName, "/");
                // If substringAfterLast returns empty, use the full name
                if (StringUtils.isEmpty(cleanName)) {
                  cleanName = containerName;
                }
                // Only add valid, non-empty container names
                if (!StringUtils.isEmpty(cleanName)
                    && !cleanName.equals(".")
                    && !cleanName.equals("..")) {
                  children.add(cleanName);
                }
              });

      size = children.size();
      lastModified = 0;
      type = FileType.FOLDER;
      dataLakeFileClient = null;
      currentFilePath = "";
      return; // Early return for root level - no need to get fileSystemClient
    }

    // Get the fileSystemClient for container and below
    DataLakeFileSystemClient fileSystemClient = service.getFileSystemClient(containerName);

    if (isContainer(fullPath)) {
      if (containerExists()) {
        type = FileType.FOLDER;
        ListPathsOptions rootLpo = new ListPathsOptions();
        rootLpo.setRecursive(false);
        fileSystemClient
            .listPaths(rootLpo, null)
            .forEach(
                pi -> {
                  String childName = pi.getName();
                  // Only add non-empty, valid child names
                  if (!childName.isEmpty() && !childName.equals(".") && !childName.equals("..")) {
                    children.add(childName);
                  }
                });
      } else {
        type = FileType.IMAGINARY;
        throw new HopException("Container does not exist: " + fullPath);
      }
    } else {
      // this is a subdirectory or file or a container/file system
      currentFilePath = ((AzureFileName) getName()).getPathAfterContainer();
      if (StringUtils.isEmpty(currentFilePath)) {
        type = FileType.FOLDER;
        ListPathsOptions rootLpo = new ListPathsOptions();
        rootLpo.setRecursive(false);
        fileSystemClient
            .listPaths(rootLpo, null)
            .forEach(
                pi -> {
                  String childName = pi.getName();
                  // Only add non-empty, valid child names
                  if (!childName.isEmpty() && !childName.equals(".") && !childName.equals("..")) {
                    children.add(childName);
                  }
                });
      } else {
        lpo.setPath(currentFilePath);
        DataLakeDirectoryClient directoryClient =
            fileSystemClient.getDirectoryClient(currentFilePath);
        final Boolean exists = directoryClient.exists();

        final Boolean isDirectory =
            exists
                && fileSystemClient
                    .getDirectoryClient(currentFilePath)
                    .getProperties()
                    .isDirectory();
        final Boolean isFile = !isDirectory;
        if (exists && isDirectory) {
          children = new ArrayList<>();
          lpo.setRecursive(false); // Only get immediate children, not recursive
          PagedIterable<PathItem> pathItems = fileSystemClient.listPaths(lpo, null);

          // Normalize the current path for comparison
          final String normalizedCurrentPath;
          String tempPath = StringUtils.removeStart(currentFilePath, "/");
          if (!tempPath.isEmpty() && !tempPath.endsWith("/")) {
            normalizedCurrentPath = tempPath + "/";
          } else {
            normalizedCurrentPath = tempPath;
          }

          pathItems.forEach(
              item -> {
                String itemName = item.getName();
                String childName;

                // Remove the current directory prefix to get the child name
                if (!normalizedCurrentPath.isEmpty()
                    && itemName.startsWith(normalizedCurrentPath)) {
                  childName = itemName.substring(normalizedCurrentPath.length());
                } else {
                  childName = itemName;
                }

                // Remove any leading slashes
                childName = StringUtils.removeStart(childName, "/");

                // Only add non-empty, valid child names
                if (!childName.isEmpty() && !childName.equals(".") && !childName.equals("..")) {
                  children.add(childName);
                }
              });
          size = children.size();
          type = FileType.FOLDER;
          lastModified = directoryClient.getProperties().getLastModified().toEpochSecond() * 1000L;
        } else if (exists && isFile) {
          dataLakeFileClient = fileSystemClient.getFileClient(currentFilePath);
          size = dataLakeFileClient.getProperties().getFileSize();
          type = FileType.FILE;
          lastModified =
              dataLakeFileClient.getProperties().getLastModified().toEpochSecond() * 1000L;
        } else {
          lastModified = 0;
          type = FileType.IMAGINARY;
          size = 0;
          pathItem = null;
          dirPathItem = null;
        }
      }
    }
  }

  private boolean containerExists() {
    String containerName = ((AzureFileName) getName()).getContainer();
    ListFileSystemsOptions fileSystemsOptions = new ListFileSystemsOptions();
    fileSystemsOptions.setPrefix(containerName);

    final DataLakeFileSystemClient fileSystemClient = service.getFileSystemClient(containerName);

    try {
      return fileSystemClient.existsWithResponse(Duration.ofSeconds(5), Context.NONE).getValue();
    } catch (IllegalStateException e) {
      return false;
    }
  }

  private boolean isContainer(String fullPath) {
    final String container = ((AzureFileName) getName()).getContainer();
    final String fullPathWithoutTralilingSlash = StringUtils.removeStart(fullPath, "/");
    if (StringUtils.equals(container, fullPathWithoutTralilingSlash)
        && !StringUtils.isEmpty(fullPathWithoutTralilingSlash)) {
      return true;
    }
    return false;
  }

  private static boolean isFileSystemRoot(String fullPath) {
    return "/".equals(fullPath);
  }

  private String getFilePath(String filename) {
    return filename.substring(filename.indexOf('/'));
  }

  @Override
  protected void doDetach() {
    if (this.attached) {
      this.attached = false;
      this.children = null;
      this.size = 0;
      this.type = null;
      this.dataLakeFileClient = null;
      this.currentFilePath = null;
      this.pathItem = null;
      this.dirPathItem = null;
    }
  }

  @Override
  protected void onChange() throws IOException {
    this.refresh();
  }

  @Override
  protected boolean doIsHidden() throws Exception {
    return getName().getBaseName().equals(markerFileName);
  }

  /**
   * Check if the file can be renamed to the new file This is not a feature supported by Azure SDK,
   * but we can implement renaming by copying the file to the new location and deleting the old one.
   * So renaming a file is possible
   *
   * @param newfile the new file
   * @return
   */
  @Override
  public boolean canRenameTo(FileObject newfile) {
    return true;
  }

  @Override
  protected void doDelete() throws Exception {
    DataLakeFileSystemClient fileSystemClient = service.getFileSystemClient(containerName);
    DataLakeFileClient fileClient = fileSystemClient.getFileClient(currentFilePath.substring(1));
    if (fileClient == null) {
      throw new UnsupportedOperationException();
    } else {
      FileObject parent = getParent();
      boolean lastFile = ((AzureFileObject) parent).doListChildren().length == 1;
      try {
        // Create delete options with recursive=true to handle non-empty directories
        DataLakePathDeleteOptions deleteOptions = new DataLakePathDeleteOptions();
        deleteOptions.setIsRecursive(true);

        if (currentFilePath.equals("")) {
          fileClient.deleteIfExistsWithResponse(deleteOptions, null, null);
        } else {
          if (StringUtils.isNotEmpty(currentFilePath) && fileClient.exists()) {
            fileClient.deleteIfExistsWithResponse(deleteOptions, null, null);
          } else if (dirPathItem != null) {
            // For directories, use the directory client with recursive delete
            DataLakeDirectoryClient directoryClient =
                fileSystemClient.getDirectoryClient(currentFilePath);
            if (directoryClient.exists()) {
              directoryClient.deleteIfExistsWithResponse(deleteOptions, null, null);
            }
          } else {
            throw new UnsupportedOperationException();
          }
          // If this was the last file in the create, we create a new
          // marker file to keep the directory open
          if (lastFile) {
            FileObject marker = parent.resolveFile(markerFileName);
            marker.createFile();
          }
        }
      } finally {
        type = FileType.IMAGINARY;
        children = null;
        size = 0;
        lastModified = 0;
      }
    }
  }

  @Override
  protected boolean doIsSameFile(FileObject destFile) throws FileSystemException {
    return true;
  }

  @Override
  protected void doRename(FileObject newfile) throws Exception {
    if (!StringUtils.isEmpty(currentFilePath)) {
      DataLakeFileSystemClient fileSystemClient = service.getFileSystemClient(containerName);
      DataLakeFileClient fileClient = fileSystemClient.getFileClient(currentFilePath.substring(1));

      // Get the new blob reference
      //      CloudBlobContainer newContainer =
      //          service.getContainerReference(((AzureFileName) newfile.getName()).getContainer());
      //      CloudBlob newBlob =
      //          newContainer.getBlobReferenceFromServer(
      //              ((AzureFileName) newfile.getName()).getPathAfterContainer().substring(1));
      // Start the copy operation
      fileClient.rename(
          containerName, ((AzureFileName) newfile.getName()).getPathAfterContainer().substring(1));
      //      newBlob.startCopy(cloudBlob.getUri());
      // Delete the original blob
      // doDelete();
    } else {
      throw new FileSystemException("Renaming of directories not supported on this file.");
    }
  }

  @Override
  protected void doCreateFolder() {
    // create a folder, we already know the path
    service.getFileSystemClient(containerName).createDirectory(currentFilePath.substring(1));
  }

  @Override
  protected long doGetLastModifiedTime() throws Exception {
    return lastModified;
  }

  @Override
  protected OutputStream doGetOutputStream(boolean bAppend) throws Exception {
    if (StringUtils.isEmpty(currentFilePath)) {
      throw new UnsupportedOperationException();
    }
    DataLakeFileSystemClient fileSystemClient = service.getFileSystemClient(containerName);
    DataLakeFileClient dataLakeFileClient = fileSystemClient.getFileClient(currentFilePath);
    if (dataLakeFileClient != null) {
      if (bAppend) {
        throw new UnsupportedOperationException();
      }
      type = FileType.FILE;
      return new BlockBlobOutputStream(dataLakeFileClient.getOutputStream());
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    if (!currentFilePath.equals("") && type == FileType.FILE) {
      DataLakeFileSystemClient fileSystemClient = service.getFileSystemClient(containerName);
      DataLakeFileClient fileClient = fileSystemClient.getFileClient(currentFilePath);
      if (!fileSystemClient.exists() || !fileClient.exists()) {
        throw new FileSystemException("File not found: " + currentFilePath);
      }
      return new BlobInputStream(fileClient.openInputStream(), size);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected FileType doGetType() throws Exception {
    return type;
  }

  @Override
  protected String[] doListChildren() throws Exception {
    return children == null
        ? ArrayUtils.toStringArray(getChildren())
        : children.toArray(new String[0]);
  }

  @Override
  public FileObject[] getChildren() throws FileSystemException {
    return super.getChildren();
  }

  @Override
  protected long doGetContentSize() throws Exception {
    return size;
  }

  @Override
  public boolean delete() throws FileSystemException {
    if (dataLakeFileClient.exists()) {
      try {
        doDelete();
        return true;
      } catch (Exception e) {
        return false;
        // TODO log an error
      }
    }
    return false;
  }

  private static String removeTrailingSlash(String itemPath) {
    while (itemPath.endsWith("/")) itemPath = itemPath.substring(0, itemPath.length() - 1);
    return itemPath;
  }

  private static String removeLeadingSlash(String relpath) {
    while (relpath.startsWith("/")) relpath = relpath.substring(1);
    return relpath;
  }
}
