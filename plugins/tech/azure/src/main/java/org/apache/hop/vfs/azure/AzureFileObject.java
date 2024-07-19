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
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;

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

  public AzureFileObject(
      AbstractFileName fileName, AzureFileSystem fileSystem, DataLakeServiceClient service)
      throws FileSystemException {
    super(fileName, fileSystem);
    this.service = service;
  }

  @Override
  protected void doAttach() {
    if (!attached) {
      String containerName = ((AzureFileName) getName()).getContainer();
      String fullPath = ((AzureFileName) getName()).getPath();
      DataLakeFileSystemClient fileSystemClient = service.getFileSystemClient(containerName);
      ListPathsOptions lpo = new ListPathsOptions();

      if (isFileSystemRoot(fullPath)) { // ROOT of the filesystem
        children = new ArrayList<>();
        lpo.setPath(fullPath);
        // TODO SR Evaluate using lpo.setRecursive

        service
            .listFileSystems()
            .iterator()
            .forEachRemaining(
                item -> {
                  children.add(item.getName());
                });

        size = children.size();
        lastModified = 0;
        type = FileType.FOLDER;
        dataLakeFileClient = null;
        currentFilePath = "";
      } else { // this is a subdirectory or file or a container

        currentFilePath = ((AzureFileName) getName()).getPathAfterContainer();
        lpo.setPath(currentFilePath);
        // TODO SR Evaluate using lpo.setRecursive
        // dataLakeFileClient =
        //   fileSystemClient.getFileClient(((AzureFileName) getName()).getContainer());
        //        DataLakeDirectoryClient directoryClient =
        //            fileSystemClient.getDirectoryClient(currentFilePath);
        // DataLakeFileClient fileClient = fileSystemClient.getFileClient(currentFilePath);
        // final Boolean exists = directoryClient.exists();
        final Boolean isDirectory =
            fileSystemClient.getDirectoryClient(currentFilePath).getProperties().isDirectory();

        if (isDirectory) {
          type = FileType.FOLDER;
          children = new ArrayList<>();
          PagedIterable<PathItem> pathItems = fileSystemClient.listPaths(lpo, null);
          pathItems.forEach(
              item -> {
                if (item.isDirectory()) {
                  children.add(item.getName());
                }
              });
        }
        if (!isDirectory) {
          DataLakeFileClient fileClient = fileSystemClient.getFileClient(currentFilePath);
          size = fileClient.getProperties().getFileSize();
          type = FileType.FILE;
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

  private static boolean isFileSystemRoot(String fullPath) {
    return "/".equals(fullPath);
  }

  private String getFilePath(String filename) {
    String filePath = filename.substring(filename.indexOf('/'), filename.length());
    return filePath;
  }

  private boolean pathsMatch(String path, String currentPath) {
    return path.replace("/" + ((AzureFileSystem) getFileSystem()).getAccount(), "")
        .equals(currentPath);
  }

  @Override
  protected void doDetach() throws Exception {
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

  public boolean canRenameTo(FileObject newfile) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void doDelete() throws Exception {

    DataLakeFileSystemClient fileSystemClient =
        service.getFileSystemClient(getAbstractFileSystem().getFilesystemName());

    if (dataLakeFileClient == null) {
      throw new UnsupportedOperationException();
    } else {
      FileObject parent = getParent();
      boolean lastFile = ((AzureFileObject) parent).doListChildren().length == 1;
      try {
        if (currentFilePath.equals("")) {
          dataLakeFileClient.delete();
        } else {
          if (pathItem != null) {
            DataLakeFileClient dataLakeFileClient =
                fileSystemClient.getFileClient(pathItem.getName());
            dataLakeFileClient.delete();
          } else if (dirPathItem != null) {
            ListPathsOptions lpo = new ListPathsOptions();
            lpo.setPath(((AzureFileName) getName()).getPathAfterContainer());
            // TODO SR Evaluate usage of lpo.setRecursive(true)

            fileSystemClient
                .listPaths(lpo, null)
                .forEach(
                    pi -> {
                      if (!pi.isDirectory()
                          && getFilePath(pi.getName()).startsWith(getName().getPath())) {
                        DataLakeFileClient dataLakeFileClient =
                            fileSystemClient.getFileClient(pathItem.getName());
                        dataLakeFileClient.delete();
                      }
                    });
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
    if (pathItem != null) {
      DataLakeFileSystemClient fileSystemClient =
          service.getFileSystemClient(getAbstractFileSystem().getFilesystemName());

      // Get the new blob reference
      //      CloudBlobContainer newContainer =
      //          service.getContainerReference(((AzureFileName) newfile.getName()).getContainer());
      //      CloudBlob newBlob =
      //          newContainer.getBlobReferenceFromServer(
      //              ((AzureFileName) newfile.getName()).getPathAfterContainer().substring(1));
      DataLakeFileClient fileClient = fileSystemClient.getFileClient(pathItem.getName());
      // Start the copy operation
      fileClient.rename(
          getAbstractFileSystem().getFilesystemName(),
          ((AzureFileName) newfile.getName()).getPathAfterContainer().substring(1));
      //      newBlob.startCopy(cloudBlob.getUri());
      // Delete the original blob
      doDelete();
    } else {
      throw new FileSystemException("Renaming of directories not supported on this file.");
    }
  }

  @Override
  protected long doGetLastModifiedTime() throws Exception {
    return lastModified;
  }

  @Override
  protected OutputStream doGetOutputStream(boolean bAppend) throws Exception {
    if (dataLakeFileClient != null && !currentFilePath.equals("")) {
      if (bAppend) throw new UnsupportedOperationException();
      type = FileType.FILE;
      return new BlockBlobOutputStream(dataLakeFileClient.getOutputStream());
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    if (dataLakeFileClient != null && !currentFilePath.equals("") && type == FileType.FILE) {
      DataLakeFileSystemClient fileSystemClient =
          service.getFileSystemClient(getAbstractFileSystem().getFilesystemName());
      DataLakeFileClient dataLakeFileClient = fileSystemClient.getFileClient(pathItem.getName());
      return new BlobInputStream(dataLakeFileClient.openInputStream(), size);
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
    return children == null ? null : children.toArray(new String[0]);
  }

  @Override
  public FileObject[] getChildren() throws FileSystemException {
    return super.getChildren();
  }

  @Override
  protected long doGetContentSize() throws Exception {
    return size;
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
