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

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.UriParser;

public class AzureFileObject extends AbstractFileObject<AzureFileSystem> {

  public class BlockBlobOutputStream extends OutputStream {

    private final CloudBlockBlob bb;
    private final OutputStream outputStream;
    long written = 0;

    public BlockBlobOutputStream(CloudBlockBlob bb, OutputStream outputStream) {
      this.bb = bb;
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

  private final CloudBlobClient service;
  private boolean attached = false;
  private long size;
  private long lastModified;
  private FileType type;
  private List<String> children = null;
  private CloudBlobContainer container;
  private String containerPath;
  private CloudBlob cloudBlob;
  private CloudBlobDirectory cloudDir;
  private final String markerFileName = ".cvfs.temp";
  private OutputStream blobOutputStream;

  public AzureFileObject(
      AbstractFileName fileName, AzureFileSystem fileSystem, CloudBlobClient service)
      throws FileSystemException {
    super(fileName, fileSystem);
    this.service = service;
  }

  @Override
  protected void doAttach() throws URISyntaxException, StorageException {
    if (!attached) {
      if (getName().getPath().equals("/")) {
        children = new ArrayList<>();
        for (CloudBlobContainer container : service.listContainers()) {
          children.add(container.getName());
        }
        size = children.size();
        lastModified = 0;
        type = FileType.FOLDER;
        container = null;
        containerPath = "";
      } else {
        String containerName = ((AzureFileName) getName()).getContainer();
        container = service.getContainerReference(containerName);
        containerPath = ((AzureFileName) getName()).getPathAfterContainer();
        String thisPath = "/" + containerName + containerPath;
        if (container.exists()) {
          children = new ArrayList<>();
          if (containerPath.equals("")) {
            if (container.exists()) {
              for (ListBlobItem item : container.listBlobs()) {
                StringBuilder path = new StringBuilder(item.getUri().getPath());
                UriParser.extractFirstElement(path);
                children.add(path.substring(1));
              }
              type = FileType.FOLDER;
            } else {
              type = FileType.IMAGINARY;
            }
            lastModified = 0;
            size = children.size();
          } else {
            /*
             * Look in the parent path for this filename AND and
             * direct descendents
             */
            cloudBlob = null;
            cloudDir = null;
            String relpath =
                removeLeadingSlash(
                    ((AzureFileName) (getName().getParent())).getPathAfterContainer());
            for (ListBlobItem item :
                relpath.equals("") ? container.listBlobs() : container.listBlobs(relpath + "/")) {
              String itemPath = removeTrailingSlash(item.getUri().getPath());
              if (pathsMatch(itemPath, thisPath)) {
                if (item instanceof CloudBlob) {
                  cloudBlob = (CloudBlob) item;
                } else {
                  cloudDir = (CloudBlobDirectory) item;
                  for (ListBlobItem blob : cloudDir.listBlobs()) {
                    URI blobUri = blob.getUri();
                    String path = blobUri.getPath();
                    while (path.endsWith("/")) path = path.substring(0, path.length() - 1);
                    int idx = path.lastIndexOf('/');
                    if (idx != -1) path = path.substring(idx + 1);
                    children.add(path);
                  }
                }
                break;
              }
            }
            if (cloudBlob != null) {
              type = FileType.FILE;
              size = cloudBlob.getProperties().getLength();
              if (cloudBlob.getMetadata().containsKey("ActualLength")) {
                size = Long.parseLong(cloudBlob.getMetadata().get("ActualLength"));
              }
              String disp = cloudBlob.getProperties().getContentDisposition();
              if (disp != null && disp.startsWith("vfs ; length=\"")) {
                size = Long.parseLong(disp.substring(14, disp.length() - 1));
              }
              Date lastModified2 = cloudBlob.getProperties().getLastModified();
              lastModified = lastModified2 == null ? 0 : lastModified2.getTime();
            } else if (cloudDir != null) {
              type = FileType.FOLDER;
              size = children.size();
              lastModified = 0;
            } else {
              lastModified = 0;
              type = FileType.IMAGINARY;
              size = 0;
            }
          }
        } else {
          lastModified = 0;
          type = FileType.IMAGINARY;
          size = 0;
          cloudBlob = null;
          cloudDir = null;
        }
      }
    }
  }

  @Override
  public FileType getType() throws FileSystemException {
    if (type == null) {
      type = super.getType();
    }
    return type;
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
      this.container = null;
      this.containerPath = null;
      this.cloudBlob = null;
      this.cloudDir = null;
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
    return true;
  }

  @Override
  protected void doDelete() throws Exception {
    doAttach();
    if (container == null) {
      throw new UnsupportedOperationException();
    } else {
      FileObject parent = getParent();
      boolean lastFile = ((AzureFileObject) parent).doListChildren().length == 1;
      try {
        if (containerPath.equals("")) {
          container.delete();
        } else {
          if (cloudBlob != null) cloudBlob.delete();
          else if (cloudDir != null) {
            for (ListBlobItem item :
                container.listBlobs(((AzureFileName) getName()).getPathAfterContainer(), true)) {
              String path = item.getUri().getPath();
              if (item instanceof CloudBlob && path.startsWith(getName().getPath())) {
                ((CloudBlob) item).delete();
              }
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
    if (cloudBlob != null) {
      // Get the new blob reference
      CloudBlobContainer newContainer =
          service.getContainerReference(((AzureFileName) newfile.getName()).getContainer());
      CloudBlob newBlob =
          newContainer.getBlobReferenceFromServer(
              ((AzureFileName) newfile.getName()).getPathAfterContainer().substring(1));

      // Start the copy operation
      newBlob.startCopy(cloudBlob.getUri());
      // Delete the original blob
      doDelete();
    } else {
      throw new FileSystemException("Renaming of directories not supported on this file.");
    }
  }

  @Override
  protected void doCreateFolder() throws StorageException, URISyntaxException, IOException {
    if (container == null) {
      throw new UnsupportedOperationException();
    } else if (containerPath.equals("")) {
      container.create();
      type = FileType.FOLDER;
      children = new ArrayList<>();
    } else {
      /*
       * Azure doesn't actually have folders, so we create a temporary
       * 'file' in the 'folder'
       */
      CloudBlockBlob blob =
          container.getBlockBlobReference(containerPath.substring(1) + "/" + markerFileName);
      byte[] buf =
          ("This is a temporary blob created by a Commons VFS application to simulate a folder. It "
                  + "may be safely deleted, but this will hide the folder in the application if it is empty.")
              .getBytes(StandardCharsets.UTF_8);
      blob.uploadFromByteArray(buf, 0, buf.length);
      type = FileType.FOLDER;
      children = new ArrayList<>();
    }
  }

  @Override
  protected long doGetLastModifiedTime() throws Exception {
    return lastModified;
  }

  @Override
  protected OutputStream doGetOutputStream(boolean bAppend) throws Exception {
    if (container != null && !containerPath.equals("")) {
      if (bAppend) throw new UnsupportedOperationException();
      final CloudBlockBlob cbb = container.getBlockBlobReference(removeLeadingSlash(containerPath));
      type = FileType.FILE;
      blobOutputStream =
          container.getBlockBlobReference(removeLeadingSlash(containerPath)).openOutputStream();
      return new BlockBlobOutputStream(cbb, blobOutputStream);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    if (container != null && !containerPath.equals("") && type == FileType.FILE) {
      return new BlobInputStream(cloudBlob.openInputStream(), size);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected FileType doGetType() {
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
