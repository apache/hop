/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.vfs.dropbox;

import org.apache.commons.vfs2.FileNotFoundException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import com.dropbox.core.DbxException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.FolderMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;
import com.dropbox.core.v2.files.UploadUploader;

/**
 * An dropbox file object.
 */
public class DropboxFileObject extends AbstractFileObject<DropboxFileSystem> {

  private final DbxClientV2 client;
  private long size;
  private long lastModified;

  protected DropboxFileObject(final AbstractFileName name, final DropboxFileSystem fileSystem)
      throws FileSystemException {
    super(name, fileSystem);

    this.client = fileSystem.getClient();
    this.size = 0;
    this.lastModified = 0;
    this.injectType(FileType.IMAGINARY);
  }

  @Override
  protected void doAttach() throws FileSystemException {
    synchronized (getFileSystem()) {
      // If root folder
      if (this.getParent() == null) {
        this.injectType(FileType.FOLDER);
      }
    }
  }

  @Override
  protected void doDetach() throws FileSystemException {
    synchronized (getFileSystem()) {
      this.size = 0;
      this.lastModified = 0;
    }
  }

  @Override
  protected FileType doGetType() throws Exception {
    return this.getName().isFile() ? FileType.FILE : FileType.FOLDER;
  }

  /**
   * Fetches the children of this folder.
   */
  private List<Metadata> doGetChildren() throws IOException {
    final List<Metadata> childrens = new ArrayList<>();
    try {
      String path = this.getName().getPath();

      // Root path should be empty
      if ("/".equals(path))
        path = "";

      ListFolderResult result = client.files().listFolder(path);
      while (true) {
        childrens.addAll(result.getEntries());

        if (!result.getHasMore()) {
          break;
        }
        result = this.client.files().listFolderContinue(result.getCursor());
      }
    } catch (DbxException e) {
      throw new FileSystemException("Error find childrens of folder ", this.getName());
    }

    return childrens;
  }

  @Override
  protected FileObject[] doListChildrenResolved() throws Exception {
    List<Metadata> childrens = doGetChildren();
    FileObject[] result = new FileObject[childrens.size()];
    int i = 0;
    final DropboxFileSystem fileSystem = this.getAbstractFileSystem();
    for (Metadata metadata : childrens) {
      DropboxFileObject file =
          (DropboxFileObject) fileSystem.resolveFile(metadata.getPathDisplay());
      if (file != null) {

        // Sets the metadata for this file object.
        if (metadata instanceof FileMetadata) {
          FileMetadata fileMetadata = (FileMetadata) metadata;
          file.injectType(FileType.FILE);
          file.lastModified = fileMetadata.getClientModified().getTime();
          file.size = fileMetadata.getSize();
        } else if (metadata instanceof FolderMetadata) {
          file.injectType(FileType.FOLDER);
          file.size = 0;
          file.lastModified = 0;
        }
        result[i++] = file;
      }
    }
    return result;
  }

  @Override
  protected String[] doListChildren() throws Exception {
    // Return null, then use resolved list of children
    return null;
  }

  @Override
  protected void doDelete() throws Exception {
    synchronized (getFileSystem()) {
      String path = this.getName().getPath();
      client.files().deleteV2(path);
    }
  }

  @Override
  protected void doRename(FileObject newFile) throws Exception {
    synchronized (getFileSystem()) {
      String fromPath = this.getName().getPath();
      String toPath = newFile.getName().getPath();
      client.files().moveV2(fromPath, toPath);
    }
  }

  @Override
  protected void doCreateFolder() throws Exception {
    synchronized (getFileSystem()) {
      String path = this.getName().getPath();
      client.files().createFolderV2(path);
    }
  }

  @Override
  protected long doGetContentSize() {
    return this.size;
  }

  @Override
  protected long doGetLastModifiedTime() {
    return this.lastModified;
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    String path = this.getName().getPath();
    InputStream is = client.files().download(path).getInputStream();
    if (is == null) {
      throw new FileNotFoundException(getName().toString());
    }
    return is;
  }

  @Override
  protected OutputStream doGetOutputStream(boolean bAppend) throws Exception {
    String path = this.getName().getPath();
    final UploadUploader upload = client.files().upload(path);
    return new FilterOutputStream(upload.getOutputStream()) {
      @Override
      public void close() throws IOException {
        super.close();
        try {
          upload.finish();
        } catch (Exception e) {
          throw new IOException("Failed to upload " + getName(), e);
        }
      }
    };
  }
}
