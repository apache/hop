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

import org.apache.commons.io.output.NullOutputStream;
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
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;
import com.dropbox.core.v2.files.UploadUploader;

/** An dropbox file object. */
public class DropboxFileObject extends AbstractFileObject<DropboxFileSystem> {

  private final DbxClientV2 client;
  private Metadata metadata;

  protected DropboxFileObject(final AbstractFileName name, final DropboxFileSystem fileSystem)
      throws FileSystemException {
    super(name, fileSystem);

    this.client = fileSystem.getClient();
    this.injectType(FileType.IMAGINARY);
  }

  @Override
  protected void doAttach() throws FileSystemException {
    synchronized (getFileSystem()) {
      // If root folder
      if (this.getParent() == null) {
        this.injectType(FileType.FOLDER);
        return;
      }
      if (this.metadata == null) {
        try {
          String path = this.getName().getPath();
          this.metadata = client.files().getMetadata(path);
        } catch (DbxException e) {
          // Ignore
        }
      }
    }
  }

  @Override
  protected void doDetach() throws FileSystemException {
    synchronized (getFileSystem()) {
      this.metadata = null;
    }
  }

  @Override
  protected FileType doGetType() throws Exception {
    synchronized (getFileSystem()) {
      if (metadata == null) {
        return FileType.IMAGINARY;
      }
      return (metadata instanceof FileMetadata) ? FileType.FILE : FileType.FOLDER;
    }
  }

  /** Fetches the children of this folder. */
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
      throw new FileSystemException("vfs.provider/list-children.error", this.getName(), e);
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
        file.metadata = metadata;
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
    String path = this.getName().getPath();
    client.files().deleteV2(path);
  }

  @Override
  protected void doRename(FileObject newFile) throws Exception {
    String fromPath = this.getName().getPath();
    String toPath = newFile.getName().getPath();
    client.files().moveV2(fromPath, toPath);
  }

  @Override
  protected void doCreateFolder() throws Exception {
    String path = this.getName().getPath();
    client.files().createFolderV2(path);
  }

  @Override
  protected long doGetContentSize() {
    if (metadata instanceof FileMetadata) {
      return ((FileMetadata) metadata).getSize();
    }
    return 0;
  }

  @Override
  protected long doGetLastModifiedTime() {
    if (metadata instanceof FileMetadata) {
      return ((FileMetadata) metadata).getClientModified().getTime();
    }
    return 0;
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    String path = this.getName().getPath();
    InputStream is = client.files().download(path).getInputStream();
    if (is == null) {
      throw new FileNotFoundException(getName());
    }
    return is;
  }

  @Override
  protected OutputStream doGetOutputStream(boolean append) throws Exception {

    String path = this.getName().getPath();
    if (append) {
      throw new FileSystemException("vfs.provider/write-append-not-supported.error", path);
    }

    // VFS try to create file first, so we return fake output stream to avoid conflict
    if (this.getType() == FileType.IMAGINARY) {
      return NullOutputStream.NULL_OUTPUT_STREAM;
    }

    // TODO: Uploader is limited to 150MB, use upload session to increase upload to 350GB.
    final UploadUploader uploader = client.files().upload(path);
    return new FilterOutputStream(uploader.getOutputStream()) {
      @Override
      public void close() throws IOException {
        try {
          uploader.finish();
        } catch (Exception e) {
          throw new IOException("Failed to upload " + getName(), e);
        } finally {
          uploader.close();
        }
      }
    };
  }
}
