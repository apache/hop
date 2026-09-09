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
package org.apache.hop.vfs.hdfs;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.hdfs.client.HdfsFileStatus;
import org.apache.hop.vfs.hdfs.client.HdfsWebHdfsClient;

public class HdfsFileObject extends AbstractFileObject<HdfsFileSystem> {
  private static final Class<?> PKG = HdfsTransport.class;

  private HdfsFileStatus status;

  protected HdfsFileObject(AbstractFileName name, HdfsFileSystem fileSystem) {
    super(name, fileSystem);
  }

  private HdfsFileSystem fs() {
    return (HdfsFileSystem) getFileSystem();
  }

  private HdfsWebHdfsClient client() {
    return fs().getClient();
  }

  private String hdfsPath() {
    return fs().toHdfsPath(getName());
  }

  @Override
  protected void doAttach() throws Exception {
    try {
      status = client().getFileStatus(hdfsPath());
      injectType(status.isDirectory() ? FileType.FOLDER : FileType.FILE);
    } catch (Exception e) {
      if (isNotFound(e)) {
        status = null;
        injectType(FileType.IMAGINARY);
      } else {
        throw e;
      }
    }
  }

  @Override
  protected void doDetach() {
    status = null;
  }

  private static boolean isNotFound(Throwable error) {
    Throwable current = error;
    while (current != null) {
      String message = current.getMessage();
      if (message != null
          && (message.contains("FileNotFoundException")
              || message.contains("File does not exist")
              || message.contains("HTTP 404"))) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  @Override
  protected FileType doGetType() {
    if (status == null) {
      return FileType.IMAGINARY;
    }
    return status.isDirectory() ? FileType.FOLDER : FileType.FILE;
  }

  @Override
  protected long doGetContentSize() {
    return status == null ? 0 : status.getLength();
  }

  @Override
  protected long doGetLastModifiedTime() {
    return status == null ? 0 : status.getModificationTime();
  }

  @Override
  protected String[] doListChildren() throws Exception {
    List<HdfsFileStatus> children = client().listStatus(hdfsPath());
    List<String> names = new ArrayList<>();
    for (HdfsFileStatus child : children) {
      String suffix = child.getPathSuffix();
      if (suffix != null && !suffix.isEmpty()) {
        names.add(suffix);
      }
    }
    return names.toArray(new String[0]);
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    return client().open(hdfsPath());
  }

  @Override
  public void createFile() throws FileSystemException {
    // HopVfs.getOutputStream() always calls createFile() then getOutputStream(). The default
    // VFS createFile opens and immediately closes an output stream, which would PUT an empty
    // file and then start a second CREATE. Mark the type only; the real bytes go on the stream.
    if (!exists()) {
      injectType(FileType.FILE);
    }
  }

  @Override
  public OutputStream doGetOutputStream(boolean append) throws Exception {
    if (append) {
      throw new FileSystemException(BaseMessages.getString(PKG, "Hdfs.Error.AppendNotSupported"));
    }
    return client().create(hdfsPath(), true);
  }

  @Override
  protected void doDelete() throws Exception {
    client().delete(hdfsPath(), true);
    doDetach();
  }

  @Override
  protected void doCreateFolder() throws Exception {
    client().mkdirs(hdfsPath());
    doDetach();
    doAttach();
  }

  @Override
  protected void doRename(FileObject newFile) throws Exception {
    String dest = fs().toHdfsPath(newFile.getName());
    client().rename(hdfsPath(), dest);
    doDetach();
  }
}
