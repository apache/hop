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
package org.apache.hop.vfs.smb;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.util.RandomAccessMode;

public class SmbFileObject extends AbstractFileObject<SmbFileSystem> {
  private SmbShare.SmbEntry entry;

  protected SmbFileObject(AbstractFileName name, SmbFileSystem fileSystem) {
    super(name, fileSystem);
  }

  private SmbFileSystem fs() {
    return (SmbFileSystem) getFileSystem();
  }

  private String sharePath() throws Exception {
    return fs().toSharePath(getName());
  }

  @Override
  protected void doAttach() throws Exception {
    try {
      entry = fs().share().stat(sharePath());
    } catch (Exception e) {
      if (SmbErrors.notFound(e)) {
        entry = null;
      } else {
        throw e;
      }
    }
    injectType(
        entry == null ? FileType.IMAGINARY : entry.directory() ? FileType.FOLDER : FileType.FILE);
  }

  @Override
  protected void doDetach() {
    entry = null;
  }

  @Override
  protected FileType doGetType() {
    if (entry == null) {
      return FileType.IMAGINARY;
    }
    return entry.directory() ? FileType.FOLDER : FileType.FILE;
  }

  @Override
  protected long doGetContentSize() {
    return entry == null ? 0 : entry.size();
  }

  @Override
  protected long doGetLastModifiedTime() {
    return entry == null ? 0 : entry.lastModified();
  }

  @Override
  protected String[] doListChildren() throws Exception {
    List<String> names = fs().share().children(sharePath());
    return names.toArray(new String[0]);
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    return fs().share().openRead(sharePath());
  }

  @Override
  protected OutputStream doGetOutputStream(boolean append) throws Exception {
    return fs().share().openWrite(sharePath(), append);
  }

  @Override
  protected RandomAccessContent doGetRandomAccessContent(RandomAccessMode mode) throws Exception {
    return new SmbRandomAccessContent(
        fs().share().openRandom(sharePath(), mode.requestWrite()), mode);
  }

  @Override
  protected void doCreateFolder() throws Exception {
    fs().share().createFolder(sharePath());
    doDetach();
    doAttach();
  }

  @Override
  protected void doDelete() throws Exception {
    if (getName().getPath().equals("/")) {
      throw new FileSystemException("vfs.provider/delete-root.error", getName());
    }
    fs().share().delete(sharePath());
    doDetach();
  }

  @Override
  protected void doRename(FileObject newFile) throws Exception {
    String destination = fs().toSharePath(newFile.getName());
    fs().share().rename(sharePath(), destination);
    doDetach();
  }
}
