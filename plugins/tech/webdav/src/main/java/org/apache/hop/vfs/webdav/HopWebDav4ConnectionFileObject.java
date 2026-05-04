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
package org.apache.hop.vfs.webdav;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileObject;

/**
 * WebDAV4 listing returns {@link FileObject}s whose names are plain {@code webdav4} URIs; {@link
 * AbstractFileObject#getChildren()} returns that array as-is. Re-resolve each child through the
 * file system so names become {@link HopWebDavConnectionFileName} (e.g. {@code nextcloud:///…}).
 */
class HopWebDav4ConnectionFileObject extends Webdav4FileObject {

  HopWebDav4ConnectionFileObject(AbstractFileName name, HopWebDavConnectionFileSystem fs)
      throws FileSystemException {
    super(name, fs);
  }

  @Override
  protected FileObject[] doListChildrenResolved() throws Exception {
    FileObject[] raw = super.doListChildrenResolved();
    if (raw == null) {
      return null;
    }
    HopWebDavConnectionFileSystem fs = (HopWebDavConnectionFileSystem) getFileSystem();
    FileObject[] out = new FileObject[raw.length];
    for (int i = 0; i < raw.length; i++) {
      AbstractFileName childName = (AbstractFileName) raw[i].getName();
      out[i] = fs.resolveFile(fs.toLogicalConnectionName(childName));
    }
    return out;
  }
}
