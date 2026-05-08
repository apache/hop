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
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.DelegateFileObject;

/**
 * Presents {@link HopWebDavFileName} to callers while delegating operations to wire WebDAV4.
 *
 * <p>Stock {@code DelegateFileObject} lists only base names; VFS then calls {@code resolveFile} for
 * each child, repeating wire work already done during the directory listing. Returning resolved
 * children reuses the wire {@link FileObject}s from that listing.
 *
 * <p>Children produced by {@link #doListChildrenResolved()} carry the wire {@link FileType} from
 * the PROPFIND so {@link #getType()} / {@link #exists()} need not hit the wire again (Hop's {@code
 * FileInputList} may call both during the same scan).
 */
final class HopWebDavFileObject extends DelegateFileObject<HopWebDavFileSystem> {

  /** When non-null, DAV listing already resolved type; cleared on {@link #refresh()}. */
  private volatile FileType listingTypeHint;

  HopWebDavFileObject(HopWebDavFileName name, HopWebDavFileSystem fs, FileObject wireDelegate)
      throws FileSystemException {
    super(name, fs, wireDelegate);
  }

  HopWebDavFileObject(
      HopWebDavFileName name,
      HopWebDavFileSystem fs,
      FileObject wireDelegate,
      FileType listingTypeHint)
      throws FileSystemException {
    super(name, fs, wireDelegate);
    this.listingTypeHint = listingTypeHint;
  }

  @Override
  protected FileType doGetType() throws FileSystemException {
    FileType hint = listingTypeHint;
    if (hint != null) {
      return hint;
    }
    return super.doGetType();
  }

  @Override
  public void refresh() throws FileSystemException {
    listingTypeHint = null;
    super.refresh();
  }

  @Override
  protected FileObject[] doListChildrenResolved() throws Exception {
    FileObject wireDir = getDelegateFile();
    if (wireDir == null) {
      return null;
    }
    FileObject[] wireChildren = wireDir.getChildren();
    HopWebDavFileSystem fs = getAbstractFileSystem();
    HopWebDavFileName parentName = (HopWebDavFileName) getName();
    FileObject[] out = new FileObject[wireChildren.length];
    for (int i = 0; i < wireChildren.length; i++) {
      FileObject w = wireChildren[i];
      FileType wt = w.getType();
      HopWebDavFileName childName =
          HopWebDavFileName.buildChild(parentName, w.getName().getBaseName(), wt);
      HopWebDavFileObject child = new HopWebDavFileObject(childName, fs, w, wt);
      fs.rememberListedChild(child);
      out[i] = child;
    }
    return out;
  }
}
