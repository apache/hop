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

package org.apache.hop.core.vfs;

import java.io.File;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.commons.vfs2.provider.local.LocalFileName;
import org.apache.commons.vfs2.provider.local.LocalFileSystem;

/**
 * The local file provider of Hop VFS: Commons VFS's own, except that creating a folder that
 * appeared in the meantime is not an error.
 *
 * <p>Commons VFS checks that a folder is missing and then calls {@code mkdirs()}, which fails when
 * another thread or process created it between the two: several copies of a transform writing files
 * into the same new folder. The folder is there, which is what the caller asked for, so only a
 * folder that is still missing is reported.
 */
public class HopLocalFileProvider extends DefaultLocalFileProvider {

  @Override
  protected FileSystem doCreateFileSystem(FileName name, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    LocalFileName rootName = (LocalFileName) name;
    return new HopLocalFileSystem(rootName, rootName.getRootFile(), fileSystemOptions);
  }

  static class HopLocalFileSystem extends LocalFileSystem {
    private final String rootFile;

    HopLocalFileSystem(FileName rootName, String rootFile, FileSystemOptions options) {
      super(rootName, rootFile, options);
      this.rootFile = rootFile;
    }

    @Override
    protected FileObject createFile(AbstractFileName name) throws FileSystemException {
      return new HopLocalFile(this, rootFile, name);
    }
  }

  static class HopLocalFile extends LocalFile {
    HopLocalFile(LocalFileSystem fileSystem, String rootFile, AbstractFileName name) {
      super(fileSystem, rootFile, name);
    }

    @Override
    protected void doCreateFolder() throws Exception {
      File file = getLocalFile();
      if (!file.mkdirs() && !file.isDirectory()) {
        throw new FileSystemException("vfs.provider.local/create-folder.error", file);
      }
    }
  }
}
