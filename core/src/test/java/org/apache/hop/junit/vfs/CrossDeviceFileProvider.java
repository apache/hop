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
package org.apache.hop.junit.vfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.ram.RamFileObject;
import org.apache.commons.vfs2.provider.ram.RamFileProvider;
import org.apache.commons.vfs2.provider.ram.RamFileSystem;

/**
 * An in-memory file system on which a rename never succeeds, which is what the operating system
 * does with a rename across a mount point. Everything else - reading, writing, listing, deleting -
 * behaves as usual, so the only thing which can go wrong is the move itself.
 *
 * <p>Two folders on this file system stand in for two local folders on two different mounts: one
 * VFS file system, so {@link FileObject#moveTo(FileObject)} picks a rename, and a rename which
 * fails. Register it on the file system manager under a scheme of your own and resolve through it:
 *
 * <pre>
 *   HopVfs.getFileSystemManager().addProvider("xdev", new CrossDeviceFileProvider());
 *   HopVfs.getFileObject("xdev:///work/sales.csv");
 * </pre>
 *
 * Registering is for the one and only file system manager, so call {@code HopVfs.reset()} when the
 * test is done. See <a href="https://github.com/apache/hop/issues/5936">issue #5936</a>.
 */
public class CrossDeviceFileProvider extends RamFileProvider {

  @Override
  protected FileSystem doCreateFileSystem(FileName rootName, FileSystemOptions fileSystemOptions) {
    return new CrossDeviceFileSystem(rootName, fileSystemOptions);
  }

  private static class CrossDeviceFileSystem extends RamFileSystem {
    CrossDeviceFileSystem(FileName rootName, FileSystemOptions fileSystemOptions) {
      super(rootName, fileSystemOptions);
    }

    @Override
    protected FileObject createFile(AbstractFileName name) {
      return new CrossDeviceFileObject(name, this);
    }
  }

  private static class CrossDeviceFileObject extends RamFileObject {
    CrossDeviceFileObject(AbstractFileName name, RamFileSystem fileSystem) {
      super(name, fileSystem);
    }

    @Override
    protected void doRename(FileObject newFile) throws FileSystemException {
      // Errno EXDEV, which LocalFile reports the same way: File.renameTo() returns false.
      throw new FileSystemException(
          "vfs.provider.local/rename-file.error", getName(), newFile.getName());
    }
  }
}
