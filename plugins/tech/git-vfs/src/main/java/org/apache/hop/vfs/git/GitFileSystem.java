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
package org.apache.hop.vfs.git;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.hop.core.logging.LogChannel;

/**
 * The file system of one named git connection: everything below its root is a file in the working
 * copy that {@link GitCheckout} put on disk.
 */
public class GitFileSystem extends AbstractFileSystem {

  private final GitCheckout checkout;

  /**
   * @param rootName the root of the connection, whose scheme is the connection name
   * @param parentLayer the file this one hangs off, or null for a root
   * @param checkout the repository behind the connection
   */
  public GitFileSystem(AbstractFileName rootName, FileObject parentLayer, GitCheckout checkout)
      throws FileSystemException {
    super(rootName, parentLayer, new FileSystemOptions());
    this.checkout = checkout;
  }

  public GitCheckout getCheckout() {
    return checkout;
  }

  @Override
  protected void addCapabilities(Collection<Capability> capabilities) {
    capabilities.add(Capability.GET_TYPE);
    capabilities.add(Capability.LIST_CHILDREN);
    capabilities.add(Capability.URI);
    capabilities.add(Capability.GET_LAST_MODIFIED);
    // A working copy is a real folder, so writing, appending, creating, deleting and renaming all
    // work. Whether this particular connection lets you is a question about the connection, and
    // GitFileObject answers that by refusing the write - which keeps "read only" a property of the
    // connection rather than of the class.
    capabilities.add(Capability.WRITE_CONTENT);
    capabilities.add(Capability.APPEND_CONTENT);
    capabilities.add(Capability.CREATE);
    capabilities.add(Capability.DELETE);
    capabilities.add(Capability.RENAME);
  }

  @Override
  protected FileObject createFile(AbstractFileName name) throws Exception {
    return new GitFileObject(name, this);
  }

  /**
   * The file in the working copy which a name of this file system points at.
   *
   * @param name a name of this file system
   * @return the file on disk, never null
   */
  Path resolveToFile(GitFileName name) throws FileSystemException {
    Path base;
    try {
      base = checkout.getBasePath();
    } catch (GitCheckoutException e) {
      // See init(): a FileSystemException reads its first argument as a bundle key, so the
      // one-argument form, which takes the message of the cause, is the one which says anything.
      throw new FileSystemException(e);
    }
    // The base path is a real folder and the name is a path relative to it, so resolving and
    // normalizing is all it takes - as long as nothing walked back out of the repository.
    Path resolved = base.resolve(stripLeadingSlash(name.getPath())).normalize();
    if (!resolved.startsWith(base)) {
      throw new FileSystemException("vfs.provider/invalid-escape-sequence.error", name.getPath());
    }
    // Git checks a symlink out as a symlink, and the JDK follows it. A repository file could
    // otherwise read any file on this machine. The served root is the boundary.
    refuseSymlinkEscape(base, resolved);
    return resolved;
  }

  private void refuseSymlinkEscape(Path base, Path resolved) throws FileSystemException {
    Path existing = resolved;
    try {
      while (existing != null && !Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
        existing = existing.getParent();
      }
      if (existing == null) {
        return;
      }
      Path realBase = base.toRealPath();
      Path real = existing.toRealPath();
      if (!real.startsWith(realBase)) {
        throw new FileSystemException(
            "vfs.provider/invalid-escape-sequence.error", resolved.toString());
      }
    } catch (FileSystemException e) {
      throw e;
    } catch (NoSuchFileException e) {
      // A broken symlink, or a base path which is not in this revision. The name is still inside
      // the checkout; reading it fails as a missing file.
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
  }

  private String stripLeadingSlash(String path) {
    String result = path.replace('\\', '/');
    while (result.startsWith("/")) {
      result = result.substring(1);
    }
    return result;
  }

  @Override
  public void init() throws FileSystemException {
    super.init();
    // Resolve the checkout, and the base path inside it, as soon as the file system opens rather
    // than on the first read: a connection pointing at a repository which cannot be fetched, or at
    // a
    // base path which is not in it, has to say so at the point the user asked for a file rather
    // than
    // halfway through reading one. Nothing else reads the base path until then, so without this a
    // bad connection resolves cleanly and only fails when the file is opened.
    try {
      checkout.getBasePath();
    } catch (GitCheckoutException e) {
      LogChannel.GENERAL.logDebug("Unable to prepare the git repository: " + e.getMessage());
      // FileSystemException looks its first argument up in the commons-vfs2 bundle. Wrapping the
      // cause this way keeps the checkout's own sentence (it becomes the code, and an unknown code
      // is quoted back) and keeps the original exception as the cause.
      throw new FileSystemException(e);
    }
  }
}
