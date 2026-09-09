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

package org.apache.hop.core.vfs;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.File;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.scope.IHopScope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Issue #8295. A namespace is closed when the last user lets go of it - switching project in Hop
 * GUI does exactly that. Work already running on another thread keeps the closed namespace, because
 * the binding is inherited when the thread is created and is never refreshed.
 *
 * <p>A closed {@code DefaultFileSystemManager} has dropped its providers, the local one included,
 * so every path it is handed afterwards looks like a path with no scheme and nothing to resolve it
 * against. That is where the reported failure comes from:
 *
 * <pre>
 * Could not find file with URI "/home/matt/git/.../run-retail-update.hwf" because it is a
 * relative path, and no base URI was provided.
 * </pre>
 *
 * <p>An absolute local path is not a relative path. Resolving one must not depend on whether some
 * other thread has since let go of the namespace this one inherited.
 */
class HopVfsClosedNamespaceTest {

  @AfterEach
  void tearDown() {
    HopVfsNamespaces.setScope(null);
    HopVfsNamespaces.reset();
    HopVfs.reset();
  }

  @Test
  @DisplayName("An absolute local path still resolves after the inherited namespace was closed")
  void absolutePathResolvesAfterTheNamespaceWasClosed(@TempDir File tempDir) throws Exception {
    File file = new File(tempDir, "run-retail-update.hwf");
    assertNotNull(file);

    HopVfsNamespace namespace = new HopVfsNamespace("project being closed");
    HopVfsNamespaces.bindThread(namespace);

    // What HopGui does on the UI thread when the next project is opened: the previous project's
    // namespace has no users left, so it is closed. The background linter is still running with it.
    namespace.close();

    FileObject resolved =
        assertDoesNotThrow(
            () -> HopVfs.getFileObject(file.getAbsolutePath()),
            "An absolute local path must not be reported as a relative path with no base URI");
    assertNotNull(resolved);
  }

  @Test
  @DisplayName("Resolving with variables also survives a closed namespace")
  void absolutePathResolvesWithVariablesAfterTheNamespaceWasClosed(@TempDir File tempDir)
      throws HopException {
    File file = new File(tempDir, "run-retail-update.hwf");

    HopVfsNamespace namespace = new HopVfsNamespace("project being closed");
    HopVfsNamespaces.bindThread(namespace);
    namespace.close();

    FileObject resolved =
        assertDoesNotThrow(() -> HopVfs.getFileObject(file.getAbsolutePath(), null));
    assertNotNull(resolved);
  }

  /**
   * Hop Web serves several tenants from one JVM, and there the process wide manager holds somebody
   * else's named connections. Borrowing it would resolve one tenant's files through another's, so
   * the namespace has to build its own connections again instead.
   */
  @Test
  @DisplayName("With several tenants the namespace is rebuilt rather than the process one borrowed")
  void aClosedNamespaceIsRebuiltWhenTenantsShareTheJvm(@TempDir File tempDir) throws Exception {
    File file = new File(tempDir, "run-retail-update.hwf");

    HopVfsNamespaces.setScope(IHopScope.process());
    HopVfsNamespace namespace = new HopVfsNamespace("one session of several");
    HopVfsNamespaces.bindThread(namespace);
    DefaultFileSystemManager closed = namespace.getFileSystemManager();
    namespace.close();

    FileObject resolved = assertDoesNotThrow(() -> HopVfs.getFileObject(file.getAbsolutePath()));
    assertNotNull(resolved);
    assertNotSame(
        closed,
        namespace.getFileSystemManager(),
        "The namespace kept its closed file system manager instead of building a new one");
    assertSame(
        namespace.getFileSystemManager(),
        resolved.getFileSystem().getFileSystemManager(),
        "The file was resolved outside this tenant's own namespace");
  }
}
