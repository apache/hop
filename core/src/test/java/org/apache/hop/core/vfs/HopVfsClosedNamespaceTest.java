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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;
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

  @Test
  @DisplayName("startsWithScheme also survives a closed namespace")
  void startsWithSchemeSurvivesAClosedNamespace() throws Exception {
    HopVfsNamespace namespace = new HopVfsNamespace("project being closed");
    HopVfsNamespaces.bindThread(namespace);
    namespace.close();

    assertTrue(
        HopVfs.startsWithScheme("file:///tmp/test.hwf"),
        "The scheme must still be recognized using the fallback file system manager");
  }

  /**
   * Every file a thread resolves through a closed namespace takes the fallback and says so. A
   * linter thread that inherited the namespace resolves a great many files, and at debug level the
   * log flood alone made Hop GUI unresponsive. Once the namespace is found closed, the thread lets
   * go of it.
   */
  @Test
  @DisplayName("A thread lets go of a closed namespace once it finds it closed")
  void aClosedNamespaceIsUnboundFromTheThread(@TempDir File tempDir) throws Exception {
    File file = new File(tempDir, "run-retail-update.hwf");

    HopVfsNamespace namespace = new HopVfsNamespace("project being closed");
    HopVfsNamespaces.bindThread(namespace);
    namespace.close();

    assertNotNull(HopVfs.getFileObject(file.getAbsolutePath()));
    assertNull(
        HopVfsNamespaces.getCurrent(),
        "The thread kept the closed namespace and will take the fallback for every file");
  }

  @Test
  @DisplayName("A thread that inherited a closed namespace lets go of its own binding only")
  void anInheritedClosedNamespaceIsUnboundFromThatThreadOnly(@TempDir File tempDir)
      throws Exception {
    File file = new File(tempDir, "run-retail-update.hwf");

    HopVfsNamespace namespace = new HopVfsNamespace("project being closed");
    HopVfsNamespaces.bindThread(namespace);

    AtomicReference<Object> afterResolve = new AtomicReference<>("not run");
    AtomicReference<Throwable> failure = new AtomicReference<>();
    namespace.close();
    Thread linter =
        new Thread(
            () -> {
              try {
                HopVfs.getFileObject(file.getAbsolutePath());
                afterResolve.set(HopVfsNamespaces.getCurrent());
              } catch (Throwable t) {
                failure.set(t);
              }
            });
    linter.start();
    linter.join();

    assertNull(failure.get());
    assertNull(afterResolve.get(), "The background thread kept the closed namespace");
    assertSame(
        namespace,
        HopVfsNamespaces.getCurrent(),
        "Another thread's resolve must not change what this thread has bound");
  }

  @Test
  @DisplayName("With several tenants a closed namespace stays bound, as it is rebuilt")
  void aClosedNamespaceStaysBoundWhenTenantsShareTheJvm(@TempDir File tempDir) throws Exception {
    File file = new File(tempDir, "run-retail-update.hwf");

    HopVfsNamespaces.setScope(IHopScope.process());
    HopVfsNamespace namespace = new HopVfsNamespace("one session of several");
    HopVfsNamespaces.bindThread(namespace);
    namespace.close();

    assertNotNull(HopVfs.getFileObject(file.getAbsolutePath()));
    assertSame(namespace, HopVfsNamespaces.getCurrent());
  }
}
