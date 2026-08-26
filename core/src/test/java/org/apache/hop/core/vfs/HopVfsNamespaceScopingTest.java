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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.scope.IHopScope;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Which context gets a VFS namespace of its own, and which is already served by the process wide
 * file system manager. The answer has to hold in a client, on a server running exports for several
 * people, and in Hop Web where one JVM serves many sessions at once.
 */
class HopVfsNamespaceScopingTest {

  @BeforeAll
  static void initLogging() {
    // Creating a namespace logs which connections it registered.
    HopLogStore.init();
  }

  @BeforeEach
  @AfterEach
  void clearState() {
    HopVfsNamespaces.reset();
    HopVfsNamespaces.setScope(null);
    HopMetadataInstance.setScope(null);
    HopMetadataInstance.setMetadataProvider(null);
    HopVfs.setBootstrapVariables(null);
    HopVfs.reset();
  }

  /**
   * What a runtime serving several tenants at once installs at startup: both the metadata and the
   * VFS namespace answer per session rather than per process.
   */
  private IHopScope<MultiMetadataProvider> installSessionScopes() {
    IHopScope<MultiMetadataProvider> perSession = IHopScope.process();
    HopMetadataInstance.setScope(perSession);
    HopVfsNamespaces.setScope(IHopScope.process());
    return perSession;
  }

  /** Build the process wide manager the way loading a project does, from this metadata. */
  private void bootstrapProcessManagerFrom(MultiMetadataProvider provider) {
    HopMetadataInstance.setMetadataProvider(provider);
    HopVfs.setBootstrapVariables(Variables.getADefaultVariableSpace());
    HopVfs.getFileSystemManager();
  }

  @Test
  @DisplayName("The metadata behind the process manager needs no namespace of its own")
  void theProcessMetadataUsesTheProcessManager() {
    MultiMetadataProvider projectMetadata = mock(MultiMetadataProvider.class);
    bootstrapProcessManagerFrom(projectMetadata);

    assertNull(
        HopVfsNamespaces.acquire(new Variables(), projectMetadata, "local run"),
        "A run against the metadata the manager was built from should just use it");
  }

  @Test
  @DisplayName("Metadata of its own - an export on a server - gets a namespace of its own")
  void otherMetadataGetsItsOwnNamespace() {
    MultiMetadataProvider serverMetadata = mock(MultiMetadataProvider.class);
    bootstrapProcessManagerFrom(serverMetadata);

    MultiMetadataProvider exportedMetadata = mock(MultiMetadataProvider.class);
    HopVfsNamespace namespace =
        HopVfsNamespaces.acquire(new Variables(), exportedMetadata, "exported workflow");
    try {
      assertNotNull(namespace, "An export carries its own connections and needs its own namespace");
      assertNotSame(
          HopVfs.getFileSystemManager(),
          namespace.getFileSystemManager(),
          "The export must not be resolving files on the shared manager");
    } finally {
      HopVfsNamespaces.release(exportedMetadata);
    }
  }

  @Test
  @DisplayName("Two Hop Web sessions on their own projects do not share a namespace")
  void sessionsDoNotShareANamespace() {
    // Hop Web gives every session its own metadata, and installs a session scope so that
    // HopMetadataInstance answers per session rather than per process.
    MultiMetadataProvider sessionA = mock(MultiMetadataProvider.class);
    MultiMetadataProvider sessionB = mock(MultiMetadataProvider.class);

    IHopScope<MultiMetadataProvider> perSession = installSessionScopes();

    // Session A opens first, so the process wide manager ends up holding its connections.
    bootstrapProcessManagerFrom(sessionA);

    // Session B opens its own project. Its metadata becomes "current" for that session, which must
    // not make it look like the metadata the shared manager was built from.
    perSession.set(sessionB);

    HopVfsNamespace namespaceB = HopVfsNamespaces.acquire(new Variables(), sessionB, "session B");
    HopVfsNamespace namespaceA = HopVfsNamespaces.acquire(new Variables(), sessionA, "session A");
    try {
      assertNotNull(
          namespaceB, "Session B must not resolve files through the connections of session A");
      assertNotNull(
          namespaceA,
          "With tenants sharing the JVM nobody falls back to the process manager: they would share"
              + " it, and one of them rebuilding its connections would take the other down");
      assertNotSame(namespaceA, namespaceB);
      assertNotSame(HopVfs.getFileSystemManager(), namespaceB.getFileSystemManager());
    } finally {
      HopVfsNamespaces.release(sessionB);
      HopVfsNamespaces.release(sessionA);
    }
  }

  @Test
  @DisplayName("Contexts sharing metadata share one namespace, and it closes with the last of them")
  void sharedMetadataSharesOneNamespace() {
    bootstrapProcessManagerFrom(mock(MultiMetadataProvider.class));
    MultiMetadataProvider shared = mock(MultiMetadataProvider.class);

    HopVfsNamespace first = HopVfsNamespaces.acquire(new Variables(), shared, "workflow");
    HopVfsNamespace second = HopVfsNamespaces.acquire(new Variables(), shared, "its pipeline");

    assertSame(first, second, "A pipeline inside a workflow resolves in the same namespace");
    assertSame(1, HopVfsNamespaces.size());

    HopVfsNamespaces.release(shared);
    assertSame(1, HopVfsNamespaces.size(), "The workflow is still using it");

    HopVfsNamespaces.release(shared);
    assertSame(0, HopVfsNamespaces.size(), "Nothing left using it, so it closes");
  }

  @Test
  @DisplayName("Refreshing one tenant's connections leaves the others alone")
  void refreshOnlyTouchesTheCallersNamespace() throws Exception {
    MultiMetadataProvider sessionA = mock(MultiMetadataProvider.class);
    MultiMetadataProvider sessionB = mock(MultiMetadataProvider.class);

    installSessionScopes();
    bootstrapProcessManagerFrom(sessionA);

    HopVfsNamespace namespaceA = HopVfsNamespaces.acquire(new Variables(), sessionA, "session A");
    HopVfsNamespace namespaceB = HopVfsNamespaces.acquire(new Variables(), sessionB, "session B");
    try {
      DefaultFileSystemManager managerOfA = namespaceA.getFileSystemManager();
      DefaultFileSystemManager managerOfB = namespaceB.getFileSystemManager();

      // Session B saves a VFS connection. Its own manager is rebuilt to pick the change up...
      HopVfsNamespace previous = HopVfsNamespaces.bindThread(namespaceB);
      try {
        assertTrue(HopVfsNamespaces.refresh(new Variables()));
      } finally {
        HopVfsNamespaces.restoreThread(previous);
      }

      assertNotSame(managerOfB, namespaceB.getFileSystemManager(), "B should have read them again");
      assertSame(
          managerOfA,
          namespaceA.getFileSystemManager(),
          "Session A was in the middle of something and must not have lost its file system");
    } finally {
      HopVfsNamespaces.release(sessionA);
      HopVfsNamespaces.release(sessionB);
    }
  }

  @Test
  @DisplayName("A scope that answers per session never falls back to what a pooled thread left")
  void aSessionNeverInheritsAnotherSessionsNamespace() throws Exception {
    // Request threads are pooled: the same thread serves one session and then another. A session
    // with nothing bound has to get nothing, not whatever the previous one left on that thread.
    HopVfsNamespace ofSessionA = new HopVfsNamespace("session A");

    Object[] sessionOnThisThread = {"A"};
    java.util.Map<Object, HopVfsNamespace> bySession = new java.util.HashMap<>();

    HopVfsNamespaces.setScope(
        new IHopScope<HopVfsNamespace>() {
          @Override
          public HopVfsNamespace get() {
            return bySession.get(sessionOnThisThread[0]);
          }

          @Override
          public void set(HopVfsNamespace value) {
            bySession.put(sessionOnThisThread[0], value);
          }

          @Override
          public void remove() {
            bySession.remove(sessionOnThisThread[0]);
          }
        });
    try {
      HopVfsNamespaces.bindThread(ofSessionA);
      assertSame(ofSessionA, HopVfsNamespaces.getCurrent());

      // The same thread now serves a session that has opened nothing yet.
      sessionOnThisThread[0] = "B";
      assertNull(
          HopVfsNamespaces.getCurrent(), "Session B was handed the VFS namespace of session A");
    } finally {
      HopVfsNamespaces.setScope(null);
    }
  }
}
