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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.commons.vfs2.provider.ram.RamFileProvider;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.extension.IPluginMock;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPluginType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Registering the named connections of a namespace runs plugin code that reads metadata, and
 * reading metadata resolves files through {@link HopVfs}. A Hop Server running two exported
 * executions at once deadlocked on exactly that: one thread held the {@link HopVfs} monitor while
 * looking its namespace up, the other held the {@link HopVfsNamespaces} monitor while registering
 * providers and wanted {@link HopVfs}. Neither of the two properties below may be given up.
 */
class HopVfsNamespaceLockingTest {

  private IPlugin registeredPlugin;

  /** What the test VFS plugin does while its providers are being registered. */
  private final AtomicReference<Runnable> whileRegistering = new AtomicReference<>(() -> {});

  /** The scheme the test VFS plugin registers for an export's metadata; "locktest" otherwise. */
  private final Map<IHopMetadataProvider, String> schemeFor = new ConcurrentHashMap<>();

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    HopMetadataInstance.setMetadataProvider(null);
    HopVfs.setBootstrapVariables(null);
    HopVfs.reset();
    registerTestVfsPlugin();
    // The process wide manager belongs to some other metadata; everything below needs a
    // namespace of its own, the way an export on a server does.
    HopMetadataInstance.setMetadataProvider(mock(MultiMetadataProvider.class));
    HopVfs.setBootstrapVariables(Variables.getADefaultVariableSpace());
    HopVfs.getFileSystemManager();
  }

  @AfterEach
  void tearDown() {
    if (registeredPlugin != null) {
      PluginRegistry.getInstance().removePlugin(VfsPluginType.class, registeredPlugin);
      registeredPlugin = null;
    }
    HopMetadataInstance.setMetadataProvider(null);
    HopVfs.setBootstrapVariables(null);
    HopVfs.reset();
  }

  @Test
  @DisplayName("Named connections are registered without holding the namespaces monitor")
  void registrationDoesNotHoldTheNamespacesMonitor() {
    // The Azure plugin, for one, reads its connections with a serializer that validates its
    // folder through HopVfs.getFileObject(): plugin code under a global monitor.
    List<Boolean> heldWhileRegistering = new CopyOnWriteArrayList<>();
    whileRegistering.set(() -> heldWhileRegistering.add(Thread.holdsLock(HopVfsNamespaces.class)));

    IHopMetadataProvider exported = mock(MultiMetadataProvider.class);
    HopVfsNamespace namespace =
        HopVfsNamespaces.acquire(variablesOf(exported), exported, "exported workflow");
    try {
      assertNotNull(namespace);
      assertFalse(
          heldWhileRegistering.isEmpty(), "the test VFS plugin was not asked for its providers");
      assertFalse(
          heldWhileRegistering.contains(Boolean.TRUE),
          "HopVfsNamespaces holds its monitor while running VFS plugin code, which resolves"
              + " files through HopVfs and so waits for whoever holds the HopVfs monitor");
    } finally {
      HopVfsNamespaces.release(exported);
    }
  }

  @Test
  @DisplayName("Resolving through HopVfs never waits for a namespace being registered elsewhere")
  void resolvingDoesNotWaitForAnotherNamespacesRegistration() throws Exception {
    // A workflow already running on the server, with its namespace in place.
    IHopMetadataProvider running = mock(MultiMetadataProvider.class);
    IVariables runningVariables = variablesOf(running);
    HopVfsNamespace runningNamespace =
        HopVfsNamespaces.acquire(runningVariables, running, "running workflow");
    assertNotNull(runningNamespace);

    // A second export arrives: its namespace is being registered, and the plugin is busy reading
    // metadata - long enough for the first workflow to go and load its pipeline meanwhile.
    CountDownLatch registering = new CountDownLatch(1);
    CountDownLatch letRegistrationFinish = new CountDownLatch(1);
    whileRegistering.set(
        () -> {
          registering.countDown();
          await(letRegistrationFinish);
        });
    IHopMetadataProvider arriving = mock(MultiMetadataProvider.class);
    Thread secondExport =
        new Thread(
            () -> HopVfsNamespaces.acquire(variablesOf(arriving), arriving, "arriving workflow"),
            "second export");
    secondExport.setDaemon(true);

    AtomicReference<DefaultFileSystemManager> resolved = new AtomicReference<>();
    Thread loadingItsPipeline =
        new Thread(
            () -> resolved.set(HopVfs.getFileSystemManager(runningVariables)),
            "running workflow loading its pipeline");
    loadingItsPipeline.setDaemon(true);

    try {
      secondExport.start();
      assertTrue(registering.await(10, TimeUnit.SECONDS), "registration never started");

      loadingItsPipeline.start();
      loadingItsPipeline.join(TimeUnit.SECONDS.toMillis(5));

      assertFalse(
          loadingItsPipeline.isAlive(),
          "HopVfs.getFileSystemManager(variables) is stuck behind the registration of an"
              + " unrelated namespace: it takes the HopVfs monitor and then waits for the"
              + " HopVfsNamespaces monitor. Had that registration needed HopVfs, as reading"
              + " metadata does, both would wait forever.");
      assertSame(runningNamespace.getFileSystemManager(), resolved.get());
    } finally {
      letRegistrationFinish.countDown();
      secondExport.join(TimeUnit.SECONDS.toMillis(10));
      loadingItsPipeline.join(TimeUnit.SECONDS.toMillis(10));
      HopVfsNamespaces.release(arriving);
      HopVfsNamespaces.release(running);
    }
  }

  @Test
  @DisplayName(
      "A second user of the same metadata waits for the connections, the registering"
          + " thread itself does not")
  void secondAcquirerWaitsForRegistration() throws Exception {
    IHopMetadataProvider shared = mock(MultiMetadataProvider.class);
    IVariables variables = variablesOf(shared);
    CountDownLatch registering = new CountDownLatch(1);
    CountDownLatch letRegistrationFinish = new CountDownLatch(1);
    AtomicReference<HopVfsNamespace> seenWhileRegistering = new AtomicReference<>();
    whileRegistering.set(
        () -> {
          // Reading metadata resolves files: on this thread that must find the namespace at
          // once, connections or not, or registration would wait for itself.
          seenWhileRegistering.set(HopVfsNamespaces.resolve(variables));
          registering.countDown();
          await(letRegistrationFinish);
        });

    AtomicReference<HopVfsNamespace> first = new AtomicReference<>();
    Thread workflow =
        new Thread(
            () -> first.set(HopVfsNamespaces.acquire(variables, shared, "workflow")), "workflow");
    workflow.setDaemon(true);
    AtomicReference<HopVfsNamespace> second = new AtomicReference<>();
    Thread itsPipeline =
        new Thread(
            () -> second.set(HopVfsNamespaces.acquire(variables, shared, "its pipeline")),
            "its pipeline");
    itsPipeline.setDaemon(true);

    try {
      workflow.start();
      assertTrue(registering.await(10, TimeUnit.SECONDS), "registration never started");
      itsPipeline.start();
      itsPipeline.join(500);
      assertTrue(
          itsPipeline.isAlive(),
          "The pipeline got the namespace before its named connections were registered");
      assertNotNull(seenWhileRegistering.get(), "re-entrant resolution did not find the namespace");
    } finally {
      letRegistrationFinish.countDown();
      workflow.join(TimeUnit.SECONDS.toMillis(10));
      itsPipeline.join(TimeUnit.SECONDS.toMillis(10));
    }
    try {
      assertFalse(workflow.isAlive() || itsPipeline.isAlive());
      assertNotNull(first.get());
      assertSame(first.get(), second.get());
      assertSame(first.get(), seenWhileRegistering.get());
      assertTrue(
          first.get().getFileSystemManager().hasProvider("locktest"),
          "the named connection is not registered");
    } finally {
      HopVfsNamespaces.release(shared);
      HopVfsNamespaces.release(shared);
    }
  }

  /**
   * The whole point of a namespace per export (#8106): its named connections reach nobody else, and
   * they go away with it. That has to survive the registration now running outside the lock.
   */
  @Test
  @DisplayName("Concurrent exports keep their connections apart and close with their last user")
  void concurrentExportsKeepTheirProvidersApartAndCloseWithTheLastUser() throws Exception {
    int exports = 8;
    int rounds = 25;
    List<IHopMetadataProvider> providers = new ArrayList<>();
    for (int i = 0; i < exports; i++) {
      IHopMetadataProvider export = mock(MultiMetadataProvider.class);
      schemeFor.put(export, "export" + i);
      providers.add(export);
    }
    DefaultFileSystemManager processWide = HopVfs.getFileSystemManager();
    Set<DefaultFileSystemManager> managers = ConcurrentHashMap.newKeySet();
    List<Throwable> failures = new CopyOnWriteArrayList<>();
    CountDownLatch go = new CountDownLatch(1);
    List<Thread> threads = new ArrayList<>();

    for (int i = 0; i < exports; i++) {
      IHopMetadataProvider export = providers.get(i);
      String mine = schemeFor.get(export);
      IVariables variables = variablesOf(export);
      Runnable oneExecution =
          () -> {
            // A workflow and, inside it, a pipeline: two users of the same namespace.
            HopVfsNamespace workflow = HopVfsNamespaces.acquire(variables, export, mine);
            try {
              HopVfsNamespace pipeline = HopVfsNamespaces.acquire(variables, export, mine);
              try {
                assertSame(workflow, pipeline);
                DefaultFileSystemManager manager = HopVfs.getFileSystemManager(variables);
                managers.add(manager);
                assertSame(workflow.getFileSystemManager(), manager);
                assertTrue(manager.hasProvider(mine), mine + " is missing from its own namespace");
                for (String other : schemeFor.values()) {
                  if (!other.equals(mine)) {
                    assertFalse(manager.hasProvider(other), other + " leaked into " + mine);
                  }
                }
                assertFalse(
                    processWide.hasProvider(mine), mine + " leaked into the process manager");
              } finally {
                HopVfsNamespaces.release(export);
              }
            } finally {
              HopVfsNamespaces.release(export);
            }
          };
      threads.add(
          new Thread(
              () -> {
                await(go);
                for (int round = 0; round < rounds; round++) {
                  try {
                    oneExecution.run();
                  } catch (Throwable t) {
                    failures.add(t);
                    return;
                  }
                }
              },
              "export " + i));
    }
    // And bystanders that only resolve, the way a running execution loads its files.
    for (int i = 0; i < exports; i++) {
      IVariables variables = variablesOf(providers.get(i));
      threads.add(
          new Thread(
              () -> {
                await(go);
                for (int round = 0; round < rounds * 4; round++) {
                  HopVfs.getFileSystemManager(variables);
                }
              },
              "bystander " + i));
    }
    threads.forEach(Thread::start);
    go.countDown();
    for (Thread thread : threads) {
      thread.join(TimeUnit.SECONDS.toMillis(60));
      assertFalse(thread.isAlive(), thread.getName() + " did not finish");
    }

    assertTrue(failures.isEmpty(), () -> "failures: " + failures);
    assertTrue(managers.size() >= exports, "each export must have had a manager of its own");
    assertSame(0, HopVfsNamespaces.size(), "namespaces left open after their last user let go");
    for (DefaultFileSystemManager manager : managers) {
      // A closed manager has dropped every provider, the local one included.
      assertFalse(manager.hasProvider("file"), "a namespace's manager was never closed");
    }
    for (String scheme : schemeFor.values()) {
      assertFalse(processWide.hasProvider(scheme), scheme + " leaked into the process manager");
    }
    assertTrue(processWide.hasProvider("file"), "the process manager itself must stay open");
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Timed out waiting for the test to move on");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  /** The variables of something running against this metadata, the way an execution's are. */
  private static Variables variablesOf(IHopMetadataProvider provider) {
    return new Variables() {
      @Override
      public IHopMetadataProvider getMetadataProvider() {
        return provider;
      }
    };
  }

  /** A VFS plugin that, like the metadata driven ones, does work of its own while registering. */
  private void registerTestVfsPlugin() {
    IVfs vfs =
        new IVfs() {
          @Override
          public String[] getUrlSchemes() {
            return new String[] {};
          }

          @Override
          public FileProvider getProvider() {
            return new RamFileProvider();
          }

          @Override
          public Map<String, FileProvider> getProviders(IVariables variables) {
            whileRegistering.get().run();
            Map<String, FileProvider> providers = new HashMap<>();
            // Each export carries a connection of its own, named after the export.
            IHopMetadataProvider owner =
                variables == null ? null : variables.findExecutionMetadataProvider();
            String scheme = owner == null ? null : schemeFor.get(owner);
            providers.put(scheme == null ? "locktest" : scheme, new RamFileProvider());
            return providers;
          }
        };

    IPluginMock plugin = mock(IPluginMock.class);
    when(plugin.getIds()).thenReturn(new String[] {"test-vfs-locking-plugin"});
    when(plugin.getName()).thenReturn("Test VFS locking plugin");
    when(plugin.getMainType()).thenReturn((Class) IVfs.class);
    when(plugin.loadClass(IVfs.class)).thenReturn(vfs);
    try {
      PluginRegistry.getInstance().registerPlugin(VfsPluginType.class, plugin);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to register the test VFS plugin", e);
    }
    registeredPlugin = plugin;
  }
}
