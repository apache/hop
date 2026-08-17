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
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.commons.logging.Log;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.commons.vfs2.provider.ram.RamFileProvider;
import org.apache.hop.core.extension.IPluginMock;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPluginType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

/**
 * {@link HopVfs} keeps one file system manager, knowing both the fixed schemes and the schemes of
 * the named VFS connections in the metadata. The variables of {@code getFileObject(path,variables)}
 * only bootstrap the metadata driven providers; they play no role in resolving a file, so both
 * entry points have to end up on that one manager.
 */
class HopVfsSingleManagerTest {

  /** Stands in for HOP_METADATA_FOLDER: only variables carrying it see the project metadata. */
  private static final String PROJECT_MARKER = "TEST_PROJECT_METADATA";

  private IPlugin registeredPlugin;

  /**
   * Clear static HopVfs state before each method. Other core VFS tests (for example {@link
   * HopVfsTest#testStartsWithScheme()}) can leave {@code bootstrapVariables} set via {@code
   * startsWithScheme(name, variables)} / {@code getFileSystemManager(variables)}. Without this,
   * {@link #noMetadataLookupBeforeVariablesArrive} sees a false positive: named providers are
   * looked up even though this test never bootstrapped a project.
   */
  @BeforeEach
  void setUp() {
    clearHopVfsState();
  }

  @AfterEach
  void tearDown() {
    if (registeredPlugin != null) {
      PluginRegistry.getInstance().removePlugin(VfsPluginType.class, registeredPlugin);
      registeredPlugin = null;
    }
    // Drop the manager built with the test providers, and the variables it was built with, so
    // other tests start clean.
    clearHopVfsState();
  }

  private static void clearHopVfsState() {
    HopVfs.setBootstrapVariables(null);
    HopVfs.reset();
  }

  @Test
  @DisplayName("Both entry points return the very same file system manager")
  void bothEntryPointsShareOneManager() {
    DefaultFileSystemManager plain = HopVfs.getFileSystemManager();
    DefaultFileSystemManager withVariables =
        HopVfs.getFileSystemManager(Variables.getADefaultVariableSpace());

    assertSame(plain, withVariables);
    assertSame(withVariables, HopVfs.getFileSystemManager());
  }

  @Test
  @DisplayName("Without variables nothing goes looking for metadata")
  void noMetadataLookupBeforeVariablesArrive(@TempDir Path tmp) throws Exception {
    // Reading the Hop configuration resolves a file, and building a default variable space reads
    // the Hop configuration. Going looking for VFS connections here loops forever, so resolving a
    // file without variables must not touch the metadata at all.
    boolean[] wentLooking = {false};
    registerTestVfsPlugin("too-early", variables -> wentLooking[0] = true);

    try (FileObject file =
        HopVfs.getFileObject(tmp.resolve("hop-config.json").toUri().toString())) {
      assertNotNull(file);
    }

    assertFalse(wentLooking[0], "The VFS connections were looked up before a project was loaded");
  }

  @Test
  @DisplayName("Files resolved with and without variables come off the same manager")
  void resolvedFilesShareOneManager(@TempDir Path tmp) throws Exception {
    String url = tmp.resolve("single-manager.txt").toUri().toString();

    try (FileObject withoutVariables = HopVfs.getFileObject(url);
        FileObject withVariables =
            HopVfs.getFileObject(url, Variables.getADefaultVariableSpace())) {
      assertSame(
          withoutVariables.getFileSystem().getFileSystemManager(),
          withVariables.getFileSystem().getFileSystemManager());
      assertSame(
          HopVfs.getFileSystemManager(), withoutVariables.getFileSystem().getFileSystemManager());
    }
  }

  @Test
  @DisplayName("Named connection providers land on the manager the no-argument getter hands out")
  void namedProvidersAreVisibleToTheNoArgumentGetter() {
    registerTestVfsPlugin("named-connection-test", variables -> {});

    // Loading a project is what hands over the variables to find the connections with.
    HopVfs.setBootstrapVariables(Variables.getADefaultVariableSpace());

    // There is one manager, so the connections are there for callers passing no variables too.
    assertTrue(
        HopVfs.getFileSystemManager().hasProvider("named-connection-test"),
        "The named VFS connection is missing from the file system manager");
    assertTrue(HopVfs.startsWithScheme("named-connection-test://some/file.txt"));
  }

  @Test
  @DisplayName("Resolving a file while the named providers register does not recurse")
  void reEntrantResolutionDuringRegistration(@TempDir Path tmp) {
    String url = tmp.resolve("metadata.json").toUri().toString();
    FileObject[] resolvedDuringRegistration = new FileObject[1];

    // Reading the VFS connection metadata resolves files through HopVfs, so registering the
    // providers re-enters getFileSystemManager(). That has to hand back the manager under
    // construction instead of building a second one or recursing forever.
    registerTestVfsPlugin(
        "reentrant-test",
        variables -> {
          try {
            resolvedDuringRegistration[0] = HopVfs.getFileObject(url);
          } catch (Exception e) {
            throw new IllegalStateException("Re-entrant file resolution failed", e);
          }
        });

    HopVfs.setBootstrapVariables(Variables.getADefaultVariableSpace());
    DefaultFileSystemManager manager = HopVfs.getFileSystemManager();

    assertNotNull(resolvedDuringRegistration[0], "Nothing was resolved during registration");
    assertSame(manager, resolvedDuringRegistration[0].getFileSystem().getFileSystemManager());
    assertTrue(manager.hasProvider("reentrant-test"));
  }

  @Test
  @DisplayName("Resolving a file before a project is enabled does not lock out the named providers")
  void earlyResolutionDoesNotLockOutNamedProviders(@TempDir Path tmp) throws Exception {
    // A named connection only visible to variables pointing at the metadata of a project, the way
    // HOP_METADATA_FOLDER points the VFS plugins at the connections of the project being opened.
    registerTestVfsPlugin(
        "project-connection",
        variables -> {},
        variables -> "true".equals(variables.getVariable(PROJECT_MARKER)));

    // HopGui resolves files long before any project is enabled: it loads its images that way.
    try (FileObject startup = HopVfs.getFileObject(tmp.resolve("icon.svg").toUri().toString())) {
      assertNotNull(startup);
    }
    assertFalse(
        HopVfs.getFileSystemManager().hasProvider("project-connection"),
        "Nothing pointed at the project metadata yet, so there is nothing to find");

    // Enabling the project hands us variables that do see its metadata.
    IVariables projectVariables = Variables.getADefaultVariableSpace();
    projectVariables.setVariable(PROJECT_MARKER, "true");

    assertTrue(
        HopVfs.getFileSystemManager(projectVariables).hasProvider("project-connection"),
        "The named VFS connection of the project was never registered");
    assertTrue(HopVfs.startsWithScheme("project-connection://container/file.txt"));
  }

  @Test
  @DisplayName("A plugin without fixed schemes leaves no unclosed component behind")
  void schemelessPluginLeavesNothingUnclosed() {
    // A provider registered under no scheme at all is adopted as a component of the manager but
    // never lands in its providers map, so close() can't reach it and complains :
    // "DefaultFilesystemManager.close: not all components are closed".
    registerTestVfsPlugin("closes-clean", variables -> {});
    HopVfs.setBootstrapVariables(Variables.getADefaultVariableSpace());

    DefaultFileSystemManager manager = HopVfs.getFileSystemManager();
    assertTrue(manager.hasProvider("closes-clean"));

    Log log = mock(Log.class);
    manager.setLogger(log);

    HopVfs.reset();

    ArgumentCaptor<Object> warnings = ArgumentCaptor.forClass(Object.class);
    verify(log, atLeast(0)).warn(warnings.capture());
    assertTrue(
        warnings.getAllValues().stream()
            .noneMatch(
                warning -> String.valueOf(warning).contains("not all components are closed")),
        "The file system manager was left with components it can't close : "
            + warnings.getAllValues());
  }

  /** Register a VFS plugin serving one named connection, and reset HopVfs so it's picked up. */
  private void registerTestVfsPlugin(String connectionName, Consumer<IVariables> whileRegistering) {
    registerTestVfsPlugin(connectionName, whileRegistering, variables -> true);
  }

  /**
   * Register a VFS plugin serving one named connection, visible only to variables accepted by
   * {@code visibleTo} the way a connection is only visible to variables pointing at the metadata
   * folder holding it.
   */
  private void registerTestVfsPlugin(
      String connectionName,
      Consumer<IVariables> whileRegistering,
      Predicate<IVariables> visibleTo) {
    IVfs vfs =
        new IVfs() {
          @Override
          public String[] getUrlSchemes() {
            // Like the Minio and Databricks plugins: all schemes come from the metadata.
            return new String[] {};
          }

          @Override
          public FileProvider getProvider() {
            return new RamFileProvider();
          }

          @Override
          public Map<String, FileProvider> getProviders(IVariables variables) {
            whileRegistering.accept(variables);
            Map<String, FileProvider> providers = new HashMap<>();
            if (visibleTo.test(variables)) {
              providers.put(connectionName, new RamFileProvider());
            }
            return providers;
          }
        };

    IPluginMock plugin = mock(IPluginMock.class);
    when(plugin.getIds()).thenReturn(new String[] {"test-vfs-plugin"});
    when(plugin.getName()).thenReturn("Test VFS plugin");
    when(plugin.getMainType()).thenReturn((Class) IVfs.class);
    when(plugin.loadClass(IVfs.class)).thenReturn(vfs);

    try {
      PluginRegistry.getInstance().registerPlugin(VfsPluginType.class, plugin);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to register the test VFS plugin", e);
    }
    registeredPlugin = plugin;

    // Forget any manager built before the test plugin existed.
    HopVfs.reset();
  }
}
