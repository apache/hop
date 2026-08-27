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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * A private VFS world: one {@link DefaultFileSystemManager} carrying the fixed schemes plus the
 * named VFS connections of exactly one metadata provider.
 *
 * <p>Named connections are registered as URI schemes, and the scheme is the connection's name. On a
 * shared JVM - a Hop Server running exports for several people, or Hop Web serving several sessions
 * - one manager therefore means one global namespace: the first connection called {@code mydata}
 * wins and everybody else silently gets it. A namespace per metadata provider keeps the scheme
 * table, the provider instances, the files cache and the replicator apart.
 *
 * <p>Instances are handed out by {@link HopVfsNamespaces}, never constructed directly.
 *
 * @see HopVfsNamespaces
 */
public class HopVfsNamespace implements AutoCloseable {

  private final String description;

  /** Swapped for a fresh one by {@link #rebuild()}; read without locking, so volatile. */
  private volatile DefaultFileSystemManager fileSystemManager;

  /** What the named connections were registered from, so this namespace can rebuild itself. */
  private IVariables variables;

  private IHopMetadataProvider metadataProvider;

  /** How many executions are using this namespace; it closes when the last one lets go. */
  private int useCount;

  HopVfsNamespace(String description) throws HopException {
    this.description = description;
    this.fileSystemManager = HopVfs.createFileSystemManager();
    try {
      this.fileSystemManager.init();
    } catch (Exception e) {
      throw new HopException("Error initializing the file system manager of " + description, e);
    }
  }

  /**
   * Register the named VFS connections of this namespace's metadata. Called once, by {@link
   * HopVfsNamespaces}, after the namespace is published so re-entrant resolution finds it.
   */
  void registerNamedProviders(IVariables variables, IHopMetadataProvider metadataProvider) {
    this.variables = variables;
    this.metadataProvider = metadataProvider;

    Set<String> before = new HashSet<>(Arrays.asList(fileSystemManager.getSchemes()));
    HopVfs.registerNamedProviders(fileSystemManager, variables, metadataProvider);
    List<String> registered =
        Arrays.stream(fileSystemManager.getSchemes())
            .filter(scheme -> !before.contains(scheme))
            .sorted()
            .toList();
    // Worth saying out loud: an empty list here is why a path like 'mydata://...' quietly turns
    // into a relative local file instead of reaching the object store.
    LogChannel.GENERAL.logBasic(
        "VFS namespace of " + description + " : named connections registered " + registered);
  }

  /**
   * Read the named connections again into a fresh file system manager, after one of them changed.
   *
   * <p>A provider holds the settings of its connection as they were when it was registered, and a
   * manager gives no way to replace one, so the whole manager is built again. Every {@link
   * org.apache.commons.vfs2.FileObject} resolved through the old one stops working - which is what
   * makes this worth scoping to the namespace whose connections actually changed, rather than doing
   * it to every namespace in the JVM.
   *
   * <p>The namespace itself survives, so whoever holds it keeps working and the use count stands.
   *
   * @throws HopException if the new manager cannot be created
   */
  void rebuild() throws HopException {
    DefaultFileSystemManager previous = fileSystemManager;
    DefaultFileSystemManager fresh = HopVfs.createFileSystemManager();
    try {
      fresh.init();
    } catch (Exception e) {
      throw new HopException("Error initializing the new file system manager of " + description, e);
    }
    fileSystemManager = fresh;
    if (metadataProvider != null) {
      registerNamedProviders(variables, metadataProvider);
    }
    closeManager(previous);
  }

  public DefaultFileSystemManager getFileSystemManager() {
    return fileSystemManager;
  }

  public String getDescription() {
    return description;
  }

  int retain() {
    return ++useCount;
  }

  int release() {
    return --useCount;
  }

  int getUseCount() {
    return useCount;
  }

  /**
   * Close the manager and everything it holds. Only the owner of the last use closes a namespace:
   * every {@link org.apache.commons.vfs2.FileObject} resolved through it dies with it, and result
   * files carry those objects out of an execution.
   */
  @Override
  public void close() {
    closeManager(fileSystemManager);
  }

  private void closeManager(DefaultFileSystemManager manager) {
    try {
      manager.freeUnusedResources();
      manager.close();
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error closing the file system manager of " + description, e);
    }
  }

  @Override
  public String toString() {
    return "VFS namespace of " + description;
  }
}
