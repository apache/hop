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

import java.util.IdentityHashMap;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.scope.IHopScope;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;

/**
 * Hands out {@link HopVfsNamespace}s, one per metadata provider.
 *
 * <p>What decides the meaning of a named VFS connection is not the thread, the session or the
 * execution: it is the metadata the connection is read from. Runs that share a metadata provider
 * genuinely share a definition and share a namespace; anything running against its own metadata -
 * an export on a Hop Server carries its own, bundled in the ZIP - gets its own.
 *
 * <p>Everything running against the metadata the process was started with keeps using the process
 * wide manager of {@link HopVfs}, so nothing changes for Hop GUI, {@code hop-run}, or a server
 * running a pipeline by filename.
 *
 * <p>See Apache Hop issue #8106.
 */
public class HopVfsNamespaces {

  /**
   * Keyed on provider identity, not equality: two providers holding the same connections are still
   * two namespaces. Deliberate - equality would have to compare whole metadata trees.
   */
  private static final Map<IHopMetadataProvider, HopVfsNamespace> NAMESPACES =
      new IdentityHashMap<>();

  /**
   * Where the current namespace lives, for the call sites that resolve a file without any variables
   * in hand. Per thread unless something replaces it: {@code Pipeline} and {@code Workflow} spawn
   * their threads with a plain {@code new Thread(...)}, so the threads of an execution inherit the
   * namespace it bound. Hop Web replaces this with a per session scope - see {@link
   * IHopVfsNamespaceScope}.
   *
   * <p>This is the fallback. Where an {@link IVariables} is available, resolving through it is
   * exact and does not depend on where the work ended up running.
   */
  private static IHopScope<HopVfsNamespace> scope = IHopScope.inheritedByThreads();

  /**
   * Change how far "current" reaches. Call this once at startup, before anything resolves a file:
   * Hop Web installs a per session scope here - the same one it installs on {@link
   * org.apache.hop.metadata.util.HopMetadataInstance} - and everything else leaves the per thread
   * default in place.
   *
   * @param newScope the scope to hold the current namespace in, null restores the default
   */
  public static synchronized void setScope(IHopScope<HopVfsNamespace> newScope) {
    scope = newScope == null ? IHopScope.inheritedByThreads() : newScope;
    // A runtime only installs a scope of its own when several tenants share the JVM. There, no
    // tenant may fall back to the process wide manager: they would share it, and one of them
    // rebuilding its connections would take the others down with it.
    isolateEverything = newScope != null;
  }

  /** Set when the runtime serves several tenants at once, so nobody shares the process manager. */
  private static boolean isolateEverything;

  private HopVfsNamespaces() {
    // Utility class
  }

  /**
   * The namespace to resolve a file with, or null when the process wide manager of {@link HopVfs}
   * applies.
   *
   * @param variables the variables of the caller, may be null
   * @return the namespace to use, or null for the process wide manager
   */
  public static HopVfsNamespace resolve(IVariables variables) {
    IHopMetadataProvider provider =
        variables == null ? null : variables.findExecutionMetadataProvider();

    // Running against the metadata the process was set up with? Then the process wide manager
    // already holds exactly these connections and there is nothing to isolate.
    if (provider != null && !isProcessMetadata(provider)) {
      HopVfsNamespace namespace = existing(provider);
      if (namespace != null) {
        return namespace;
      }
      // Metadata of its own, but nobody took a namespace for it. Resolution falls back below, so
      // a named connection carried by this metadata is about to be missed. Nothing is wired to
      // acquire() on this path: worth knowing which one it is.
      if (LogChannel.GENERAL.isDebug()) {
        LogChannel.GENERAL.logDebug(
            "Resolving a file against metadata with no VFS namespace of its own: "
                + provider.getDescription()
                + ". Its named VFS connections are not registered.");
      }
    }
    return scope.get();
  }

  /** The namespace bound to this thread, or null. */
  public static HopVfsNamespace getCurrent() {
    return scope.get();
  }

  /**
   * Take the namespace of a context running against its own metadata, creating it if this is the
   * first user, and return it. Every caller that gets a namespace back must {@link #release} it
   * when it is done - an execution when it finishes, a Hop Web session when it is disposed.
   *
   * <p>Returns null - and takes nothing - when the context runs against the metadata of the process
   * itself, which is the ordinary case for Hop GUI, {@code hop-run} and a server running a pipeline
   * by filename. Those keep using the process wide manager of {@link HopVfs}.
   *
   * @param variables the variables the connections resolve their settings with
   * @param metadataProvider the metadata this context runs against
   * @param description what this namespace belongs to, for logging
   * @return the namespace, or null when the process wide manager applies
   */
  public static synchronized HopVfsNamespace acquire(
      IVariables variables, IHopMetadataProvider metadataProvider, String description) {
    if (metadataProvider == null || isProcessMetadata(metadataProvider)) {
      LogChannel.GENERAL.logDebug(
          "No separate VFS namespace for "
              + description
              + ": it runs against the metadata of the process itself");
      return null;
    }

    HopVfsNamespace namespace = NAMESPACES.get(metadataProvider);
    if (namespace == null) {
      try {
        namespace = new HopVfsNamespace(description);
      } catch (HopException e) {
        LogChannel.GENERAL.logError(
            "Unable to create a VFS namespace for "
                + description
                + ", falling back to the process wide file system manager",
            e);
        return null;
      }
      // Publish before registering: registering the named connections reads metadata, which
      // resolves files, which lands right back in resolve().
      NAMESPACES.put(metadataProvider, namespace);
      namespace.registerNamedProviders(variables, metadataProvider);
      LogChannel.GENERAL.logBasic(
          "Created a VFS namespace for "
              + description
              + ", holding the named VFS connections of "
              + metadataProvider.getDescription());
    }
    namespace.retain();
    return namespace;
  }

  /**
   * Let go of a namespace taken with {@link #acquire}. It is closed once nothing is using it any
   * more: every file object resolved through it dies with it, so release only after the results of
   * the execution have been dealt with.
   *
   * @param metadataProvider the metadata the namespace was taken for
   */
  public static synchronized void release(IHopMetadataProvider metadataProvider) {
    if (metadataProvider == null) {
      return;
    }
    HopVfsNamespace namespace = NAMESPACES.get(metadataProvider);
    if (namespace == null) {
      return;
    }
    if (namespace.release() <= 0) {
      NAMESPACES.remove(metadataProvider);
      namespace.close();
    }
  }

  /**
   * Make a namespace the one for this thread and the threads it starts, so the call sites that
   * resolve a file without variables land in it too.
   *
   * <p>Returns whatever was bound before, which the caller must hand back to {@link #restoreThread}
   * in a finally. Executions nest - a pipeline inside a workflow, a resolver pipeline inside a
   * transform - and they run on the same thread, so a nested execution that simply cleared the
   * binding on its way out would leave the rest of its parent resolving files in the wrong
   * namespace.
   *
   * <p>A null namespace leaves the binding untouched: an execution running against the metadata of
   * the process has nothing of its own to bind, and must not drop what its parent bound. Threads
   * already started keep the namespace they inherited either way.
   *
   * @param namespace the namespace to bind, or null to leave the binding as it is
   * @return the namespace that was bound before, to pass to {@link #restoreThread}
   */
  public static HopVfsNamespace bindThread(HopVfsNamespace namespace) {
    HopVfsNamespace previous = scope.get();
    if (namespace != null) {
      scope.set(namespace);
    }
    return previous;
  }

  /**
   * Put back what {@link #bindThread} returned.
   *
   * @param previous the namespace that was bound before, may be null
   */
  public static void restoreThread(HopVfsNamespace previous) {
    if (previous == null) {
      scope.remove();
    } else {
      scope.set(previous);
    }
  }

  /**
   * Is this the metadata the process wide file system manager already holds the connections of?
   *
   * <p>Compared against the provider that manager was built from, not against whatever is current
   * now: {@link HopMetadataInstance} reaches only the session in Hop Web, so the current provider
   * there says nothing about a manager shared by every session. Before anything has registered, the
   * current provider is the best answer available and matches how the manager is about to be built.
   */
  private static boolean isProcessMetadata(IHopMetadataProvider provider) {
    if (isolateEverything) {
      return false;
    }
    IHopMetadataProvider behindTheManager = HopVfs.getDefaultNamespaceProvider();
    if (behindTheManager != null) {
      return provider == behindTheManager;
    }
    return provider == HopMetadataInstance.getMetadataProvider();
  }

  /**
   * Read the named connections again for the namespace these variables resolve files in, after one
   * of them changed.
   *
   * <p>Scoped on purpose: rebuilding invalidates every file object of that namespace, and in a JVM
   * shared by several tenants - Hop Web - the others must not notice. Returns false when the caller
   * has no namespace of its own and the process wide manager is what needs rebuilding instead.
   *
   * @param variables the variables of whoever changed a connection
   * @return true when a namespace was rebuilt
   * @throws HopException if the namespace cannot be rebuilt
   */
  public static synchronized boolean refresh(IVariables variables) throws HopException {
    HopVfsNamespace namespace = resolve(variables);
    if (namespace == null) {
      return false;
    }
    namespace.rebuild();
    return true;
  }

  private static synchronized HopVfsNamespace existing(IHopMetadataProvider provider) {
    return NAMESPACES.get(provider);
  }

  /**
   * Close every namespace and unbind the current one. For tests and for a clean shutdown.
   *
   * <p>Leaves the installed scope in place: which runtime this is does not change because the
   * namespaces were closed. A test that installed one puts it back with {@code setScope(null)}.
   */
  public static synchronized void reset() {
    NAMESPACES.values().forEach(HopVfsNamespace::close);
    NAMESPACES.clear();
    scope.remove();
  }

  /** How many namespaces are open right now. For tests and diagnostics. */
  public static synchronized int size() {
    return NAMESPACES.size();
  }
}
