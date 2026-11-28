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
package org.apache.hop.execution.context;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.RequiredArgsConstructor;

/**
 * Optional, process-wide scoped activation of existing logging identities. Listeners own all
 * context; Hop only pairs activation with restoration. No lifecycle or current-context state is
 * maintained here.
 *
 * <p>Each activation uses a registration-order snapshot. Registration changes affect subsequent
 * snapshots, not callbacks or scopes already captured. Callbacks run outside registration locks and
 * may run concurrently or reentrantly. Listener failures, including errors, are isolated both
 * during activation and cleanup.
 */
public final class ExecutionContexts {
  private static final ExecutionScope NOOP = () -> {};
  private static final CopyOnWriteArrayList<Registration> LISTENERS = new CopyOnWriteArrayList<>();

  private ExecutionContexts() {}

  /**
   * Registers a non-null listener. Closing the result on any thread removes only this registration;
   * duplicate registrations are independent. Removal does not wait for in-flight callbacks.
   */
  public static ExecutionScope addListener(final ExecutionContextListener listener) {
    final var registration = new Registration(Objects.requireNonNull(listener, "listener"));
    LISTENERS.add(registration);
    return registration;
  }

  /**
   * Activates the supplied log channel ID on this thread. Null IDs and an empty listener snapshot
   * return a shared no-op. Non-null IDs are passed through without validation or registry lookup.
   *
   * <p>Close the returned scope on this thread, in LIFO order relative to other activations.
   * Cleanup runs once in reverse callback order, including after listener removal. A wrong-thread
   * close of an open restoration scope throws {@link IllegalStateException} without consuming it.
   * LIFO order between activations is the caller's responsibility, not tracked by Hop.
   */
  public static ExecutionScope activate(final String logChannelId) {
    if (logChannelId == null || LISTENERS.isEmpty()) {
      return NOOP;
    }
    final var registrations = LISTENERS.iterator();
    final List<ExecutionScope> scopes = new ArrayList<>();
    while (registrations.hasNext()) {
      try {
        final var scope = registrations.next().listener.activate(logChannelId);
        if (scope != null) {
          scopes.add(scope);
        }
      } catch (Throwable ignored) {
        // Optional instrumentation must not prevent execution or the remaining callbacks.
      }
    }
    return scopes.isEmpty() ? NOOP : new Activation(Thread.currentThread(), scopes);
  }

  @RequiredArgsConstructor
  private static final class Registration implements ExecutionScope {
    private final ExecutionContextListener listener;

    @Override
    public void close() {
      LISTENERS.remove(this);
    }
  }

  @RequiredArgsConstructor
  private static final class Activation implements ExecutionScope {
    private final Thread owner;
    private final List<ExecutionScope> scopes;
    private volatile boolean closed;

    @Override
    public void close() {
      if (closed) {
        return;
      }
      if (Thread.currentThread() != owner) {
        throw new IllegalStateException("Activation scope must close on its opening thread");
      }
      closed = true;
      for (int i = scopes.size() - 1; i >= 0; i--) {
        try {
          scopes.get(i).close();
        } catch (Throwable ignored) {
          // Continue restoring other listeners even when one cleanup fails.
        }
      }
      scopes.clear();
    }
  }
}
