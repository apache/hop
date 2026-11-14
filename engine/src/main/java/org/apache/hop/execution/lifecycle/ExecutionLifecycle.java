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
package org.apache.hop.execution.lifecycle;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Optional, process-wide execution observation. Subjects are always compared by identity. A begin
 * does not activate: callers explicitly activate on each executing thread and finish exactly once
 * when execution ends. Activation scopes must close on the opening thread in LIFO order, including
 * across nested executions; they restore the previous current handle even if either handle
 * finished.
 *
 * <p>Start/finish callbacks are serialized under a reentrant lock to preserve ordering. Observers
 * must not block waiting for another thread's lifecycle operations. Activation and scope closure
 * run outside that lock on the calling thread. Registration changes affect future executions only:
 * successfully started observers remain paired with their execution until finish. With no
 * registered observers begin, activate and associate are allocation-free no-ops.
 */
public final class ExecutionLifecycle {
  private static final ExecutionScope NOOP = () -> {};
  private static final ReentrantLock LOCK = new ReentrantLock();
  private static final List<Registration> OBSERVERS = new ArrayList<>();
  private static final Map<Object, Active> ACTIVE = new IdentityHashMap<>();
  private static final ReferenceQueue<Object> PENDING_QUEUE = new ReferenceQueue<>();
  private static final Map<WeakIdentityKey, ExecutionHandle> PENDING = new HashMap<>();
  private static final ThreadLocal<ExecutionHandle> CURRENT = new ThreadLocal<>();
  private static volatile boolean enabled;
  private static volatile int activeCount;

  private ExecutionLifecycle() {}

  /** Registers an observer; closing the returned scope removes only this registration. */
  public static ExecutionScope addObserver(ExecutionObserver observer) {
    Objects.requireNonNull(observer, "observer");
    LOCK.lock();
    try {
      Registration registration = new Registration(observer);
      OBSERVERS.add(registration);
      enabled = true;
      return registration;
    } finally {
      LOCK.unlock();
    }
  }

  /**
   * Starts once per active subject identity, or returns null when observation is disabled. Parent
   * precedence is captured association (even a captured null), explicit active parent, then
   * current. Names never participate in parent resolution. A finished subject can begin a new,
   * distinct run.
   */
  public static ExecutionHandle begin(
      Object subject, Object parentSubject, ExecutionKind kind, String name, int copy) {
    if (!enabled) {
      return null;
    }
    LOCK.lock();
    try {
      if (!enabled) {
        return null;
      }
      Objects.requireNonNull(subject, "subject");
      Objects.requireNonNull(kind, "kind");
      purgePending();
      Active existing = ACTIVE.get(subject);
      if (existing != null) {
        return existing.handle;
      }
      WeakIdentityKey key = new WeakIdentityKey(subject, null);
      ExecutionHandle parent;
      if (PENDING.containsKey(key)) {
        parent = PENDING.remove(key);
      } else {
        Active explicit = ACTIVE.get(parentSubject);
        parent = explicit == null ? CURRENT.get() : explicit.handle;
      }
      Active active = new Active(new ExecutionHandle(parent, subject, kind, name, copy));
      ACTIVE.put(subject, active);
      activeCount = ACTIVE.size();
      List<Registration> registrations = new ArrayList<>(OBSERVERS);
      for (Registration registration : registrations) {
        try {
          registration.observer.started(active.handle);
          active.observers.add(registration.observer);
        } catch (Throwable ignored) {
          // Instrumentation must never prevent execution.
        }
        if (active.state != null) {
          break;
        }
      }
      active.starting = false;
      if (active.state != null) {
        notifyFinished(active);
      }
      return active.handle;
    } finally {
      LOCK.unlock();
    }
  }

  /**
   * Returns only an active handle, or null; a detached handle remains valid for retained context.
   */
  public static ExecutionHandle get(Object subject) {
    LOCK.lock();
    try {
      Active active = ACTIVE.get(subject);
      return active == null ? null : active.handle;
    } finally {
      LOCK.unlock();
    }
  }

  /** Returns this thread's activated handle, including a handle finished inside its open scope. */
  public static ExecutionHandle current() {
    ExecutionHandle handle = CURRENT.get();
    if (handle == null) {
      CURRENT.remove();
    }
    return handle;
  }

  /**
   * Activates an active subject and its observers, returning a same-thread, LIFO restoration scope.
   */
  public static ExecutionScope activate(Object subject) {
    if (!enabled && activeCount == 0) {
      return NOOP;
    }
    final ExecutionHandle handle;
    final List<ExecutionObserver> observers;
    LOCK.lock();
    try {
      final var active = ACTIVE.get(subject);
      if (active == null) {
        return NOOP;
      }
      handle = active.handle;
      observers = new ArrayList<>(active.observers);
    } finally {
      LOCK.unlock();
    }
    final var previous = CURRENT.get();
    CURRENT.set(handle);
    final var scopes = new ArrayList<ExecutionScope>(observers.size());
    // Thread-affine activation callbacks must not serialize unrelated worker threads.
    for (final var observer : observers) {
      try {
        final var scope = observer.activate(handle);
        if (scope != null) {
          scopes.add(scope);
        }
      } catch (Throwable ignored) {
        // Successfully opened scopes still belong to the returned restoration scope.
      }
    }
    return new Activation(previous, scopes);
  }

  /** Removes the active run before notifying observers. Duplicate finishes are harmless. */
  public static void finish(Object subject, ExecutionState state, Throwable failure) {
    LOCK.lock();
    try {
      purgePending();
      Active active = ACTIVE.get(subject);
      if (active == null) {
        return;
      }
      Objects.requireNonNull(state, "state");
      ACTIVE.remove(subject);
      activeCount = ACTIVE.size();
      active.state = state;
      active.failure = failure;
      if (!active.starting) {
        notifyFinished(active);
      }
    } finally {
      LOCK.unlock();
    }
  }

  /**
   * Captures the invoking subject's exact active handle before an asynchronous launch. A missing
   * invoking handle captures null, preventing accidental fallback to the worker's current
   * execution. The one-shot association survives parent finish and is consumed by begin. Pending
   * children are weakly identity-keyed; abandoned entries are expunged on subsequent lifecycle
   * mutations. Removing the last observer clears pending associations. Associate before launch, not
   * concurrently with begin.
   */
  public static void associate(Object child, Object invokingSubject) {
    if (!enabled) {
      return;
    }
    LOCK.lock();
    try {
      if (!enabled) {
        return;
      }
      Objects.requireNonNull(child, "child");
      purgePending();
      if (ACTIVE.containsKey(child)) {
        return;
      }
      Active parent = ACTIVE.get(invokingSubject);
      PENDING.put(new WeakIdentityKey(child, PENDING_QUEUE), parent == null ? null : parent.handle);
    } finally {
      LOCK.unlock();
    }
  }

  private static void purgePending() {
    WeakIdentityKey key;
    while ((key = (WeakIdentityKey) PENDING_QUEUE.poll()) != null) {
      PENDING.remove(key);
    }
  }

  private static void notifyFinished(Active active) {
    for (ExecutionObserver observer : active.observers) {
      try {
        observer.finished(active.handle, active.state, active.failure);
      } catch (Throwable ignored) {
        // Finish all observers even if one fails.
      }
    }
  }

  private static final class Active {
    private final ExecutionHandle handle;
    private final List<ExecutionObserver> observers = new ArrayList<>();
    private boolean starting = true;
    private ExecutionState state;
    private Throwable failure;

    private Active(ExecutionHandle handle) {
      this.handle = handle;
    }
  }

  private static final class Registration implements ExecutionScope {
    private final ExecutionObserver observer;

    private Registration(ExecutionObserver observer) {
      this.observer = observer;
    }

    @Override
    public void close() {
      LOCK.lock();
      try {
        OBSERVERS.remove(this);
        enabled = !OBSERVERS.isEmpty();
        if (!enabled) {
          PENDING.clear();
          purgePending();
        }
      } finally {
        LOCK.unlock();
      }
    }
  }

  private static final class Activation implements ExecutionScope {
    private final ExecutionHandle previous;
    private final List<ExecutionScope> scopes;
    private boolean closed;

    private Activation(ExecutionHandle previous, List<ExecutionScope> scopes) {
      this.previous = previous;
      this.scopes = scopes;
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      try {
        for (int i = scopes.size() - 1; i >= 0; i--) {
          try {
            scopes.get(i).close();
          } catch (Throwable ignored) {
            // Always close the remaining scopes and restore the thread context.
          }
        }
      } finally {
        if (previous == null) {
          CURRENT.remove();
        } else {
          CURRENT.set(previous);
        }
        scopes.clear();
      }
    }
  }

  private static final class WeakIdentityKey extends WeakReference<Object> {
    private final int hash;

    private WeakIdentityKey(Object subject, ReferenceQueue<Object> queue) {
      super(subject, queue);
      hash = System.identityHashCode(subject);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      return other instanceof WeakIdentityKey key && get() != null && get() == key.get();
    }
  }
}
