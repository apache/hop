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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ExecutionContextsTest {
  private final List<ExecutionScope> registrations = new ArrayList<>();

  @AfterEach
  void cleanup() {
    registrations.forEach(ExecutionScope::close);
  }

  @Test
  void noListenersAndNullIdsUseSharedNoop() {
    final var noop = ExecutionContexts.activate("unregistered");
    assertSame(noop, ExecutionContexts.activate("another"));
    final var calls = new AtomicInteger();
    listen(
        id -> {
          calls.incrementAndGet();
          return null;
        });
    assertSame(noop, ExecutionContexts.activate(null));
    noop.close();
    noop.close();
    assertEquals(0, calls.get());
    assertThrows(NullPointerException.class, () -> ExecutionContexts.addListener(null));
  }

  @Test
  void callbacksReceiveExactIdAndCleanupRunsOnceInReverseOrder() {
    final List<String> events = new ArrayList<>();
    listen(
        id -> {
          events.add("first:" + id);
          return () -> events.add("close:first");
        });
    listen(
        id -> {
          events.add("second:" + id);
          return () -> events.add("close:second");
        });
    final var scope = ExecutionContexts.activate("existing-log-id");
    assertEquals(List.of("first:existing-log-id", "second:existing-log-id"), events);
    scope.close();
    scope.close();
    assertEquals(
        List.of("first:existing-log-id", "second:existing-log-id", "close:second", "close:first"),
        events);
  }

  @Test
  void duplicateRegistrationsAreIndependentAndRemovalPreservesOpenScopes() {
    final var calls = new AtomicInteger();
    final var closes = new AtomicInteger();
    final ExecutionContextListener listener =
        id -> {
          calls.incrementAndGet();
          return () -> closes.incrementAndGet();
        };
    final var first = listen(listener);
    final var second = listen(listener);
    final var open = ExecutionContexts.activate("first");
    first.close();
    first.close();
    try (final var ignored = ExecutionContexts.activate("second")) {
      assertEquals(3, calls.get());
    }
    second.close();
    open.close();
    open.close();
    ExecutionContexts.activate("removed").close();
    assertEquals(3, calls.get());
    assertEquals(3, closes.get());
  }

  @Test
  void nullRestorationDoesNotPreventLaterCallbacksOrCleanup() {
    final List<String> events = new ArrayList<>();
    listen(
        id -> {
          events.add("null");
          return null;
        });
    listen(id -> () -> events.add("closed"));
    ExecutionContexts.activate("").close();
    assertEquals(List.of("null", "closed"), events);
  }

  @Test
  void exceptionsAndErrorsAreIsolatedDuringActivationAndReverseCleanup() {
    final List<String> events = new ArrayList<>();
    listen(id -> () -> events.add("last-cleanup"));
    listen(
        id -> {
          events.add("activation-exception");
          throw new IllegalStateException("observer");
        });
    listen(
        id ->
            () -> {
              events.add("cleanup-exception");
              throw new IllegalStateException("cleanup");
            });
    listen(
        id -> {
          events.add("activation-error");
          throw new AssertionError("observer");
        });
    listen(
        id ->
            () -> {
              events.add("cleanup-error");
              throw new AssertionError("cleanup");
            });
    listen(
        id -> {
          events.add("last-activation");
          return null;
        });
    final var scope = assertDoesNotThrow(() -> ExecutionContexts.activate("id"));
    assertDoesNotThrow(scope::close);
    assertDoesNotThrow(scope::close);
    assertEquals(
        List.of(
            "activation-exception",
            "activation-error",
            "last-activation",
            "cleanup-error",
            "cleanup-exception",
            "last-cleanup"),
        events);
  }

  @Test
  void listenerOwnedContextRestoresNestingAfterExecutionFailureAndThreadReuse() throws Exception {
    final var context = new ThreadLocal<String>();
    listen(
        id -> {
          final var previous = context.get();
          context.set(id);
          return () -> {
            if (previous == null) {
              context.remove();
            } else {
              context.set(previous);
            }
          };
        });
    try (final var executor = Executors.newSingleThreadExecutor()) {
      executor
          .submit(
              () -> {
                context.set("external");
                try (final var outer = ExecutionContexts.activate("outer")) {
                  assertEquals("outer", context.get());
                  try (final var noop = ExecutionContexts.activate(null)) {
                    assertEquals("outer", context.get());
                  }
                  assertThrows(
                      AssertionError.class,
                      () -> {
                        try (final var inner = ExecutionContexts.activate("inner")) {
                          assertEquals("inner", context.get());
                          throw new AssertionError("execution");
                        }
                      });
                  assertEquals("outer", context.get());
                }
                assertEquals("external", context.get());
                context.remove();
              })
          .get(5, TimeUnit.SECONDS);
      executor
          .submit(
              () -> {
                assertNull(context.get());
                try (final var ignored = ExecutionContexts.activate("reused")) {
                  assertEquals("reused", context.get());
                }
                assertNull(context.get());
              })
          .get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void wrongThreadCloseIsRejectedWithoutConsumingRestoration() throws Exception {
    final var cleanupThread = new AtomicReference<Thread>();
    final var closes = new AtomicInteger();
    listen(
        id ->
            () -> {
              cleanupThread.set(Thread.currentThread());
              closes.incrementAndGet();
            });
    final var scope = ExecutionContexts.activate("id");
    try (final var executor = Executors.newSingleThreadExecutor()) {
      executor
          .submit(() -> assertThrows(IllegalStateException.class, scope::close))
          .get(5, TimeUnit.SECONDS);
      assertEquals(0, closes.get());
      scope.close();
      executor.submit(scope::close).get(5, TimeUnit.SECONDS);
    } finally {
      scope.close();
    }
    assertSame(Thread.currentThread(), cleanupThread.get());
    assertEquals(1, closes.get());
  }

  @Test
  void concurrentCallbacksOverlapAndRestoreOnTheirOwnThreads() throws Exception {
    final var barrier = new CyclicBarrier(2);
    final var completed = new AtomicInteger();
    final var restored = new AtomicInteger();
    final var failures = new AtomicReference<Throwable>();
    listen(
        id -> {
          final var owner = Thread.currentThread();
          try {
            barrier.await(5, TimeUnit.SECONDS);
            completed.incrementAndGet();
          } catch (Exception failure) {
            failures.set(failure);
          }
          return () -> {
            if (Thread.currentThread() == owner) {
              restored.incrementAndGet();
            }
          };
        });
    try (final var executor = Executors.newFixedThreadPool(2)) {
      final var first = executor.submit(() -> ExecutionContexts.activate("first").close());
      final var second = executor.submit(() -> ExecutionContexts.activate("second").close());
      first.get(5, TimeUnit.SECONDS);
      second.get(5, TimeUnit.SECONDS);
    }
    assertNull(failures.get());
    assertEquals(2, completed.get());
    assertEquals(2, restored.get());
  }

  @Test
  void registrationChangesDoNotBlockOnCallbacksOrChangeCapturedSnapshot() throws Exception {
    final var entered = new CountDownLatch(1);
    final var release = new CountDownLatch(1);
    final var oldCalls = new AtomicInteger();
    final var oldCloses = new AtomicInteger();
    final var newCalls = new AtomicInteger();
    final var failures = new AtomicReference<Throwable>();
    final var blocking =
        listen(
            id -> {
              entered.countDown();
              try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                  failures.set(new AssertionError("Callback was not released"));
                }
              } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                failures.set(failure);
              }
              return () -> oldCloses.incrementAndGet();
            });
    final var old =
        listen(
            id -> {
              oldCalls.incrementAndGet();
              return () -> oldCloses.incrementAndGet();
            });
    try (final var executor = Executors.newFixedThreadPool(2)) {
      final var activation = executor.submit(() -> ExecutionContexts.activate("captured").close());
      try {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        final var change =
            executor.submit(
                () -> {
                  blocking.close();
                  old.close();
                  return ExecutionContexts.addListener(
                      id -> {
                        newCalls.incrementAndGet();
                        return null;
                      });
                });
        registrations.add(change.get(5, TimeUnit.SECONDS));
      } finally {
        release.countDown();
      }
      activation.get(5, TimeUnit.SECONDS);
    }
    assertNull(failures.get());
    assertEquals(1, oldCalls.get());
    assertEquals(2, oldCloses.get());
    assertEquals(0, newCalls.get());
    ExecutionContexts.activate("later").close();
    assertEquals(1, newCalls.get());
    assertEquals(1, oldCalls.get());
  }

  @Test
  void concurrentDuplicateRegistrationsAndRemovalsRemainIndependent() throws Exception {
    final var calls = new AtomicInteger();
    final ExecutionContextListener listener =
        id -> {
          calls.incrementAndGet();
          return null;
        };
    try (final var executor = Executors.newFixedThreadPool(4)) {
      final List<Future<ExecutionScope>> additions = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        additions.add(executor.submit(() -> ExecutionContexts.addListener(listener)));
      }
      for (final var addition : additions) {
        registrations.add(addition.get(5, TimeUnit.SECONDS));
      }
      ExecutionContexts.activate("all").close();
      assertEquals(32, calls.get());
      final List<Future<?>> removals = new ArrayList<>();
      for (final var registration : registrations) {
        removals.add(executor.submit(registration::close));
        removals.add(executor.submit(registration::close));
      }
      for (final var removal : removals) {
        removal.get(5, TimeUnit.SECONDS);
      }
      ExecutionContexts.activate("none").close();
      assertEquals(32, calls.get());
    }
  }

  @Test
  void callbacksCanDeregisterThemselvesAndActivateReentrantly() {
    final List<String> events = new ArrayList<>();
    final var self = new AtomicReference<ExecutionScope>();
    self.set(
        listen(
            id -> {
              events.add("self:" + id);
              self.get().close();
              try (final var ignored = ExecutionContexts.activate("nested")) {
                events.add("body:nested");
              }
              return () -> events.add("close:self");
            }));
    listen(
        id -> {
          events.add("other:" + id);
          return () -> events.add("close:" + id);
        });
    ExecutionContexts.activate("outer").close();
    assertEquals(
        List.of(
            "self:outer",
            "other:nested",
            "body:nested",
            "close:nested",
            "other:outer",
            "close:outer",
            "close:self"),
        events);
  }

  @Test
  void restorationCanReenterItsOwnCloseWithoutRunningTwice() {
    final var scope = new AtomicReference<ExecutionScope>();
    final var closes = new AtomicInteger();
    listen(
        id ->
            () -> {
              closes.incrementAndGet();
              scope.get().close();
            });
    scope.set(ExecutionContexts.activate("id"));
    scope.get().close();
    assertEquals(1, closes.get());
  }

  private ExecutionScope listen(final ExecutionContextListener listener) {
    final var registration = ExecutionContexts.addListener(listener);
    registrations.add(registration);
    return registration;
  }
}
