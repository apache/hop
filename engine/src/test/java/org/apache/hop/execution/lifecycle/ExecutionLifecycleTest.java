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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ExecutionLifecycleTest {
  private final List<Object> subjects = Collections.synchronizedList(new ArrayList<>());
  private final List<ExecutionScope> registrations = new ArrayList<>();

  @AfterEach
  void cleanup() {
    for (Object subject : subjects) {
      ExecutionLifecycle.finish(subject, ExecutionState.CANCELLED, null);
    }
    for (ExecutionScope registration : registrations) {
      registration.close();
    }
    assertNull(ExecutionLifecycle.current());
  }

  @Test
  void disabledOperationsAreNoopsAndDoNotLeaveAssociations() {
    Object child = new Object();
    Object parent = new Object();
    assertNull(ExecutionLifecycle.begin(parent, null, ExecutionKind.WORKFLOW, "parent", 0));
    assertSame(ExecutionLifecycle.activate(child), ExecutionLifecycle.activate(parent));
    ExecutionLifecycle.associate(child, parent);
    assertNull(ExecutionLifecycle.get(parent));
    assertNull(ExecutionLifecycle.current());
    ExecutionLifecycle.finish(parent, ExecutionState.SUCCEEDED, null);
    observe(handle -> {});
    ExecutionHandle root = begin(parent, null);
    try (ExecutionScope ignored = ExecutionLifecycle.activate(parent)) {
      assertSame(root, begin(child, null).getParent());
    }
  }

  @Test
  void identityMetadataAndAttachmentsNeverUseSubjectEquality() {
    observe(handle -> {});
    Object first = new HostileIdentity();
    Object second = new HostileIdentity();
    ExecutionHandle a = begin(first, null);
    ExecutionHandle b = begin(second, null);
    assertNotSame(a, b);
    assertSame(first, a.getSubject());
    assertEquals(ExecutionKind.PIPELINE, a.getKind());
    assertEquals("same-name", a.getName());
    assertEquals(3, a.getCopy());
    assertNotEquals(a.getId(), b.getId());
    assertSame(a, ExecutionLifecycle.get(first));
    assertNull(a.getParent());
    assertNull(b.getParent());
    Object key1 = new HostileIdentity();
    Object key2 = new HostileIdentity();
    a.setAttachment(key1, "one");
    a.setAttachment(key2, "two");
    assertEquals("one", a.getAttachment(key1));
    assertEquals("two", a.getAttachment(key2));
    a.setAttachment(key1, null);
    assertNull(a.getAttachment(key1));
  }

  @Test
  void beginAndFinishAreIdempotentButSubsequentRunsAreDistinct() {
    AtomicInteger starts = new AtomicInteger();
    List<ExecutionState> states = new ArrayList<>();
    List<Throwable> failures = new ArrayList<>();
    List<ExecutionHandle> activeAtFinish = new ArrayList<>();
    Throwable failure = new IllegalStateException("execution failure");
    observe(
        new ExecutionObserver() {
          @Override
          public void started(ExecutionHandle handle) {
            starts.incrementAndGet();
          }

          @Override
          public void finished(ExecutionHandle handle, ExecutionState state, Throwable error) {
            activeAtFinish.add(ExecutionLifecycle.get(handle.getSubject()));
            states.add(state);
            failures.add(error);
          }
        });
    Object subject = new Object();
    ExecutionHandle first = begin(subject, null);
    assertSame(first, begin(subject, new Object()));
    ExecutionLifecycle.finish(subject, ExecutionState.FAILED, failure);
    ExecutionLifecycle.finish(subject, ExecutionState.CANCELLED, null);
    assertNull(ExecutionLifecycle.get(subject));
    ExecutionHandle second = begin(subject, null);
    assertNotEquals(first.getId(), second.getId());
    ExecutionLifecycle.finish(subject, ExecutionState.SUCCEEDED, failure);
    assertEquals(2, starts.get());
    assertEquals(List.of(ExecutionState.FAILED, ExecutionState.SUCCEEDED), states);
    assertEquals(List.of(failure, failure), failures);
    assertEquals(2, activeAtFinish.size());
    activeAtFinish.forEach(handle -> assertNull(handle));
  }

  @Test
  void associationRetainsExactFinishedParentAndContextAcrossAsyncLaunch() throws Exception {
    Object contextKey = new Object();
    observe(handle -> handle.setAttachment(contextKey, handle.getId()));
    Object parent = new Object();
    Object child = new Object();
    ExecutionHandle original = begin(parent, null);
    ExecutionLifecycle.associate(child, parent);
    ExecutionLifecycle.finish(parent, ExecutionState.SUCCEEDED, null);
    ExecutionHandle replacement = begin(parent, null);
    try (var executor = Executors.newSingleThreadExecutor()) {
      ExecutionHandle descendant =
          executor.submit(() -> begin(child, parent)).get(5, TimeUnit.SECONDS);
      assertSame(original, descendant.getParent());
      assertNotSame(replacement, descendant.getParent());
      assertEquals(original.getId(), descendant.getParent().getAttachment(contextKey));
    }
    ExecutionLifecycle.finish(child, ExecutionState.SUCCEEDED, null);
    assertSame(replacement, begin(child, parent).getParent());
  }

  @Test
  void parentPrecedenceIsAssociationThenExplicitThenCurrent() {
    observe(handle -> {});
    Object current = new Object();
    Object explicit = new Object();
    Object associated = new Object();
    ExecutionHandle currentHandle = begin(current, null);
    ExecutionHandle explicitHandle = begin(explicit, null);
    ExecutionHandle associatedHandle = begin(associated, null);
    try (ExecutionScope ignored = ExecutionLifecycle.activate(current)) {
      assertSame(currentHandle, begin(new Object(), null).getParent());
      assertSame(explicitHandle, begin(new Object(), explicit).getParent());
      Object child = new Object();
      ExecutionLifecycle.associate(child, associated);
      assertSame(associatedHandle, begin(child, explicit).getParent());
      Object orphan = new Object();
      ExecutionLifecycle.associate(orphan, new Object());
      assertNull(begin(orphan, explicit).getParent());
    }
  }

  @Test
  void associationsUseIdentityEvenWhenEqualityMethodsThrow() {
    observe(handle -> {});
    Object parent = new HostileIdentity();
    Object child = new HostileIdentity();
    Object unrelated = new HostileIdentity();
    ExecutionHandle root = begin(parent, null);
    ExecutionLifecycle.associate(child, parent);
    assertNull(begin(unrelated, null).getParent());
    ExecutionLifecycle.finish(parent, ExecutionState.SUCCEEDED, null);
    assertSame(root, begin(child, null).getParent());
  }

  @Test
  void attachmentsRemainThreadSafeAndAvailableAfterFinish() throws Exception {
    observe(handle -> {});
    Object subject = new Object();
    ExecutionHandle handle = begin(subject, null);
    ExecutionLifecycle.finish(subject, ExecutionState.SUCCEEDED, null);
    try (var executor = Executors.newFixedThreadPool(4)) {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < 16; i++) {
        Object key = new HostileIdentity();
        futures.add(
            executor.submit(
                () -> {
                  for (int j = 0; j < 100; j++) {
                    handle.setAttachment(key, j);
                    assertEquals(j, handle.getAttachment(key));
                  }
                  handle.setAttachment(key, null);
                  assertNull(handle.getAttachment(key));
                }));
      }
      for (var future : futures) {
        future.get(5, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  void inactiveActivationPreservesCurrentAndDuplicateRegistrationsRemoveIndependently() {
    AtomicInteger starts = new AtomicInteger();
    ExecutionObserver observer = handle -> starts.incrementAndGet();
    ExecutionScope first = observe(observer);
    observe(observer);
    first.close();
    first.close();
    Object subject = new Object();
    ExecutionHandle root = begin(subject, null);
    try (ExecutionScope ignored = ExecutionLifecycle.activate(subject);
        ExecutionScope inactive = ExecutionLifecycle.activate(new Object())) {
      assertSame(root, ExecutionLifecycle.current());
    }
    assertEquals(1, starts.get());
  }

  @Test
  void nestedScopesRestoreFinishedParentsAndCloseObserversInReverseOrder() {
    List<String> events = new ArrayList<>();
    observe(scopedObserver("a", events));
    observe(scopedObserver("b", events));
    Object parent = new Object();
    Object child = new Object();
    ExecutionHandle root = begin(parent, null);
    ExecutionHandle nested = begin(child, parent);
    ExecutionScope outer = ExecutionLifecycle.activate(parent);
    assertSame(root, ExecutionLifecycle.current());
    ExecutionScope inner = ExecutionLifecycle.activate(child);
    assertSame(nested, ExecutionLifecycle.current());
    ExecutionLifecycle.finish(parent, ExecutionState.SUCCEEDED, null);
    inner.close();
    inner.close();
    assertSame(root, ExecutionLifecycle.current());
    outer.close();
    assertNull(ExecutionLifecycle.current());
    assertEquals(List.of("a+", "b+", "a+", "b+", "a!", "b!", "b-", "a-", "b-", "a-"), events);
  }

  @Test
  void concurrentRootsDoNotShareCurrentOrParent() throws Exception {
    observe(handle -> {});
    CyclicBarrier barrier = new CyclicBarrier(2);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var first = executor.submit(() -> concurrentRoot(barrier));
      var second = executor.submit(() -> concurrentRoot(barrier));
      ExecutionHandle a = first.get(10, TimeUnit.SECONDS);
      ExecutionHandle b = second.get(10, TimeUnit.SECONDS);
      assertNull(a.getParent());
      assertNull(b.getParent());
      assertNotEquals(a.getId(), b.getId());
    }
  }

  @Test
  void concurrentBeginAndFinishNotifyOnlyOnce() throws Exception {
    AtomicInteger starts = new AtomicInteger();
    AtomicInteger finishes = new AtomicInteger();
    observe(
        new ExecutionObserver() {
          @Override
          public void started(ExecutionHandle handle) {
            starts.incrementAndGet();
          }

          @Override
          public void finished(ExecutionHandle handle, ExecutionState state, Throwable failure) {
            finishes.incrementAndGet();
          }
        });
    Object subject = new Object();
    try (var executor = Executors.newFixedThreadPool(8)) {
      List<Future<ExecutionHandle>> futures = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        futures.add(executor.submit(() -> begin(subject, null)));
      }
      ExecutionHandle handle = futures.getFirst().get(5, TimeUnit.SECONDS);
      for (var future : futures) {
        assertSame(handle, future.get(5, TimeUnit.SECONDS));
      }
      List<Future<?>> finishesPending = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        finishesPending.add(
            executor.submit(
                () -> ExecutionLifecycle.finish(subject, ExecutionState.SUCCEEDED, null)));
      }
      for (var future : finishesPending) {
        future.get(5, TimeUnit.SECONDS);
      }
    }
    assertEquals(1, starts.get());
    assertEquals(1, finishes.get());
  }

  @Test
  void observerFailuresDoNotPreventOtherCallbacksOrScopeCleanup() {
    List<String> events = new ArrayList<>();
    observe(
        handle -> {
          throw new AssertionError("start");
        });
    observe(scopedObserver("first", events));
    observe(
        new ExecutionObserver() {
          @Override
          public void started(ExecutionHandle handle) {}

          @Override
          public ExecutionScope activate(ExecutionHandle handle) {
            throw new AssertionError("activate");
          }

          @Override
          public void finished(ExecutionHandle handle, ExecutionState state, Throwable failure) {
            throw new AssertionError("finish");
          }
        });
    observe(
        new ExecutionObserver() {
          @Override
          public void started(ExecutionHandle handle) {}

          @Override
          public ExecutionScope activate(ExecutionHandle handle) {
            return () -> {
              events.add("faulty-close");
              throw new AssertionError("close");
            };
          }
        });
    observe(scopedObserver("last", events));
    Object subject = new Object();
    ExecutionHandle handle = begin(subject, null);
    try (ExecutionScope ignored = ExecutionLifecycle.activate(subject)) {
      assertSame(handle, ExecutionLifecycle.current());
    }
    assertNull(ExecutionLifecycle.current());
    ExecutionLifecycle.finish(subject, ExecutionState.FAILED, null);
    assertEquals(
        List.of("first+", "last+", "last-", "faulty-close", "first-", "first!", "last!"), events);
  }

  @Test
  void registrationRemovalIsIdempotentAndPreservesInFlightFinishAndCurrent() {
    AtomicInteger finishes = new AtomicInteger();
    ExecutionScope registration =
        observe(
            new ExecutionObserver() {
              @Override
              public void started(ExecutionHandle handle) {}

              @Override
              public void finished(
                  ExecutionHandle handle, ExecutionState state, Throwable failure) {
                finishes.incrementAndGet();
              }
            });
    Object subject = new Object();
    ExecutionHandle handle = begin(subject, null);
    try (ExecutionScope ignored = ExecutionLifecycle.activate(subject)) {
      registration.close();
      registration.close();
      ExecutionLifecycle.finish(subject, ExecutionState.CANCELLED, null);
      assertSame(handle, ExecutionLifecycle.current());
      assertNull(ExecutionLifecycle.begin(new Object(), null, ExecutionKind.ACTION, "disabled", 0));
    }
    assertEquals(1, finishes.get());
  }

  @Test
  void reentrantFinishIsDeliveredAfterStartReturns() {
    List<String> events = new ArrayList<>();
    observe(
        new ExecutionObserver() {
          @Override
          public void started(ExecutionHandle handle) {
            events.add("start");
            assertSame(handle, ExecutionLifecycle.get(handle.getSubject()));
            ExecutionLifecycle.finish(handle.getSubject(), ExecutionState.CANCELLED, null);
            events.add("started");
          }

          @Override
          public void finished(ExecutionHandle handle, ExecutionState state, Throwable failure) {
            events.add("finish");
          }
        });
    Object subject = new Object();
    begin(subject, null);
    assertEquals(List.of("start", "started", "finish"), events);
    assertNull(ExecutionLifecycle.get(subject));
  }

  @Test
  void abandonedAssociationDoesNotRetainUnstartedChild() throws Exception {
    observe(handle -> {});
    Object parent = new Object();
    begin(parent, null);
    WeakReference<Object> child = abandonedChild(parent);
    for (int i = 0; i < 100 && child.get() != null; i++) {
      System.gc();
      Thread.sleep(10);
    }
    assertNull(child.get(), "pending associations must not strongly retain children");
    // Exercise reference-queue cleanup without exposing registry internals.
    begin(new Object(), null);
  }

  private WeakReference<Object> abandonedChild(Object parent) {
    Object child = new Object();
    ExecutionLifecycle.associate(child, parent);
    return new WeakReference<>(child);
  }

  private ExecutionHandle concurrentRoot(CyclicBarrier barrier) throws Exception {
    Object subject = new Object();
    ExecutionHandle root = begin(subject, null);
    try (ExecutionScope ignored = ExecutionLifecycle.activate(subject)) {
      barrier.await(5, TimeUnit.SECONDS);
      assertSame(root, ExecutionLifecycle.current());
      assertSame(root, begin(new Object(), null).getParent());
    }
    assertNull(ExecutionLifecycle.current());
    return root;
  }

  private ExecutionScope observe(ExecutionObserver observer) {
    ExecutionScope registration = ExecutionLifecycle.addObserver(observer);
    registrations.add(registration);
    return registration;
  }

  private ExecutionHandle begin(Object subject, Object parent) {
    subjects.add(subject);
    return ExecutionLifecycle.begin(subject, parent, ExecutionKind.PIPELINE, "same-name", 3);
  }

  private ExecutionObserver scopedObserver(String name, List<String> events) {
    return new ExecutionObserver() {
      @Override
      public void started(ExecutionHandle handle) {}

      @Override
      public ExecutionScope activate(ExecutionHandle handle) {
        events.add(name + "+");
        return () -> events.add(name + "-");
      }

      @Override
      public void finished(ExecutionHandle handle, ExecutionState state, Throwable failure) {
        events.add(name + "!");
      }
    };
  }

  @Test
  void activationCallbacksDoNotSerializeUnrelatedWorkers() throws Exception {
    final var barrier = new CyclicBarrier(2);
    final var activatedKey = new Object();
    observe(
        new ExecutionObserver() {
          @Override
          public void started(final ExecutionHandle handle) {}

          @Override
          public ExecutionScope activate(final ExecutionHandle handle) {
            try {
              barrier.await(5, TimeUnit.SECONDS);
            } catch (Exception failure) {
              throw new AssertionError(failure);
            }
            handle.setAttachment(activatedKey, Boolean.TRUE);
            return () -> {};
          }
        });
    final var first = new Object();
    final var second = new Object();
    final var a = begin(first, null);
    final var b = begin(second, null);
    try (final var executor = Executors.newFixedThreadPool(2)) {
      final var one =
          executor.submit(
              () -> {
                try (final var scope = ExecutionLifecycle.activate(first)) {
                  assertSame(a, ExecutionLifecycle.current());
                }
              });
      final var two =
          executor.submit(
              () -> {
                try (final var scope = ExecutionLifecycle.activate(second)) {
                  assertSame(b, ExecutionLifecycle.current());
                }
              });
      one.get(10, TimeUnit.SECONDS);
      two.get(10, TimeUnit.SECONDS);
    }
    // Failure isolation must not hide a callback timeout from this test.
    assertEquals(Boolean.TRUE, a.getAttachment(activatedKey));
    assertEquals(Boolean.TRUE, b.getAttachment(activatedKey));
  }

  private static final class HostileIdentity {
    @Override
    public boolean equals(Object other) {
      throw new AssertionError("equals must not be invoked");
    }

    @Override
    public int hashCode() {
      throw new AssertionError("hashCode must not be invoked");
    }
  }
}
