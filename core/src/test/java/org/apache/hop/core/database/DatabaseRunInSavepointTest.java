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

package org.apache.hop.core.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InOrder;

/** {@link Database#runInSavepoint(ISavepointWork)} and {@link Database#isUsingRowSavepoints}. */
class DatabaseRunInSavepointTest {

  private final ILoggingObject log = mock(ILoggingObject.class);
  private Connection connection;
  private Savepoint savepoint;
  private IDatabase dialect;
  private Database db;
  private ExecutorService other;

  @BeforeEach
  void setUp() throws Exception {
    when(log.getLogLevel()).thenReturn(LogLevel.NOTHING);
    connection = mock(Connection.class);
    savepoint = mock(Savepoint.class);
    when(connection.setSavepoint()).thenReturn(savepoint);
    dialect = mock(IDatabase.class);
    when(dialect.isUseSafePoints()).thenReturn(true);
    when(dialect.isReleaseSavepoint()).thenReturn(true);
    DatabaseMeta meta = mock(DatabaseMeta.class);
    when(meta.getIDatabase()).thenReturn(dialect);
    db = new Database(log, new Variables(), meta);
    db.setConnection(connection);
    db.setCommit(100);
    other = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  void tearDown() {
    other.shutdownNow();
  }

  @Test
  void aRowThatSucceedsReleasesItsSavepoint() throws Exception {
    assertEquals("done", db.runInSavepoint(() -> "done"));

    InOrder order = inOrder(connection);
    order.verify(connection).setSavepoint();
    order.verify(connection).releaseSavepoint(savepoint);
    verify(connection, never()).rollback(any(Savepoint.class));
  }

  @Test
  void aDatabaseThatDoesNotReleaseKeepsTheSavepoint() throws Exception {
    when(dialect.isReleaseSavepoint()).thenReturn(false);
    db.runInSavepoint(() -> null);
    verify(connection, never()).releaseSavepoint(any());
    verify(connection, never()).rollback(any(Savepoint.class));
  }

  @Test
  void aRowThatFailsIsRolledBackToItsSavepointAndThrownAgain() throws Exception {
    HopDatabaseException failure = new HopDatabaseException("duplicate key");
    HopException thrown =
        assertThrows(
            HopException.class,
            () ->
                db.runInSavepoint(
                    () -> {
                      throw failure;
                    }));
    assertSame(failure, thrown);

    InOrder order = inOrder(connection);
    order.verify(connection).setSavepoint();
    order.verify(connection).rollback(savepoint);
    order.verify(connection).releaseSavepoint(savepoint);
    // Only the row is undone, never the whole transaction.
    verify(connection, never()).rollback();
    verify(connection, never()).commit();
  }

  @Test
  void aFailedRowOnADatabaseThatDoesNotReleaseIsOnlyRolledBack() throws Exception {
    when(dialect.isReleaseSavepoint()).thenReturn(false);
    assertThrows(
        HopException.class,
        () ->
            db.runInSavepoint(
                () -> {
                  throw new HopException("bad row");
                }));
    verify(connection).rollback(savepoint);
    verify(connection, never()).releaseSavepoint(any());
  }

  @Test
  void aRuntimeExceptionIsRolledBackToo() throws Exception {
    IllegalStateException failure = new IllegalStateException("bug in a transform");
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () ->
                db.runInSavepoint(
                    () -> {
                      throw failure;
                    })));
    verify(connection).rollback(savepoint);
  }

  @Test
  void aFailedRollbackIsAddedToTheRowFailure() throws Exception {
    SQLException rollbackFailure = new SQLException("connection lost");
    doThrow(rollbackFailure).when(connection).rollback(savepoint);
    HopException failure = new HopException("bad row");

    HopException thrown =
        assertThrows(
            HopException.class,
            () ->
                db.runInSavepoint(
                    () -> {
                      throw failure;
                    }));

    assertSame(failure, thrown);
    assertEquals(1, thrown.getSuppressed().length);
    assertSame(rollbackFailure, thrown.getSuppressed()[0].getCause());
  }

  @Test
  void theWorkDoesNotRunWithoutASavepoint() throws Exception {
    when(connection.setSavepoint()).thenThrow(new SQLException("no savepoints in auto-commit"));
    AtomicBoolean ran = new AtomicBoolean();
    assertThrows(
        HopDatabaseException.class,
        () ->
            db.runInSavepoint(
                () -> {
                  ran.set(true);
                  return null;
                }));
    assertFalse(ran.get());
  }

  @Test
  void aFailedReleaseIsThrown() throws Exception {
    doThrow(new SQLException("savepoint does not exist")).when(connection).releaseSavepoint(any());
    assertThrows(HopDatabaseException.class, () -> db.runInSavepoint(() -> null));
  }

  @Test
  void withoutASharedConnectionNothingIsLocked() throws Exception {
    db.runInSavepoint(
        () -> {
          assertNull(db.getSharedConnectionLock());
          return null;
        });
  }

  @Test
  void theWorkHoldsTheLockOfASharedConnection() throws Exception {
    SharedConnectionLock lock = share();
    db.runInSavepoint(
        () -> {
          assertTrue(lock.isHeldByCurrentThread(), "the work runs outside the lock");
          return null;
        });
    assertFalse(lock.isLocked());
  }

  @Test
  void aFailedRowReleasesTheLockOfASharedConnection() throws Exception {
    SharedConnectionLock lock = share();
    assertThrows(
        HopException.class,
        () ->
            db.runInSavepoint(
                () -> {
                  throw new HopException("bad row");
                }));
    assertFalse(lock.isLocked());
  }

  @Test
  void aFailedSavepointReleasesTheLockOfASharedConnection() throws Exception {
    SharedConnectionLock lock = share();
    when(connection.setSavepoint()).thenThrow(new SQLException("no"));
    assertThrows(HopDatabaseException.class, () -> db.runInSavepoint(() -> null));
    assertFalse(lock.isLocked());
  }

  /**
   * The case that loses rows without the lock: another transform on the shared connection writes a
   * row while this transform's savepoint is open, then this transform rolls back to its savepoint.
   * With the lock the other transform's statement waits until the section is closed.
   */
  @Test
  void anotherTransformsStatementWaitsUntilTheSectionIsClosed() throws Exception {
    PreparedStatement targetStatement = mock(PreparedStatement.class);
    when(connection.prepareStatement(anyString())).thenReturn(targetStatement);
    List<String> events = new CopyOnWriteArrayList<>();
    when(targetStatement.executeUpdate())
        .thenAnswer(
            invocation -> {
              events.add("other transform's insert");
              return 1;
            });
    doAnswer(
            invocation -> {
              events.add("rollback to savepoint");
              return null;
            })
        .when(connection)
        .rollback(savepoint);

    share();
    PreparedStatement shared = db.getConnection().prepareStatement("insert");
    CountDownLatch inSection = new CountDownLatch(1);
    CompletableFuture<Void> otherTransform = new CompletableFuture<>();

    assertThrows(
        HopException.class,
        () ->
            db.runInSavepoint(
                () -> {
                  other.submit(
                      () -> {
                        try {
                          inSection.await();
                          shared.executeUpdate();
                          otherTransform.complete(null);
                        } catch (Exception e) {
                          otherTransform.completeExceptionally(e);
                        }
                      });
                  inSection.countDown();
                  // Give the other transform every chance to get in.
                  LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(300));
                  events.add("this row fails");
                  throw new HopException("bad row");
                }));

    otherTransform.get(2, TimeUnit.SECONDS);
    assertEquals(
        List.of("this row fails", "rollback to savepoint", "other transform's insert"), events);
  }

  /** Two sections on a shared connection never overlap, so no release removes the other's. */
  @Test
  void twoSectionsNeverOverlap() throws Exception {
    SharedConnectionLock lock = share();
    Database second = new Database(log, new Variables(), db.getDatabaseMeta());
    second.setConnection(db.getConnection());
    second.setSharedConnectionLock(lock);
    second.setCommit(100);

    AtomicBoolean inSection = new AtomicBoolean();
    AtomicBoolean overlapped = new AtomicBoolean();
    ISavepointWork<Void> work =
        () -> {
          if (!inSection.compareAndSet(false, true)) {
            overlapped.set(true);
          }
          Thread.onSpinWait();
          inSection.set(false);
          return null;
        };

    CompletableFuture<Void> secondThread =
        CompletableFuture.runAsync(
            () -> {
              for (int i = 0; i < 2_000; i++) {
                try {
                  second.runInSavepoint(work);
                } catch (HopException e) {
                  throw new IllegalStateException(e);
                }
              }
            },
            other);
    for (int i = 0; i < 2_000; i++) {
      db.runInSavepoint(work);
    }
    secondThread.get(10, TimeUnit.SECONDS);

    assertFalse(overlapped.get(), "two savepoint sections ran at the same time");
  }

  @ParameterizedTest(name = "error hop={0}, savepoints={1}, commit size={2} -> {3}")
  @CsvSource({
    "true, true, 100, true",
    "false, true, 100, false",
    "true, false, 100, false",
    "true, true, 0, false",
    "false, false, 0, false"
  })
  void rowSavepointsAreOnlyForAnErrorHopOnADatabaseThatNeedsThem(
      boolean errorHandling, boolean useSafePoints, int commitSize, boolean expected) {
    when(dialect.isUseSafePoints()).thenReturn(useSafePoints);
    db.setCommit(commitSize);
    assertEquals(expected, db.isUsingRowSavepoints(errorHandling));
  }

  private SharedConnectionLock share() {
    SharedConnectionLock lock = SharedConnectionLock.wrap(connection);
    db.setConnection(lock.getConnection());
    db.setSharedConnectionLock(lock);
    return lock;
  }
}
