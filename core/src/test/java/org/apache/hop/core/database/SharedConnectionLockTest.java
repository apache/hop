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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The wrapped connection of a transactional pipeline: what waits for a savepoint section of another
 * thread, and what passes.
 */
class SharedConnectionLockTest {

  /** Long enough for a call that is not blocked to finish. */
  private static final long NOT_BLOCKED_MS = 2_000;

  /** How long a blocked call is watched to stay blocked. */
  private static final long STAYS_BLOCKED_MS = 300;

  private Connection target;
  private PreparedStatement targetPrepared;
  private Statement targetStatement;
  private CallableStatement targetCallable;
  private SharedConnectionLock lock;
  private Connection connection;
  private ExecutorService other;

  /** Something done with the wrapped connection, from another thread. */
  @FunctionalInterface
  interface JdbcCall {
    void call(Connection connection) throws Exception;
  }

  @BeforeEach
  void setUp() throws Exception {
    target = mock(Connection.class);
    targetPrepared = mock(PreparedStatement.class);
    targetStatement = mock(Statement.class);
    targetCallable = mock(CallableStatement.class);
    when(target.prepareStatement(anyString())).thenReturn(targetPrepared);
    when(target.createStatement()).thenReturn(targetStatement);
    when(target.prepareCall(anyString())).thenReturn(targetCallable);
    when(target.setSavepoint()).thenReturn(mock(Savepoint.class));
    lock = SharedConnectionLock.wrap(target);
    connection = lock.getConnection();
    other = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  void tearDown() {
    other.shutdownNow();
  }

  static Stream<Arguments> callsThatWait() {
    return Stream.of(
        Arguments.of(
            "PreparedStatement.executeUpdate",
            (JdbcCall) c -> c.prepareStatement("x").executeUpdate()),
        Arguments.of(
            "PreparedStatement.execute", (JdbcCall) c -> c.prepareStatement("x").execute()),
        Arguments.of(
            "PreparedStatement.executeQuery",
            (JdbcCall) c -> c.prepareStatement("x").executeQuery()),
        Arguments.of(
            "PreparedStatement.executeBatch",
            (JdbcCall) c -> c.prepareStatement("x").executeBatch()),
        Arguments.of(
            "PreparedStatement.executeLargeUpdate",
            (JdbcCall) c -> c.prepareStatement("x").executeLargeUpdate()),
        Arguments.of("Statement.execute(sql)", (JdbcCall) c -> c.createStatement().execute("x")),
        Arguments.of(
            "Statement.executeUpdate(sql)", (JdbcCall) c -> c.createStatement().executeUpdate("x")),
        Arguments.of(
            "Statement.executeQuery(sql)", (JdbcCall) c -> c.createStatement().executeQuery("x")),
        Arguments.of(
            "Statement.executeLargeBatch", (JdbcCall) c -> c.createStatement().executeLargeBatch()),
        Arguments.of("CallableStatement.execute", (JdbcCall) c -> c.prepareCall("x").execute()),
        Arguments.of("Connection.commit", (JdbcCall) Connection::commit),
        Arguments.of("Connection.rollback", (JdbcCall) Connection::rollback),
        Arguments.of(
            "Connection.rollback(savepoint)", (JdbcCall) c -> c.rollback(mock(Savepoint.class))),
        Arguments.of("Connection.setSavepoint", (JdbcCall) Connection::setSavepoint),
        Arguments.of("Connection.setSavepoint(name)", (JdbcCall) c -> c.setSavepoint("name")),
        Arguments.of(
            "Connection.releaseSavepoint",
            (JdbcCall) c -> c.releaseSavepoint(mock(Savepoint.class))),
        Arguments.of("Connection.setAutoCommit", (JdbcCall) c -> c.setAutoCommit(false)),
        Arguments.of("Connection.close", (JdbcCall) Connection::close));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("callsThatWait")
  void waitsWhileAnotherThreadHoldsTheLock(String name, JdbcCall call) throws Exception {
    lock.lock();
    CompletableFuture<Void> done;
    try {
      done = runOnOtherThread(call);
      assertThrows(
          TimeoutException.class,
          () -> done.get(STAYS_BLOCKED_MS, TimeUnit.MILLISECONDS),
          name + " ran while another thread was in a savepoint section");
    } finally {
      lock.unlock();
    }
    done.get(NOT_BLOCKED_MS, TimeUnit.MILLISECONDS);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("callsThatWait")
  void runsWhenNobodyHoldsTheLock(String name, JdbcCall call) throws Exception {
    runOnOtherThread(call).get(NOT_BLOCKED_MS, TimeUnit.MILLISECONDS);
    assertFalse(lock.isLocked(), name + " left the lock held");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("callsThatWait")
  void theThreadHoldingTheLockPasses(String name, JdbcCall call) throws Exception {
    lock.lock();
    try {
      call.call(connection);
      assertTrue(lock.isHeldByCurrentThread());
    } finally {
      lock.unlock();
    }
    assertFalse(lock.isLocked(), name + " left the lock held");
  }

  /** Binding values and reading results don't talk to the database: they don't wait. */
  @Test
  void bindingDoesNotWait() throws Exception {
    PreparedStatement prepared = connection.prepareStatement("x");
    lock.lock();
    try {
      runOnOtherThread(
              c -> {
                prepared.setString(1, "a");
                prepared.setLong(2, 1L);
                prepared.addBatch();
                prepared.clearBatch();
                prepared.getUpdateCount();
              })
          .get(NOT_BLOCKED_MS, TimeUnit.MILLISECONDS);
    } finally {
      lock.unlock();
    }
    verify(targetPrepared).setString(1, "a");
    verify(targetPrepared).addBatch();
  }

  @Test
  void callsReachTheRealConnection() throws Exception {
    PreparedStatement prepared = connection.prepareStatement("insert");
    when(targetPrepared.executeUpdate()).thenReturn(7);
    assertEquals(7, prepared.executeUpdate());
    connection.commit();
    verify(target).prepareStatement("insert");
    verify(targetPrepared).executeUpdate();
    verify(target).commit();
  }

  @Test
  void statementsAreWrappedByTheirKind() throws Exception {
    assertInstanceOf(PreparedStatement.class, connection.prepareStatement("x"));
    assertInstanceOf(CallableStatement.class, connection.prepareCall("x"));
    Statement plain = connection.createStatement();
    assertFalse(plain instanceof PreparedStatement);
    assertNotEquals(targetPrepared, connection.prepareStatement("x"));
  }

  @Test
  void aStatementReturnsTheWrappedConnection() throws Exception {
    assertSame(connection, connection.prepareStatement("x").getConnection());
    assertSame(connection, connection.createStatement().getConnection());
    assertSame(connection, connection.prepareCall("x").getConnection());
  }

  /** PGBulkLoader unwraps the connection to reach the driver's COPY API. */
  @Test
  void unwrapReachesTheDriver() throws Exception {
    VendorConnection vendor = mock(VendorConnection.class);
    when(target.unwrap(VendorConnection.class)).thenReturn(vendor);
    when(target.isWrapperFor(VendorConnection.class)).thenReturn(true);

    assertSame(vendor, connection.unwrap(VendorConnection.class));
    assertTrue(connection.isWrapperFor(VendorConnection.class));
    assertSame(connection, connection.unwrap(Connection.class));
    assertTrue(connection.isWrapperFor(Connection.class));
    assertSame(target, lock.getTarget());
  }

  @Test
  void aFailureComesOutAsTheDriverThrewIt() throws Exception {
    SQLException failure = new SQLException("duplicate key", "23505");
    when(targetPrepared.executeUpdate()).thenThrow(failure);
    PreparedStatement prepared = connection.prepareStatement("x");
    SQLException thrown = assertThrows(SQLException.class, prepared::executeUpdate);
    assertSame(failure, thrown);
    assertFalse(lock.isLocked(), "a failed statement left the lock held");
  }

  @Test
  void aFailedCommitReleasesTheLock() throws Exception {
    SQLException failure = new SQLException("serialization failure", "40001");
    doThrow(failure).when(target).commit();
    assertSame(failure, assertThrows(SQLException.class, connection::commit));
    assertFalse(lock.isLocked());
  }

  @Test
  void equalityIsIdentity() throws Exception {
    assertEquals(connection, connection);
    assertEquals(System.identityHashCode(connection), connection.hashCode());
    PreparedStatement prepared = connection.prepareStatement("x");
    assertEquals(prepared, prepared);
    assertNotEquals(prepared, connection.prepareStatement("x"));
    assertTrue(connection.toString().contains("SharedConnectionLock"));
  }

  private CompletableFuture<Void> runOnOtherThread(JdbcCall call) {
    return CompletableFuture.runAsync(
        () -> {
          try {
            call.call(connection);
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        },
        other);
  }

  /** Stands in for a driver's own connection type, such as PGConnection. */
  interface VendorConnection extends Connection {}
}
