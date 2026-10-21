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

package org.apache.hop.pipeline.transforms.databasejoin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link DatabaseJoin}
 *
 * @author lance 2026/8/22 7:43
 */
class DatabaseJoinTest {

  DatabaseJoinMeta mockTransformMetaInterface;
  DatabaseJoinData mockTransformDataInterface;
  DatabaseJoin mockDatabaseJoin;

  @BeforeEach
  void setUp() {

    TransformMeta mockTransformMeta = mock(TransformMeta.class);
    PipelineMeta mockPipelineMeta = mock(PipelineMeta.class);
    Pipeline mockPipeline = spy(new LocalPipelineEngine());
    TransformPartitioningMeta mockTransformPartitioningMeta = mock(TransformPartitioningMeta.class);

    when(mockTransformMeta.getName()).thenReturn("MockTransform");
    when(mockPipelineMeta.findTransform(anyString())).thenReturn(mockTransformMeta);
    when(mockTransformMeta.getTargetTransformPartitioningMeta())
        .thenReturn(mockTransformPartitioningMeta);

    mockTransformMetaInterface =
        mock(DatabaseJoinMeta.class, withSettings().extraInterfaces(ITransformMeta.class));
    mockTransformDataInterface =
        mock(DatabaseJoinData.class, withSettings().extraInterfaces(ITransformMeta.class));
    mockTransformDataInterface.db = mock(Database.class);
    mockTransformDataInterface.pstmt = mock(PreparedStatement.class);
    mockDatabaseJoin =
        spy(
            new DatabaseJoin(
                mockTransformMeta,
                mockTransformMetaInterface,
                mockTransformDataInterface,
                1,
                mockPipelineMeta,
                mockPipeline));
  }

  /**
   * Pipeline.stopTransform marks the transform stopped before stopRunning(). That flag must not
   * skip the cancel.
   */
  @Test
  void testStopRunningCancelsWhenTransformIsAlreadyStopped() throws HopException {
    doReturn(true).when(mockDatabaseJoin).isStopped();
    doReturn(false).when(mockTransformDataInterface).isDisposed();
    when(mockTransformDataInterface.db.getConnection()).thenReturn(mock(Connection.class));

    mockDatabaseJoin.stopRunning();

    verify(mockTransformDataInterface.db, times(1)).cancelStatement(any(PreparedStatement.class));
    assertTrue(mockTransformDataInterface.isCanceled);
    verify(mockDatabaseJoin, times(1)).setStopped(true);
  }

  @Test
  void testStopRunningWhenTransformDataInterfaceIsDisposed() throws HopException {
    doReturn(true).when(mockTransformDataInterface).isDisposed();

    mockDatabaseJoin.stopRunning();

    verify(mockTransformDataInterface, times(1)).isDisposed();
    verify(mockTransformDataInterface.db, times(0)).cancelStatement(any(PreparedStatement.class));
    assertFalse(mockTransformDataInterface.isCanceled);
  }

  @Test
  void testStopRunningCancelsWhenDatabaseConnectionIsValid() throws HopException {
    doReturn(false).when(mockTransformDataInterface).isDisposed();
    when(mockTransformDataInterface.db.getConnection()).thenReturn(mock(Connection.class));

    mockDatabaseJoin.stopRunning();

    verify(mockTransformDataInterface, times(1)).isDisposed();
    verify(mockTransformDataInterface.db, times(1)).getConnection();
    verify(mockTransformDataInterface.db, times(1)).cancelStatement(any(PreparedStatement.class));
    assertTrue(mockTransformDataInterface.isCanceled);
  }

  @Test
  void testStopRunningDoesNotCancelTwice() throws HopException {
    doReturn(false).when(mockTransformDataInterface).isDisposed();
    when(mockTransformDataInterface.db.getConnection()).thenReturn(mock(Connection.class));

    mockDatabaseJoin.stopRunning();
    mockDatabaseJoin.stopRunning();

    verify(mockTransformDataInterface.db, times(1)).cancelStatement(any(PreparedStatement.class));
    assertTrue(mockTransformDataInterface.isCanceled);
  }

  @Test
  void testStopRunningWhenDatabaseConnectionIsNotValid() throws HopException {
    doReturn(false).when(mockTransformDataInterface).isDisposed();
    when(mockTransformDataInterface.db.getConnection()).thenReturn(null);

    mockDatabaseJoin.stopRunning();

    verify(mockTransformDataInterface, times(1)).isDisposed();
    verify(mockTransformDataInterface.db, times(1)).getConnection();
    verify(mockTransformDataInterface.db, times(0)).cancelStatement(any(PreparedStatement.class));
    assertFalse(mockTransformDataInterface.isCanceled);
  }

  /**
   * lookupValues holds {@code dbLock} for the whole lookup, including the query and {@code putRow}.
   * stopAll holds the pipeline transform list while it calls stopRunning. Cancel has to run without
   * that lock, or the stop never returns.
   */
  @Test
  void stopDuringRunningQueryDoesNotDeadlockAndEndsWithNoErrors() throws Exception {
    stubRunningLookup();
    doReturn(false).when(mockTransformDataInterface).isDisposed();
    when(mockTransformDataInterface.db.getConnection()).thenReturn(mock(Connection.class));

    CountDownLatch queryStarted = new CountDownLatch(1);
    CountDownLatch cancelCalled = new CountDownLatch(1);
    when(mockTransformDataInterface.db.openQuery(
            any(PreparedStatement.class), any(IRowMeta.class), any(Object[].class)))
        .thenAnswer(
            invocation -> {
              // Still inside lookupValues, so dbLock is held by this thread.
              queryStarted.countDown();
              assertTrue(
                  cancelCalled.await(5, TimeUnit.SECONDS),
                  "stopRunning did not cancel while the lookup held dbLock");
              throw new HopDatabaseException(
                  "The query was canceled", new SQLException("The query was canceled"));
            });
    doAnswer(
            invocation -> {
              cancelCalled.countDown();
              return null;
            })
        .when(mockTransformDataInterface.db)
        .cancelStatement(any(PreparedStatement.class));

    AtomicReference<Throwable> queryFailure = new AtomicReference<>();
    AtomicBoolean keepGoing = new AtomicBoolean(true);
    Thread queryThread =
        new Thread(
            () -> {
              try {
                keepGoing.set(mockDatabaseJoin.processRow());
              } catch (Throwable t) {
                queryFailure.set(t);
              }
            },
            "database-join-lookup");
    queryThread.setDaemon(true);
    queryThread.start();
    assertTrue(queryStarted.await(5, TimeUnit.SECONDS), "lookup did not reach the query");

    AtomicReference<Throwable> stopFailure = new AtomicReference<>();
    Thread stopThread =
        new Thread(
            () -> {
              try {
                // Pipeline.stopTransform sets the flag before stopRunning().
                mockDatabaseJoin.setStopped(true);
                mockDatabaseJoin.stopRunning();
              } catch (Throwable t) {
                stopFailure.set(t);
              }
            },
            "database-join-stop");
    stopThread.setDaemon(true);
    stopThread.start();
    stopThread.join(3_000);

    assertFalse(stopThread.isAlive(), "stopRunning deadlocked while the lookup held dbLock");
    assertNull(stopFailure.get());
    queryThread.join(3_000);
    assertFalse(queryThread.isAlive(), "lookup did not finish after the query was cancelled");
    assertNull(queryFailure.get());
    assertFalse(keepGoing.get());
    assertEquals(0, mockDatabaseJoin.getErrors());
    verify(mockDatabaseJoin, never()).stopAll();
    verify(mockDatabaseJoin, never()).logError(anyString(), any(Throwable.class));
    verify(mockTransformDataInterface.db, times(1)).cancelStatement(any(PreparedStatement.class));
  }

  /** A cancel that surfaces from the running lookup is a clean stop, not a failed SQL query. */
  @Test
  void cancelDuringQueryEndsWithNoErrors() throws Exception {
    stubRunningLookup();
    when(mockTransformDataInterface.db.openQuery(
            any(PreparedStatement.class), any(IRowMeta.class), any(Object[].class)))
        .thenThrow(
            new HopDatabaseException(
                "The query was canceled", new SQLException("The query was canceled")));
    mockDatabaseJoin.setStopped(true);

    assertFalse(mockDatabaseJoin.processRow());

    assertEquals(0, mockDatabaseJoin.getErrors());
    verify(mockDatabaseJoin, never()).stopAll();
    verify(mockDatabaseJoin, never()).logError(anyString(), any(Throwable.class));
    verify(mockDatabaseJoin).logDebug(anyString());
  }

  /** A query error while the pipeline is still running is still a failure. */
  @Test
  void queryFailureWhileRunningSetsAnError() throws Exception {
    stubRunningLookup();
    doNothing().when(mockDatabaseJoin).stopAll();
    doNothing().when(mockDatabaseJoin).setOutputDone();
    when(mockTransformDataInterface.db.openQuery(
            any(PreparedStatement.class), any(IRowMeta.class), any(Object[].class)))
        .thenThrow(new HopDatabaseException("syntax error", new SQLException("bad sql")));

    assertFalse(mockDatabaseJoin.processRow());

    assertEquals(1, mockDatabaseJoin.getErrors());
    verify(mockDatabaseJoin).stopAll();
    verify(mockDatabaseJoin).logError(anyString(), any(Throwable.class));
  }

  /** Skip the first-row metadata setup and block in the lookup query itself. */
  private void stubRunningLookup() throws HopException {
    mockDatabaseJoin.first = false;
    mockTransformDataInterface.lookupRowMeta = new RowMeta();
    mockTransformDataInterface.keynrs = new int[0];
    doReturn(new Object[] {"in"}).when(mockDatabaseJoin).getRow();
    doReturn(new RowMeta()).when(mockDatabaseJoin).getInputRowMeta();
    doNothing().when(mockDatabaseJoin).logDebug(anyString());
    doNothing().when(mockDatabaseJoin).logError(anyString(), any(Throwable.class));
  }
}
