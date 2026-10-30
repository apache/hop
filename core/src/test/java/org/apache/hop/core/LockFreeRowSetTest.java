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

package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.hop.core.row.RowMeta;
import org.junit.jupiter.api.Test;

class LockFreeRowSetTest {

  @Test
  void keepsOrderAndReportsSize() {
    LockFreeRowSet rowSet = new LockFreeRowSet(4);
    Object[] a = {1L};
    Object[] b = {2L};
    assertTrue(rowSet.putRow(new RowMeta(), a));
    assertTrue(rowSet.putRow(new RowMeta(), b));
    assertEquals(2, rowSet.size());
    assertSame(a, rowSet.getRowImmediate());
    assertSame(b, rowSet.getRow());
    assertEquals(0, rowSet.size());
    assertNull(rowSet.getRowImmediate());
  }

  @Test
  void timesOutWhenFullOrEmpty() {
    LockFreeRowSet rowSet = new LockFreeRowSet(2);
    assertTrue(rowSet.putRowWait(new RowMeta(), new Object[] {1}, 10, TimeUnit.MILLISECONDS));
    assertTrue(rowSet.putRowWait(new RowMeta(), new Object[] {2}, 10, TimeUnit.MILLISECONDS));
    assertFalse(rowSet.putRowWait(new RowMeta(), new Object[] {3}, 10, TimeUnit.MILLISECONDS));
    assertEquals(2, rowSet.size());
    rowSet.clear();
    assertEquals(0, rowSet.size());
    assertNull(rowSet.getRowWait(10, TimeUnit.MILLISECONDS));
  }

  @Test
  void holdsTheConfiguredNumberOfRowsNotTheRoundedUpCapacity() {
    LockFreeRowSet rowSet = new LockFreeRowSet(10); // 16 slots
    for (int i = 0; i < 10; i++) {
      assertTrue(rowSet.putRowWait(new RowMeta(), new Object[] {i}, 0, TimeUnit.MILLISECONDS));
    }
    assertFalse(rowSet.putRowWait(new RowMeta(), new Object[] {10}, 0, TimeUnit.MILLISECONDS));
    assertEquals(10, rowSet.size());
    for (int i = 0; i < 10; i++) {
      assertEquals(i, rowSet.getRowImmediate()[0]);
      assertTrue(rowSet.putRowWait(new RowMeta(), new Object[] {10 + i}, 0, TimeUnit.MILLISECONDS));
      assertEquals(10, rowSet.size());
    }
  }

  @Test
  void reportsTheExactNumberOfRowsWhileRowsMove() throws Exception {
    // the writer and the reader move in step, so the row set always holds 100 to 102 rows
    int keep = 100;
    LockFreeRowSet rowSet = new LockFreeRowSet(1000);
    AtomicLong put = new AtomicLong();
    AtomicLong taken = new AtomicLong();
    AtomicBoolean stop = new AtomicBoolean();
    AtomicBoolean rowMissing = new AtomicBoolean();
    Thread writer =
        new Thread(
            () -> {
              for (long i = 0; !stop.get(); i++) {
                while (taken.get() < i - keep - 1) {
                  if (stop.get()) {
                    return;
                  }
                  Thread.yield();
                }
                rowSet.putRow(new RowMeta(), new Object[] {i});
                put.set(i + 1);
              }
            });
    Thread reader =
        new Thread(
            () -> {
              for (long i = 0; !stop.get(); i++) {
                while (put.get() < i + keep + 1) {
                  if (stop.get()) {
                    return;
                  }
                  Thread.yield();
                }
                if (rowSet.getRowImmediate() == null) {
                  rowMissing.set(true);
                }
                taken.set(i + 1);
              }
            });
    writer.start();
    reader.start();
    try {
      while (put.get() <= keep) {
        Thread.yield();
      }
      long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
      int checks = 0;
      while (System.nanoTime() < end) {
        int size = rowSet.size();
        assertTrue(size >= keep && size <= keep + 2, "size " + size + " after " + checks);
        checks++;
      }
    } finally {
      stop.set(true);
      writer.join(10_000);
      reader.join(10_000);
    }
    assertFalse(rowMissing.get());
    assertTrue(taken.get() > 0, "no rows moved");
  }

  @Test
  void aReaderInAShortParkIsWokenByABatchOnly() {
    // 64 rows: a wake batch of 16
    LockFreeRowSet rowSet = new LockFreeRowSet(64);
    Thread reader = new Thread(() -> {});
    rowSet.parkedConsumer = reader;
    for (int i = 0; i < 15; i++) {
      rowSet.putRow(new RowMeta(), new Object[] {i});
    }
    assertSame(reader, rowSet.parkedConsumer, "woken before a batch was queued");
    for (int i = 15; i < 17; i++) {
      rowSet.putRow(new RowMeta(), new Object[] {i});
    }
    assertNull(rowSet.parkedConsumer, "not woken once a batch was queued");
  }

  @Test
  void aReaderInALongParkIsWokenByTheNextRow() {
    LockFreeRowSet rowSet = new LockFreeRowSet(64);
    rowSet.putRow(new RowMeta(), new Object[] {0}); // the next row is not one the producer checks
    Thread reader = new Thread(() -> {});
    rowSet.wakeOnAnyRow = true;
    rowSet.parkedConsumer = reader;
    rowSet.putRow(new RowMeta(), new Object[] {1});
    assertNull(rowSet.parkedConsumer, "not woken by a single row");
  }

  @Test
  void aParkedWriterIsWokenOnceABatchIsFree() {
    LockFreeRowSet rowSet = new LockFreeRowSet(64);
    for (int i = 0; i < 64; i++) {
      rowSet.putRow(new RowMeta(), new Object[] {i});
    }
    Thread writer = new Thread(() -> {});
    rowSet.parkedProducer = writer;
    for (int i = 0; i < 15; i++) {
      rowSet.getRowImmediate();
    }
    assertSame(writer, rowSet.parkedProducer, "woken before a batch was free");
    rowSet.getRowImmediate();
    assertNull(rowSet.parkedProducer, "not woken once a batch was free");
  }

  @Test
  void anIdleReaderBacksOffAndIsWokenByASingleRow() throws Exception {
    LockFreeRowSet rowSet = new LockFreeRowSet(10_000);
    // a reader that found nothing goes on with longer parks
    assertNull(rowSet.getRowWait(30, TimeUnit.MILLISECONDS));
    assertTrue(rowSet.consumerParkNanos > LockFreeRowSetFields.MAX_PARK_NANOS);
    assertFalse(rowSet.wakeOnAnyRow);
    assertNull(rowSet.parkedConsumer);

    Object[][] received = new Object[1][];
    Thread reader = new Thread(() -> received[0] = rowSet.getRowWait(10, TimeUnit.SECONDS));
    reader.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!rowSet.wakeOnAnyRow && System.nanoTime() < deadline) {
      Thread.sleep(1);
    }
    assertTrue(rowSet.wakeOnAnyRow, "the reader never parked long");
    Object[] row = {1};
    rowSet.putRow(new RowMeta(), row);
    reader.join(5_000);
    assertSame(row, received[0]);
    // rows flow again: back to short parks woken by batches
    assertEquals(LockFreeRowSetFields.MAX_PARK_NANOS, rowSet.consumerParkNanos);
    assertFalse(rowSet.wakeOnAnyRow);
    assertNull(rowSet.parkedConsumer);
  }

  @Test
  void awaitAnyOnlyReportsAParkThatRanItsFullTime() {
    LockFreeRowSet empty = new LockFreeRowSet(8);
    LockFreeRowSet withRow = new LockFreeRowSet(8);
    withRow.putRow(new RowMeta(), new Object[] {1});
    LockSupport.parkNanos(1); // use up a wake-up an earlier test may have left on this thread

    assertTrue(LockFreeRowSet.awaitAny(List.of(empty), TimeUnit.MILLISECONDS.toNanos(5)));
    assertFalse(LockFreeRowSet.awaitAny(List.of(empty, withRow), TimeUnit.SECONDS.toNanos(10)));
    // a wake-up left over from earlier ends the park at once: that is no reason to wait longer
    LockSupport.unpark(Thread.currentThread());
    assertFalse(LockFreeRowSet.awaitAny(List.of(empty), TimeUnit.SECONDS.toNanos(10)));
  }

  @Test
  void anInterruptEndsAwaitAnyAndIsCleared() {
    // otherwise every later park returns at once and the transform spins on a full core
    LockFreeRowSet rowSet = new LockFreeRowSet(8);
    Thread.currentThread().interrupt();
    long start = System.nanoTime();
    LockFreeRowSet.awaitAny(List.of(rowSet), TimeUnit.SECONDS.toNanos(10));
    assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 1000);
    assertFalse(Thread.interrupted());
    assertNull(rowSet.parkedConsumer);
  }

  @Test
  void rowsPutBeforeDoneAreStillReadable() {
    LockFreeRowSet rowSet = new LockFreeRowSet(8);
    rowSet.putRow(new RowMeta(), new Object[] {1});
    rowSet.setDone();
    assertTrue(rowSet.isDone());
    assertEquals(1, rowSet.size());
    assertEquals(1, rowSet.getRowImmediate()[0]);
  }

  @Test
  void waitsWithAHugeTimeoutInsteadOfGivingUp() throws Exception {
    // RowProducer uses Long.MAX_VALUE days: the deadline must not overflow into "expired"
    LockFreeRowSet rowSet = new LockFreeRowSet(2);
    rowSet.putRow(new RowMeta(), new Object[] {1});
    rowSet.putRow(new RowMeta(), new Object[] {2});
    Thread consumer =
        new Thread(
            () -> {
              try {
                Thread.sleep(200);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              rowSet.getRowImmediate();
            });
    consumer.start();
    long start = System.nanoTime();
    assertTrue(rowSet.putRowWait(new RowMeta(), new Object[] {3}, Long.MAX_VALUE, TimeUnit.DAYS));
    assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) >= 150);
    consumer.join();
  }

  @Test
  void awaitAnyReturnsAtOnceWhenAnInputHasRowsOrIsDone() {
    LockFreeRowSet empty = new LockFreeRowSet(8);
    LockFreeRowSet withRow = new LockFreeRowSet(8);
    LockFreeRowSet done = new LockFreeRowSet(8);
    withRow.putRow(new RowMeta(), new Object[] {1});
    done.setDone();

    long start = System.nanoTime();
    LockFreeRowSet.awaitAny(List.of(empty, withRow), TimeUnit.SECONDS.toNanos(10));
    LockFreeRowSet.awaitAny(List.of(empty, done), TimeUnit.SECONDS.toNanos(10));
    assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 1000);
  }

  @Test
  void awaitAnyLeavesNoParkedReaderBehind() {
    LockFreeRowSet a = new LockFreeRowSet(8);
    LockFreeRowSet b = new LockFreeRowSet(8);
    LockFreeRowSet.awaitAny(List.of(a, b), TimeUnit.MILLISECONDS.toNanos(5));
    assertNull(a.parkedConsumer);
    assertNull(b.parkedConsumer);
  }

  @Test
  void awaitAnyIsWokenByRowsOnAnyInput() throws Exception {
    // wake-ups come in batches of a quarter of the buffer: 2 rows for a buffer of 8
    LockFreeRowSet a = new LockFreeRowSet(8);
    LockFreeRowSet b = new LockFreeRowSet(8);
    List<IRowSet> inputs = List.of(a, b);
    Thread producer =
        new Thread(
            () -> {
              try {
                Thread.sleep(50);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              b.putRow(new RowMeta(), new Object[] {1});
              b.putRow(new RowMeta(), new Object[] {2});
            });
    producer.start();
    while (b.size() == 0) {
      LockFreeRowSet.awaitAny(inputs, TimeUnit.SECONDS.toNanos(10));
    }
    producer.join();
    assertEquals(2, b.size());
    assertEquals(0, a.size());
  }

  @Test
  void deliversEveryRowOnceWithSeveralProducers() throws Exception {
    LockFreeRowSet rowSet = new LockFreeRowSet(1000);
    int producers = 4;
    int perProducer = 250_000;
    List<Thread> threads = new ArrayList<>();
    for (int p = 0; p < producers; p++) {
      long base = (long) p * perProducer;
      Thread t =
          new Thread(
              () -> {
                for (int i = 0; i < perProducer; i++) {
                  Object[] row = {base + i};
                  while (!rowSet.putRow(new RowMeta(), row)) {
                    // full: retry, like BaseTransform does
                  }
                }
              });
      threads.add(t);
      t.start();
    }
    boolean[] seen = new boolean[producers * perProducer];
    int received = 0;
    while (received < seen.length) {
      Object[] row = rowSet.getRowWait(50, TimeUnit.MILLISECONDS);
      if (row != null) {
        int id = (int) (long) (Long) row[0];
        assertFalse(seen[id], "row " + id + " delivered twice");
        seen[id] = true;
        received++;
      }
    }
    for (Thread t : threads) {
      t.join(10_000);
    }
    assertNull(rowSet.getRowImmediate());
    assertEquals(0, rowSet.size());
  }
}
