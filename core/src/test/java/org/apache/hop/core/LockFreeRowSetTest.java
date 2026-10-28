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
