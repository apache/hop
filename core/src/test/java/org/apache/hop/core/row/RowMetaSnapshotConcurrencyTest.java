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

package org.apache.hop.core.row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

/**
 * {@link RowMeta#size()}, {@link RowMeta#getValueMeta(int)} and the row comparisons read a volatile
 * snapshot instead of taking the read lock. These tests pin the two properties that makes rely on:
 * the snapshot always agrees with the list it was published from, and concurrent readers never see
 * a torn or stale one while another thread mutates the row meta.
 */
class RowMetaSnapshotConcurrencyTest {

  private static IValueMeta field(int i) {
    return new ValueMetaString("field" + i);
  }

  @Test
  void snapshotTracksEveryMutation() {
    RowMeta rowMeta = new RowMeta();
    assertEquals(0, rowMeta.size());

    for (int i = 0; i < 10; i++) {
      rowMeta.addValueMeta(field(i));
      assertEquals(i + 1, rowMeta.size());
      assertEquals("field" + i, rowMeta.getValueMeta(i).getName());
    }

    rowMeta.addValueMeta(3, new ValueMetaString("inserted"));
    assertEquals(11, rowMeta.size());
    assertEquals("inserted", rowMeta.getValueMeta(3).getName());

    rowMeta.setValueMeta(0, new ValueMetaString("replaced"));
    assertEquals("replaced", rowMeta.getValueMeta(0).getName());

    rowMeta.removeValueMeta(3);
    assertEquals(10, rowMeta.size());
    assertEquals("field3", rowMeta.getValueMeta(3).getName());

    List<IValueMeta> wholesale = new ArrayList<>();
    wholesale.add(new ValueMetaString("only"));
    rowMeta.setValueMetaList(wholesale);
    assertEquals(1, rowMeta.size());
    assertEquals("only", rowMeta.getValueMeta(0).getName());

    // a clone must carry its own populated snapshot, not the empty one from the delegated
    // constructor
    RowMeta copy = rowMeta.clone();
    assertEquals(1, copy.size());
    assertEquals("only", copy.getValueMeta(0).getName());

    rowMeta.clear();
    assertEquals(0, rowMeta.size());
  }

  @Test
  void readersSeeAConsistentSnapshotWhileTheRowMetaChanges() throws Exception {
    RowMeta rowMeta = new RowMeta();
    for (int i = 0; i < 20; i++) {
      rowMeta.addValueMeta(field(i));
    }

    int readers = 4;
    CountDownLatch start = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    List<Thread> threads = new ArrayList<>();

    for (int r = 0; r < readers; r++) {
      Thread t =
          new Thread(
              () -> {
                try {
                  start.await();
                  for (int n = 0; n < 200_000; n++) {
                    int size = rowMeta.size();
                    // size() and getValueMeta() are two separate operations, so the row meta may
                    // change in between - that was equally true when each took the read lock, and
                    // is not what this asserts. What must hold is that a read never throws, never
                    // tears, and never hands back a half-published value meta.
                    IValueMeta vm = rowMeta.getValueMeta(size - 1);
                    if (vm != null) {
                      assertNotNull(vm.getName());
                      assertTrue(
                          vm.getName().startsWith("field") || vm.getName().startsWith("extra"),
                          "unexpected field name: " + vm.getName());
                    }
                    // comfortably out of range must be null rather than throwing
                    assertEquals(null, rowMeta.getValueMeta(size + 1000));
                    assertEquals(null, rowMeta.getValueMeta(-1));
                  }
                } catch (Throwable e) {
                  failure.compareAndSet(null, e);
                }
              });
      threads.add(t);
      t.start();
    }

    Thread writer =
        new Thread(
            () -> {
              try {
                start.await();
                for (int n = 0; n < 20_000; n++) {
                  rowMeta.addValueMeta(new ValueMetaString("extra" + n));
                  rowMeta.removeValueMeta(rowMeta.size() - 1);
                }
              } catch (Throwable e) {
                failure.compareAndSet(null, e);
              }
            });
    threads.add(writer);
    writer.start();

    start.countDown();
    for (Thread t : threads) {
      t.join(TimeUnit.MINUTES.toMillis(1));
    }

    if (failure.get() != null) {
      throw new AssertionError("concurrent access failed", failure.get());
    }
    assertEquals(20, rowMeta.size());
  }

  private static RowMeta keyedRowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    IValueMeta descending = new ValueMetaInteger("amount");
    descending.setSortedDescending(true);
    rowMeta.addValueMeta(descending);
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  @Test
  void comparisonsFollowTheKeyOrderAndSortDirection() throws Exception {
    RowMeta rowMeta = keyedRowMeta();
    Object[] low = {1L, 10L, "a"};
    Object[] high = {2L, 10L, "a"};
    Object[] sameIdLargerAmount = {1L, 20L, "a"};
    Object[] sameKeysOtherName = {1L, 10L, "b"};
    int[] keys = {0, 1};

    assertTrue(rowMeta.compare(low, high, keys) < 0);
    assertTrue(rowMeta.compare(high, low, keys) > 0);
    // amount sorts descending, so the larger amount comes first
    assertTrue(rowMeta.compare(sameIdLargerAmount, low, keys) < 0);
    assertEquals(0, rowMeta.compare(low, sameKeysOtherName, keys));
    assertTrue(rowMeta.equals(low, sameKeysOtherName, keys));
    assertFalse(rowMeta.equals(low, high, keys));

    // all fields
    assertEquals(0, rowMeta.compare(low, low.clone()));
    assertTrue(rowMeta.compare(low, sameKeysOtherName) < 0);

    // different key positions on each side: name of the first row against name of the second
    Object[] swapped = {"a", 1L};
    RowMeta swappedMeta = new RowMeta();
    swappedMeta.addValueMeta(new ValueMetaString("name"));
    swappedMeta.addValueMeta(new ValueMetaInteger("id"));
    assertEquals(0, rowMeta.compare(low, swappedMeta, swapped, new int[] {0, 2}, new int[] {1, 0}));
    assertTrue(rowMeta.compare(high, swappedMeta, swapped, new int[] {0, 2}, new int[] {1, 0}) > 0);

    // two key lists on rows of the same layout: id of the first row against amount of the second
    assertTrue(rowMeta.compare(low, high, new int[] {0}, new int[] {1}) < 0);
    assertEquals(
        0, rowMeta.compare(new Object[] {10L, 0L, "a"}, low, new int[] {0}, new int[] {1}));
  }

  @Test
  void comparisonsStayCorrectWhileTheRowMetaChanges() throws Exception {
    RowMeta rowMeta = keyedRowMeta();
    // one spare position: the writer adds and removes a fourth field
    Object[] low = {1L, 10L, "a", null};
    Object[] high = {2L, 10L, "a", null};
    int[] keys = {0, 1};

    CountDownLatch start = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    List<Thread> threads = new ArrayList<>();

    for (int r = 0; r < 4; r++) {
      Thread t =
          new Thread(
              () -> {
                try {
                  start.await();
                  for (int n = 0; n < 200_000; n++) {
                    assertTrue(rowMeta.compare(low, high, keys) < 0);
                    assertTrue(rowMeta.compare(high, low, keys, keys) > 0);
                    assertTrue(rowMeta.compare(low, rowMeta, high, keys, keys) < 0);
                    assertTrue(rowMeta.equals(low, low, keys));
                    // all fields, with or without the writer's extra field
                    assertEquals(0, rowMeta.compare(low, low));
                    assertTrue(rowMeta.compare(low, high) < 0);
                  }
                } catch (Throwable e) {
                  failure.compareAndSet(null, e);
                }
              });
      threads.add(t);
      t.start();
    }

    Thread writer =
        new Thread(
            () -> {
              try {
                start.await();
                for (int n = 0; n < 20_000; n++) {
                  rowMeta.addValueMeta(new ValueMetaString("extra" + n));
                  rowMeta.removeValueMeta(rowMeta.size() - 1);
                }
              } catch (Throwable e) {
                failure.compareAndSet(null, e);
              }
            });
    threads.add(writer);
    writer.start();

    start.countDown();
    for (Thread t : threads) {
      t.join(TimeUnit.MINUTES.toMillis(1));
    }

    if (failure.get() != null) {
      throw new AssertionError("concurrent comparison failed", failure.get());
    }
    assertEquals(3, rowMeta.size());
  }
}
