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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

/**
 * Lock-free readers of {@link RowMeta} while another thread grows it. Appends and inserts never
 * shrink the row, so a reader that saw a size must find a field - the right one, for appends - at
 * every index below it, also through the comparisons and the name lookups.
 */
class RowMetaAppendConcurrencyTest {

  private static final int READERS = 4;

  /** Runs the readers until the writer is done, and fails with the first error of any thread. */
  private static void run(Runnable writer, Runnable reader) throws Exception {
    CountDownLatch start = new CountDownLatch(1);
    AtomicBoolean writing = new AtomicBoolean(true);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    List<Thread> threads = new ArrayList<>();

    for (int r = 0; r < READERS; r++) {
      threads.add(
          new Thread(
              () -> {
                try {
                  start.await();
                  while (writing.get() && failure.get() == null) {
                    reader.run();
                  }
                  // one more pass over the final state
                  reader.run();
                } catch (Throwable e) {
                  failure.compareAndSet(null, e);
                }
              }));
    }
    threads.add(
        new Thread(
            () -> {
              try {
                start.await();
                writer.run();
              } catch (Throwable e) {
                failure.compareAndSet(null, e);
              } finally {
                writing.set(false);
              }
            }));

    for (Thread thread : threads) {
      thread.start();
    }
    start.countDown();
    for (Thread thread : threads) {
      thread.join(TimeUnit.MINUTES.toMillis(2));
      assertFalse(thread.isAlive(), "a thread did not finish in time");
    }
    if (failure.get() != null) {
      throw new AssertionError("concurrent access failed", failure.get());
    }
  }

  private static Object[] rowOfValues(int length) {
    Object[] row = RowDataUtil.allocateRowData(length);
    for (int i = 0; i < length; i++) {
      row[i] = "v" + i;
    }
    return row;
  }

  @Test
  void readersFindTheRightFieldBelowTheSizeTheySawWhileAnotherThreadAppends() throws Exception {
    final int rounds = 10;
    final int appends = 2_000;
    AtomicReference<RowMeta> current = new AtomicReference<>(new RowMeta());
    Object[] row = rowOfValues(appends);
    Object[] same = rowOfValues(appends);

    Runnable writer =
        () -> {
          for (int round = 0; round < rounds; round++) {
            RowMeta rowMeta = new RowMeta();
            current.set(rowMeta);
            for (int i = 0; i < appends; i++) {
              rowMeta.addValueMeta(new ValueMetaString("f" + i));
            }
          }
        };

    Runnable reader =
        () -> {
          RowMeta rowMeta = current.get();
          int size = rowMeta.size();
          if (size == 0) {
            return;
          }
          int last = size - 1;
          int random = ThreadLocalRandom.current().nextInt(size);
          for (int i : new int[] {0, size / 2, random, last}) {
            IValueMeta valueMeta = rowMeta.getValueMeta(i);
            assertNotNull(valueMeta, "getValueMeta(" + i + ") below size " + size);
            assertEquals("f" + i, valueMeta.getName());
          }
          assertTrue(rowMeta.size() >= size, "the size went down");
          assertEquals(last, rowMeta.indexOfValue("f" + last));
          assertEquals(random, rowMeta.indexOfValue("F" + random));
          assertNotNull(rowMeta.searchValueMeta("f" + last));
          String[] names = rowMeta.getFieldNames();
          assertTrue(names.length >= size);
          assertEquals("f" + last, names[last]);
          assertEquals("v" + last, assertDoesNotThrowGetString(rowMeta, row, last));
          try {
            assertEquals(0, rowMeta.compare(row, same));
            assertEquals(0, rowMeta.compare(row, same, new int[] {0, random, last}));
            assertTrue(rowMeta.equals(row, same, new int[] {last, 0}));
            assertEquals(0, rowMeta.compare(row, same, new int[] {last}, new int[] {last}));
            assertEquals(
                0, rowMeta.compare(row, rowMeta, same, new int[] {last}, new int[] {last}));
          } catch (Exception e) {
            throw new AssertionError(e);
          }
          List<IValueMeta> list = rowMeta.getValueMetaList();
          assertTrue(list.size() >= size);
          assertEquals("f" + last, list.get(last).getName());
        };

    run(writer, reader);
    assertEquals(appends, current.get().size());
  }

  private static String assertDoesNotThrowGetString(RowMeta rowMeta, Object[] row, int index) {
    try {
      return rowMeta.getString(row, index);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  @Test
  void readersNeverSeeAMissingFieldWhileAnotherThreadInsertsAndAppends() throws Exception {
    final int base = 100;
    final int stable = base / 2;
    final int operations = 1_500;
    RowMeta rowMeta = new RowMeta();
    for (int i = 0; i < base; i++) {
      rowMeta.addValueMeta(new ValueMetaString("base" + i));
    }
    Object[] row = rowOfValues(base + 2 * operations);
    Object[] same = rowOfValues(base + 2 * operations);

    Runnable writer =
        () -> {
          for (int n = 0; n < operations; n++) {
            // inserting at half the size never moves the first half of the base fields
            rowMeta.addValueMeta(rowMeta.size() / 2, new ValueMetaString("ins" + n));
            rowMeta.addValueMeta(new ValueMetaString("app" + n));
          }
        };

    ThreadLocal<int[]> lastSizeSeen = ThreadLocal.withInitial(() -> new int[] {0});
    Runnable reader =
        () -> {
          int size = rowMeta.size();
          int[] lastSize = lastSizeSeen.get();
          assertTrue(size >= lastSize[0], "the size went down");
          lastSize[0] = size;

          ThreadLocalRandom random = ThreadLocalRandom.current();
          for (int k = 0; k < 8; k++) {
            int i = random.nextInt(size);
            IValueMeta valueMeta = rowMeta.getValueMeta(i);
            assertNotNull(valueMeta, "getValueMeta(" + i + ") below size " + size);
            String name = valueMeta.getName();
            assertTrue(
                name.startsWith("base") || name.startsWith("ins") || name.startsWith("app"),
                "unexpected field name: " + name);
          }
          int i = random.nextInt(stable);
          assertEquals("base" + i, rowMeta.getValueMeta(i).getName());
          assertEquals(i, rowMeta.indexOfValue("BASE" + i));
          assertNotNull(rowMeta.getValueMeta(size - 1));
          try {
            assertEquals(0, rowMeta.compare(row, same));
            assertEquals(0, rowMeta.compare(row, same, new int[] {size - 1, 0}));
            assertTrue(rowMeta.equals(row, same, new int[] {size - 1}));
          } catch (Exception e) {
            throw new AssertionError(e);
          }

          List<IValueMeta> list = rowMeta.getValueMetaList();
          assertTrue(list.size() >= size);
          for (int j = 0; j < stable; j++) {
            assertEquals("base" + j, list.get(j).getName());
          }
          for (IValueMeta valueMeta : list) {
            assertNotNull(valueMeta);
          }
        };

    run(writer, reader);
    assertEquals(base + 2 * operations, rowMeta.size());
    assertEquals("app" + (operations - 1), rowMeta.getValueMeta(rowMeta.size() - 1).getName());
    for (int i = 0; i < stable; i++) {
      assertEquals(i, rowMeta.indexOfValue("base" + i));
    }
  }
}
