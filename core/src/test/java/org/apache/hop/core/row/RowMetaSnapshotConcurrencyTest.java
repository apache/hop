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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

/**
 * {@link RowMeta#size()} and {@link RowMeta#getValueMeta(int)} read a volatile snapshot instead of
 * taking the read lock. These tests pin the two properties that makes rely on: the snapshot always
 * agrees with the list it was published from, and concurrent readers never see a torn or stale one
 * while another thread mutates the row meta.
 */
class RowMetaSnapshotConcurrencyTest {

  private static IValueMeta field(int i) {
    return new ValueMetaString("field" + i);
  }

  @Test
  void snapshotTracksEveryMutation() throws Exception {
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
}
