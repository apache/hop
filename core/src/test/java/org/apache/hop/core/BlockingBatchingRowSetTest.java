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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Test class for the basic functionality of the blocking & batching row set. */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class BlockingBatchingRowSetTest {

  public IRowMeta createRowMetaInterface() {
    IRowMeta rm = new RowMeta();

    IValueMeta[] valuesMeta = {
      new ValueMetaInteger("ROWNR"),
    };

    for (IValueMeta iValueMeta : valuesMeta) {
      rm.addValueMeta(iValueMeta);
    }

    return rm;
  }

  /** The basic stuff. */
  @Test
  void testBasicCreation() {
    IRowSet set = new BlockingBatchingRowSet(10);

    assertFalse(set.isDone());
    assertEquals(0, set.size());
  }

  /** Functionality test. */
  @Test
  void testFunctionality1() {
    BlockingBatchingRowSet set = new BlockingBatchingRowSet(10);

    IRowMeta rm = createRowMetaInterface();

    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      rows.add(
          new Object[] {
            (long) i,
          });
    }

    assertEquals(0, set.size());

    // Pop off row. This should return null (no row available: has a timeout)
    //
    Object[] r = set.getRow();
    assertNull(r);

    // Add rows. set doesn't report rows, batches them
    // this batching row set has 2 buffers with 2 rows, the 5th row will cause the rows to be
    // exposed.
    //
    int index = 0;
    while (index < 4) {
      set.putRow(rm, rows.get(index++));
      assertEquals(0, set.size());
    }
    set.putRow(rm, rows.get(index));
    assertEquals(5, set.size());

    // Signal done...
    //
    set.setDone();
    assertTrue(set.isDone());

    // Get a row back...
    //
    r = set.getRow();
    assertNotNull(r);
    assertArrayEquals(rows.get(0), r);

    // Get a row back...
    //
    r = set.getRow();
    assertNotNull(r);
    assertArrayEquals(rows.get(1), r);

    // Get a row back...
    //
    r = set.getRow();
    assertNotNull(r);
    assertArrayEquals(rows.get(2), r);
  }

  /**
   * Issue #7742: the last partial batch must be enqueued before the done flag is set, otherwise a
   * concurrent consumer can observe isDone() and exit before the last rows are available.
   */
  @Test
  void testSetDoneEnqueuesPartialBatchBeforeDoneFlag() throws Exception {
    BlockingBatchingRowSet set = new BlockingBatchingRowSet(10);
    IRowMeta rm = createRowMetaInterface();

    // 3 rows with buffer size 5 = partial batch (not flushed by putRow)
    for (int i = 0; i < 3; i++) {
      assertTrue(set.putRow(rm, new Object[] {(long) i}));
    }
    assertEquals(0, set.size());

    AtomicBoolean offerSeen = new AtomicBoolean(false);
    AtomicBoolean doneAtOffer = new AtomicBoolean(true); // fail closed if offer never checked
    instrumentGetArrayOffers(
        set,
        () -> {
          offerSeen.set(true);
          doneAtOffer.set(set.isDone());
        });

    set.setDone();

    assertTrue(offerSeen.get(), "partial batch must be offered in setDone()");
    assertFalse(
        doneAtOffer.get(),
        "done flag must not be set before the last batch is offered (issue #7742)");
    assertTrue(set.isDone());
  }

  /** When there is nothing left to flush, setDone() should only mark the rowset done. */
  @Test
  void testSetDoneWithNoPartialBatchOnlyMarksDone() throws Exception {
    BlockingBatchingRowSet set = new BlockingBatchingRowSet(10);
    AtomicBoolean offerSeen = new AtomicBoolean(false);
    instrumentGetArrayOffers(set, () -> offerSeen.set(true));

    set.setDone();

    assertFalse(offerSeen.get(), "no batch should be offered when nothing was put");
    assertTrue(set.isDone());
  }

  /**
   * When the last put already filled and published a full batch, setDone() must not re-offer; only
   * the done flag is set.
   */
  @Test
  void testSetDoneAfterFullBatchOnlyMarksDone() throws Exception {
    BlockingBatchingRowSet set = new BlockingBatchingRowSet(10);
    IRowMeta rm = createRowMetaInterface();

    // Exactly one full batch (size = maxSize / BATCHSIZE = 5)
    for (int i = 0; i < 5; i++) {
      assertTrue(set.putRow(rm, new Object[] {(long) i}));
    }
    assertEquals(5, set.size());

    AtomicBoolean offerSeen = new AtomicBoolean(false);
    instrumentGetArrayOffers(set, () -> offerSeen.set(true));

    set.setDone();

    assertFalse(offerSeen.get(), "full batch was already published; setDone must not re-offer");
    assertTrue(set.isDone());
  }

  /**
   * After setDone() with a partial batch, all non-null rows must be readable (sequential drain).
   */
  @Test
  void testPartialBatchDrainableAfterSetDone() {
    BlockingBatchingRowSet set = new BlockingBatchingRowSet(10);
    IRowMeta rm = createRowMetaInterface();
    final int rowCount = 3;

    for (int i = 0; i < rowCount; i++) {
      assertTrue(set.putRow(rm, new Object[] {(long) i}));
    }

    set.setDone();
    assertTrue(set.isDone());

    List<Object[]> received = drainNonNullRows(set);
    assertEquals(rowCount, received.size());
    for (int i = 0; i < rowCount; i++) {
      assertArrayEquals(new Object[] {(long) i}, received.get(i));
    }
  }

  /**
   * Concurrent producer/consumer using the same done-check pattern as BaseTransform.getRow().
   * Partial batches must never be lost across repeated runs (issue #7742).
   */
  @Test
  void testNoDataLossOnConcurrentSetDone() throws Exception {
    final int iterations = 200;
    final int rowCount = 3;
    IRowMeta rm = createRowMetaInterface();

    for (int iter = 0; iter < iterations; iter++) {
      BlockingBatchingRowSet set = new BlockingBatchingRowSet(10);
      List<Object[]> received = Collections.synchronizedList(new ArrayList<>());
      CountDownLatch consumerStarted = new CountDownLatch(1);
      AtomicReference<Throwable> consumerError = new AtomicReference<>();

      Thread consumer =
          new Thread(
              () -> {
                try {
                  consumerStarted.countDown();
                  // Mirrors BaseTransform handling when getRowWait returns null
                  while (true) {
                    Object[] row = set.getRowWait(20, TimeUnit.MILLISECONDS);
                    if (row != null) {
                      received.add(row);
                    } else if (set.isDone()) {
                      row = set.getRowWait(1, TimeUnit.MILLISECONDS);
                      if (row == null) {
                        break;
                      }
                      received.add(row);
                    }
                  }
                } catch (Throwable t) {
                  consumerError.set(t);
                }
              },
              "batching-rowset-consumer-" + iter);

      consumer.start();
      assertTrue(consumerStarted.await(5, TimeUnit.SECONDS));

      for (int i = 0; i < rowCount; i++) {
        assertTrue(set.putRow(rm, new Object[] {(long) i}));
      }
      set.setDone();

      consumer.join(10_000);
      assertFalse(consumer.isAlive(), "consumer did not finish at iteration " + iter);
      assertNull(consumerError.get(), () -> "consumer failed: " + consumerError.get());
      assertEquals(
          rowCount,
          received.size(),
          "data loss at iteration " + iter + ", got " + received.size() + " rows");
      for (int i = 0; i < rowCount; i++) {
        assertArrayEquals(new Object[] {(long) i}, received.get(i));
      }
    }
  }

  /**
   * Replace getArray with a queue that invokes {@code onOffer} for every subsequent offer. Existing
   * batches are preserved without firing the callback.
   */
  private static void instrumentGetArrayOffers(BlockingBatchingRowSet set, Runnable onOffer)
      throws Exception {
    Field getArrayField = BlockingBatchingRowSet.class.getDeclaredField("getArray");
    getArrayField.setAccessible(true);
    @SuppressWarnings("unchecked")
    BlockingQueue<Object[][]> originalGetArray = (BlockingQueue<Object[][]>) getArrayField.get(set);

    AtomicBoolean logOffers = new AtomicBoolean(false);
    BlockingQueue<Object[][]> loggingGetArray =
        new ArrayBlockingQueue<>(Math.max(2, originalGetArray.size() + 2), true) {
          @Override
          public boolean offer(Object[][] e) {
            if (logOffers.get()) {
              onOffer.run();
            }
            return super.offer(e);
          }

          @Override
          public boolean offer(Object[][] e, long timeout, TimeUnit unit)
              throws InterruptedException {
            if (logOffers.get()) {
              onOffer.run();
            }
            return super.offer(e, timeout, unit);
          }
        };
    // Preserve any batches already published (drainTo uses offer; logging still off)
    originalGetArray.drainTo(loggingGetArray);
    getArrayField.set(set, loggingGetArray);
    logOffers.set(true);
  }

  private static List<Object[]> drainNonNullRows(BlockingBatchingRowSet set) {
    List<Object[]> received = new ArrayList<>();
    Object[] row;
    // Partial batches pad with null sentinels; stop after the first null once we have started
    // seeing data, but keep polling while empty before setDone flush visibility in sequential use.
    while ((row = set.getRowWait(100, TimeUnit.MILLISECONDS)) != null) {
      received.add(row);
    }
    return received;
  }
}
