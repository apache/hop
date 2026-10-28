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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.hop.core.row.IRowMeta;

/**
 * A row buffer without locks on the hot path: a bounded multi-producer / single-consumer ring (the
 * design of JCTools' MpscArrayQueue).
 *
 * <ul>
 *   <li>A slot that is not null holds a row, so there is no separate sequence array.
 *   <li>Producers claim a slot with one compare-and-set on the producer index. They keep a cached
 *       limit (consumer index + capacity) and only read the consumer index again when they reach
 *       it, so a producer does not touch the consumer's cache line for every row.
 *   <li>A row set is read by exactly one transform thread, so the consumer moves its index with a
 *       plain release store. (Several producers are allowed: some transforms call putRow from
 *       worker threads, e.g. LanguageModelChat.)
 *   <li>The producer fields, the consumer fields and the parked-thread fields each sit on their own
 *       cache line (padding through the superclasses below, which the JVM lays out first).
 * </ul>
 *
 * <p>When the ring is full (or empty) the thread spins briefly, then parks. The other side only
 * unparks it once a batch of rows (or free slots) is ready, a quarter of the capacity, or when the
 * row set is done, claiming the parked thread so it is woken once. That lets a fast consumer
 * process runs of rows instead of waking up for every row, which is what the per-row size() check
 * and Thread.sleep in BaseTransform approximate for the lock-based row sets. Parking is bounded to
 * {@link LockFreeRowSetFields#MAX_PARK_NANOS}, so nothing can stall.
 */
public class LockFreeRowSet extends LockFreeRowSetPad5 implements Comparable<IRowSet>, IRowSet {
  private static final VarHandle SLOT = MethodHandles.arrayElementVarHandle(Object[][].class);
  private static final VarHandle PRODUCER_INDEX;
  private static final VarHandle CONSUMER_INDEX;
  private static final VarHandle PARKED_CONSUMER;
  private static final VarHandle PARKED_PRODUCER;

  static {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      PRODUCER_INDEX =
          lookup.findVarHandle(LockFreeRowSetProducerFields.class, "producerIndex", long.class);
      CONSUMER_INDEX =
          lookup.findVarHandle(LockFreeRowSetConsumerFields.class, "consumerIndex", long.class);
      PARKED_CONSUMER =
          lookup.findVarHandle(LockFreeRowSetParkedConsumer.class, "parkedConsumer", Thread.class);
      PARKED_PRODUCER =
          lookup.findVarHandle(LockFreeRowSetParkedProducer.class, "parkedProducer", Thread.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * @param maxSize the number of rows the buffer holds (rounded up to a power of two)
   */
  public LockFreeRowSet(int maxSize) {
    super(maxSize);
  }

  // ---- ring ----------------------------------------------------------------------------------

  /**
   * @return the claimed index, or -1 when the ring is full
   */
  private long offer(Object[] row) {
    long limit = producerLimit;
    long index;
    do {
      index = producerIndex;
      if (index >= limit) {
        limit = consumerIndex + capacity;
        if (index >= limit) {
          return -1; // full
        }
        producerLimit = limit;
      }
    } while (!PRODUCER_INDEX.compareAndSet(this, index, index + 1));
    SLOT.setRelease(slots, (int) (index & mask), row);
    return index;
  }

  private Object[] poll() {
    long index = consumerIndex;
    int offset = (int) (index & mask);
    Object[] row = (Object[]) SLOT.getAcquire(slots, offset);
    if (row == null) {
      if (index == producerIndex) {
        return null; // empty
      }
      // a producer claimed this slot and is about to fill it
      do {
        Thread.onSpinWait();
        row = (Object[]) SLOT.getAcquire(slots, offset);
      } while (row == null);
    }
    slots[offset] = null;
    CONSUMER_INDEX.setRelease(this, index + 1); // publishes the null above to producers
    return row;
  }

  // ---- waiting -------------------------------------------------------------------------------

  private static void wake(Thread thread) {
    if (thread != null) {
      LockSupport.unpark(thread);
    }
  }

  /**
   * Called by a producer after it added the row at {@code index}: wake a parked consumer once a
   * batch is queued. Only every {@code wakeCheckMask + 1}th row looks at the consumer's state, so a
   * producer does not read another thread's cache line for every row; a wake-up is late by at most
   * that many rows.
   */
  private void rowAdded(long index) {
    if ((index & wakeCheckMask) != 0) {
      return;
    }
    Thread consumer = parkedConsumer;
    // claim the parked thread first, so it is unparked once and not again for every row added
    // before it gets to run
    if (consumer != null
        && producerIndex - consumerIndex >= wakeBatch
        && PARKED_CONSUMER.compareAndSet(this, consumer, null)) {
      LockSupport.unpark(consumer);
    }
  }

  /** Called by the consumer after it took a row: wake a parked producer once a batch is free. */
  private void rowTaken() {
    if ((consumerIndex & wakeCheckMask) != 0) {
      return;
    }
    Thread producer = parkedProducer;
    if (producer != null
        && capacity - (producerIndex - consumerIndex) >= wakeBatch
        && PARKED_PRODUCER.compareAndSet(this, producer, null)) {
      LockSupport.unpark(producer);
    }
  }

  @Override
  public void setDone() {
    super.setDone();
    wake(parkedConsumer); // the last rows may never fill a batch
  }

  @Override
  public boolean putRow(IRowMeta rowMeta, Object[] rowData) {
    return putRowWait(rowMeta, rowData, timeoutPut, TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean putRowWait(IRowMeta rowMeta, Object[] rowData, long time, TimeUnit tu) {
    if (this.rowMeta != rowMeta) {
      this.rowMeta = rowMeta; // don't dirty this cache line for every row
    }
    if (rowData == null) {
      return false; // same as ArrayBlockingQueue, which refuses null
    }
    long index = offer(rowData);
    if (index >= 0) {
      rowAdded(index);
      return true;
    }
    for (int i = 0; i < SPINS; i++) {
      Thread.onSpinWait();
      index = offer(rowData);
      if (index >= 0) {
        rowAdded(index);
        return true;
      }
    }
    // elapsed-time comparison: RowProducer waits Long.MAX_VALUE days, where now + timeout overflows
    long timeoutNanos = tu.toNanos(time);
    long start = System.nanoTime();
    Thread current = Thread.currentThread();
    while (true) {
      parkedProducer = current;
      index = offer(rowData); // re-check after announcing ourselves, so a wake-up is not lost
      if (index >= 0) {
        parkedProducer = null;
        rowAdded(index);
        return true;
      }
      long remaining = timeoutNanos - (System.nanoTime() - start);
      if (remaining <= 0 || Thread.interrupted()) {
        parkedProducer = null;
        return false;
      }
      LockSupport.parkNanos(this, Math.min(remaining, MAX_PARK_NANOS));
    }
  }

  @Override
  public Object[] getRow() {
    return getRowWait(timeoutGet, TimeUnit.MILLISECONDS);
  }

  @Override
  public Object[] getRowImmediate() {
    Object[] row = poll();
    if (row != null) {
      rowTaken();
    }
    return row;
  }

  @Override
  public Object[] getRowWait(long timeout, TimeUnit tu) {
    Object[] row = poll();
    if (row != null) {
      rowTaken();
      return row;
    }
    for (int i = 0; i < SPINS; i++) {
      Thread.onSpinWait();
      row = poll();
      if (row != null) {
        rowTaken();
        return row;
      }
    }
    long timeoutNanos = tu.toNanos(timeout);
    long start = System.nanoTime();
    Thread current = Thread.currentThread();
    while (true) {
      parkedConsumer = current;
      row = poll();
      if (row != null) {
        parkedConsumer = null;
        rowTaken();
        return row;
      }
      long remaining = timeoutNanos - (System.nanoTime() - start);
      if (remaining <= 0 || Thread.interrupted()) {
        parkedConsumer = null;
        return null;
      }
      LockSupport.parkNanos(this, Math.min(remaining, MAX_PARK_NANOS));
    }
  }

  /**
   * Waits, as the reader of all these row sets, until one of them has rows or is done: spins
   * briefly, then parks until a {@link LockFreeRowSet} among them has a batch of rows ready or is
   * done, at most {@code maxNanos}. A transform with several inputs cannot wait on one of them with
   * {@link #getRowWait(long, TimeUnit)}: rows arriving on another one would not wake it.
   *
   * @param rowSets the input row sets of the calling transform
   * @param maxNanos the longest time to park; row sets of another type are only checked again then
   */
  public static void awaitAny(List<IRowSet> rowSets, long maxNanos) {
    for (int i = 0; i < SPINS; i++) {
      if (anyReady(rowSets)) {
        return;
      }
      Thread.onSpinWait();
    }
    Thread current = Thread.currentThread();
    for (IRowSet rowSet : rowSets) {
      if (rowSet instanceof LockFreeRowSet lockFree) {
        lockFree.parkedConsumer = current;
      }
    }
    // check again after announcing ourselves on every input, so a wake-up is not lost
    if (!anyReady(rowSets)) {
      LockSupport.parkNanos(rowSets, Math.min(maxNanos, MAX_PARK_NANOS));
    }
    for (IRowSet rowSet : rowSets) {
      if (rowSet instanceof LockFreeRowSet lockFree) {
        PARKED_CONSUMER.compareAndSet(lockFree, current, null);
      }
    }
  }

  private static boolean anyReady(List<IRowSet> rowSets) {
    for (IRowSet rowSet : rowSets) {
      if (rowSet.size() > 0 || rowSet.isDone()) {
        return true;
      }
    }
    return false;
  }

  /** The ring spins and parks by itself; the transform's per-row sleep only adds latency. */
  @Override
  public boolean needsThrottling() {
    return false;
  }

  @Override
  public int size() {
    long size = producerIndex - consumerIndex;
    return (int) Math.max(0, Math.min(size, capacity));
  }

  /** Only called when no transform reads from this row set anymore (see Pipeline). */
  @Override
  public void clear() {
    while (poll() != null) {
      // drain
    }
    done.set(false);
    wake(parkedProducer);
  }
}

// ---- field layout: the JVM places superclass fields first, so these classes keep the hot fields
// ---- of the producer, the consumer and the parked threads on separate 128-byte cache lines.

@SuppressWarnings("unused")
abstract class LockFreeRowSetPad0 extends BaseRowSet {
  long p00, p01, p02, p03, p04, p05, p06, p07, p08, p09, p0a, p0b, p0c, p0d, p0e, p0f;
}

/** Read-only after construction. */
abstract class LockFreeRowSetFields extends LockFreeRowSetPad0 {
  static final int SPINS = 16;
  static final long MAX_PARK_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

  final int capacity;
  final int mask;
  final int wakeBatch;
  final int wakeCheckMask;
  final Object[][] slots;
  final int timeoutPut;
  final int timeoutGet;

  LockFreeRowSetFields(int maxSize) {
    capacity = Integer.highestOneBit(Math.max(2, maxSize) - 1) << 1;
    mask = capacity - 1;
    slots = new Object[capacity][];
    // A quarter of the capacity measured best for rowset sizes from 8k to 128k rows.
    wakeBatch = Math.max(1, capacity / 4);
    // look at the other side every 64th row, or every row when the wake batch is smaller
    wakeCheckMask = Integer.highestOneBit(Math.min(64, wakeBatch)) - 1;
    timeoutGet =
        Const.toInt(System.getProperty(Const.HOP_ROWSET_GET_TIMEOUT), Const.TIMEOUT_GET_MILLIS);
    timeoutPut =
        Const.toInt(System.getProperty(Const.HOP_ROWSET_PUT_TIMEOUT), Const.TIMEOUT_PUT_MILLIS);
  }
}

@SuppressWarnings("unused")
abstract class LockFreeRowSetPad1 extends LockFreeRowSetFields {
  long p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p1a, p1b, p1c, p1d, p1e, p1f;

  LockFreeRowSetPad1(int maxSize) {
    super(maxSize);
  }
}

/** Written by producers. */
abstract class LockFreeRowSetProducerFields extends LockFreeRowSetPad1 {
  volatile long producerIndex;
  long producerLimit; // cache of consumerIndex + capacity; a stale value only costs a re-read

  LockFreeRowSetProducerFields(int maxSize) {
    super(maxSize);
  }
}

@SuppressWarnings("unused")
abstract class LockFreeRowSetPad2 extends LockFreeRowSetProducerFields {
  long p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p2a, p2b, p2c, p2d, p2e, p2f;

  LockFreeRowSetPad2(int maxSize) {
    super(maxSize);
  }
}

/** Written by the consumer. */
abstract class LockFreeRowSetConsumerFields extends LockFreeRowSetPad2 {
  volatile long consumerIndex;

  LockFreeRowSetConsumerFields(int maxSize) {
    super(maxSize);
  }
}

@SuppressWarnings("unused")
abstract class LockFreeRowSetPad3 extends LockFreeRowSetConsumerFields {
  long p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p3a, p3b, p3c, p3d, p3e, p3f;

  LockFreeRowSetPad3(int maxSize) {
    super(maxSize);
  }
}

/** Written by the consumer when it parks, read by producers for every row. */
abstract class LockFreeRowSetParkedConsumer extends LockFreeRowSetPad3 {
  volatile Thread parkedConsumer;

  LockFreeRowSetParkedConsumer(int maxSize) {
    super(maxSize);
  }
}

@SuppressWarnings("unused")
abstract class LockFreeRowSetPad4 extends LockFreeRowSetParkedConsumer {
  long p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p4a, p4b, p4c, p4d, p4e, p4f;

  LockFreeRowSetPad4(int maxSize) {
    super(maxSize);
  }
}

/** Written by a producer when it parks, read by the consumer for every row. */
abstract class LockFreeRowSetParkedProducer extends LockFreeRowSetPad4 {
  volatile Thread parkedProducer;

  LockFreeRowSetParkedProducer(int maxSize) {
    super(maxSize);
  }
}

@SuppressWarnings("unused")
abstract class LockFreeRowSetPad5 extends LockFreeRowSetParkedProducer {
  long p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p5a, p5b, p5c, p5d, p5e, p5f;

  LockFreeRowSetPad5(int maxSize) {
    super(maxSize);
  }
}
