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
 *   <li>The slot array is rounded up to a power of two, so an index maps to a slot with a mask. The
 *       row set still holds at most the configured number of rows.
 *   <li>Producers claim a slot with one compare-and-set on the producer index. They keep a cached
 *       limit (consumer index + size) and only read the consumer index again when they reach it, so
 *       a producer does not touch the consumer's cache line for every row.
 *   <li>A row set is read by exactly one transform thread, so the consumer moves its index with a
 *       plain release store. (Several producers are allowed: some transforms call putRow from
 *       worker threads, e.g. LanguageModelChat.)
 *   <li>The producer fields, the consumer fields and the parked-thread fields each sit on their own
 *       cache line (padding through the superclasses below, which the JVM lays out first).
 * </ul>
 *
 * <p>When the ring is full (or empty) the thread spins briefly, then parks. The other side only
 * unparks it once a batch of rows (or free slots) is ready, a quarter of the size, or when the row
 * set is done, claiming the parked thread so it is woken once. That lets a fast consumer process
 * runs of rows instead of waking up for every row, which is what the per-row size() check and
 * Thread.sleep in BaseTransform approximate for the lock-based row sets.
 *
 * <p>A thread parks for {@link LockFreeRowSetFields#MAX_PARK_NANOS} at first. While nothing changes
 * it parks longer, up to {@link LockFreeRowSetFields#MAX_IDLE_PARK_NANOS}, so an idle transform
 * costs little CPU. A reader in such a long park is woken by the next row instead of a batch, so a
 * trickle of rows is not delayed either.
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
   * @param maxSize the number of rows the buffer holds
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
        limit = consumerIndex + maxRows;
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
   * Called by a producer after it added the row at {@code index}: wake a consumer in a short park
   * once a batch is queued, and one in a long park at once. Every row reads whether a consumer is
   * parked, a cache line the consumer only writes when it parks. Only every {@code wakeCheckMask +
   * 1}th row counts the queued rows for a short park, so that wake-up is late by at most that many
   * rows.
   */
  private void rowAdded(long index) {
    Thread consumer = parkedConsumer;
    if (consumer == null) {
      return;
    }
    boolean anyRow = wakeOnAnyRow; // read after parkedConsumer, which the consumer writes last
    if ((index & wakeCheckMask) != 0 && !anyRow) {
      return;
    }
    // claim the parked thread first, so it is unparked once and not again for every row added
    // before it gets to run
    if ((anyRow || producerIndex - consumerIndex >= wakeBatch)
        && PARKED_CONSUMER.compareAndSet(this, consumer, null)) {
      LockSupport.unpark(consumer);
    }
  }

  /** Called by the consumer after it took a row: wake a parked producer once a batch is free. */
  private void rowTaken() {
    if ((consumerIndex & wakeCheckMask) != 0) {
      return;
    }
    // poll() moves the consumer index with a release store, which the read below may overtake.
    // The fence orders them, so a producer that parks after this check sees the slots freed so
    // far, and a producer that parked before it is seen here and woken once a batch is free.
    VarHandle.fullFence();
    Thread producer = parkedProducer;
    if (producer != null
        && maxRows - (producerIndex - consumerIndex) >= wakeBatch
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
    long parkNanos = producerParkNanos;
    while (true) {
      // Only one producer is announced and woken by the consumer. With several producers the
      // others park briefly and check again by themselves.
      boolean announced =
          parkedProducer == current || PARKED_PRODUCER.compareAndSet(this, null, current);
      index = offer(rowData); // re-check after announcing ourselves, so a wake-up is not lost
      if (index >= 0) {
        PARKED_PRODUCER.compareAndSet(this, current, null);
        if (producerParkNanos != MAX_PARK_NANOS) {
          producerParkNanos = MAX_PARK_NANOS; // the buffer moves again
        }
        rowAdded(index);
        return true;
      }
      long remaining = timeoutNanos - (System.nanoTime() - start);
      if (remaining <= 0 || Thread.interrupted()) {
        PARKED_PRODUCER.compareAndSet(this, current, null);
        if (announced) {
          producerParkNanos = parkNanos; // the next call goes on backing off
        }
        return false;
      }
      if (parkTimedOut(this, Math.min(remaining, announced ? parkNanos : MAX_PARK_NANOS))
          && announced) {
        parkNanos = Math.min(parkNanos * 2, MAX_IDLE_PARK_NANOS);
      }
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
    long parkNanos = consumerParkNanos;
    boolean deep = false;
    while (true) {
      // after a while without rows park longer, and then let the next row wake us
      if (!deep && parkNanos > MAX_PARK_NANOS) {
        deep = true;
        wakeOnAnyRow = true;
      }
      parkedConsumer = current;
      row = poll();
      if (row != null) {
        stopWaiting(deep);
        if (consumerParkNanos != MAX_PARK_NANOS) {
          consumerParkNanos = MAX_PARK_NANOS; // rows flow again
        }
        rowTaken();
        return row;
      }
      long remaining = timeoutNanos - (System.nanoTime() - start);
      if (remaining <= 0 || Thread.interrupted()) {
        stopWaiting(deep);
        consumerParkNanos = parkNanos; // the next call goes on backing off
        return null;
      }
      if (parkTimedOut(this, Math.min(remaining, parkNanos))) {
        parkNanos = Math.min(parkNanos * 2, MAX_IDLE_PARK_NANOS);
      }
    }
  }

  private void stopWaiting(boolean deep) {
    parkedConsumer = null;
    if (deep) {
      wakeOnAnyRow = false;
    }
  }

  /**
   * Waits, as the reader of all these row sets, until one of them has rows or is done: spins
   * briefly, then parks until a {@link LockFreeRowSet} among them has a batch of rows ready or is
   * done, at most {@code maxNanos}. A transform with several inputs cannot wait on one of them with
   * {@link #getRowWait(long, TimeUnit)}: rows arriving on another one would not wake it.
   *
   * <p>A park longer than 1 ms is only used when every input is a {@link LockFreeRowSet}. The next
   * row on any of them then wakes the reader, not a batch. Other row sets cannot wake it, so they
   * are checked again every 1 ms.
   *
   * <p>An interrupt ends the wait and is cleared, as in {@link #getRowWait(long, TimeUnit)}:
   * otherwise every later park would return at once and the caller would spin.
   *
   * @param rowSets the input row sets of the calling transform
   * @param maxNanos the longest time to park; row sets of another type are only checked again then
   * @return true if the park ran its full time without a wake-up: the caller may wait longer next
   *     time. False when it returned early, also on a wake-up left over from earlier, so a busy
   *     reader keeps its short parks and its batches.
   */
  public static boolean awaitAny(List<IRowSet> rowSets, long maxNanos) {
    for (int i = 0; i < SPINS; i++) {
      if (anyReady(rowSets)) {
        return false;
      }
      Thread.onSpinWait();
    }
    if (Thread.interrupted()) {
      return false;
    }
    Thread current = Thread.currentThread();
    // a long park only when every input can wake us; other row sets are polled every 1 ms
    boolean deep = maxNanos > MAX_PARK_NANOS;
    for (IRowSet rowSet : rowSets) {
      if (!(rowSet instanceof LockFreeRowSet)) {
        deep = false;
        maxNanos = MAX_PARK_NANOS;
        break;
      }
    }
    for (IRowSet rowSet : rowSets) {
      if (rowSet instanceof LockFreeRowSet lockFree) {
        if (deep) {
          lockFree.wakeOnAnyRow = true;
        }
        lockFree.parkedConsumer = current;
      }
    }
    // check again after announcing ourselves on every input, so a wake-up is not lost
    boolean timedOut = false;
    if (!anyReady(rowSets)) {
      timedOut = parkTimedOut(rowSets, maxNanos);
    }
    for (IRowSet rowSet : rowSets) {
      if (rowSet instanceof LockFreeRowSet lockFree) {
        PARKED_CONSUMER.compareAndSet(lockFree, current, null);
        if (deep) {
          lockFree.wakeOnAnyRow = false;
        }
      }
    }
    return timedOut;
  }

  /**
   * Parks at most {@code nanos}.
   *
   * @return true if the park ran its full time: nothing woke the thread, not even a wake-up left
   *     over from earlier
   */
  private static boolean parkTimedOut(Object blocker, long nanos) {
    long start = System.nanoTime();
    LockSupport.parkNanos(blocker, nanos);
    return System.nanoTime() - start >= nanos;
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

  /**
   * The exact number of rows in the row set at one moment during the call. The two indexes are read
   * separately, so the consumer index is read again: only when it did not move in between do the
   * two indexes belong to the same moment. (A row a producer has claimed but is still writing
   * counts as in the row set.)
   */
  @Override
  public int size() {
    long after = consumerIndex;
    while (true) {
      long before = after;
      long produced = producerIndex;
      after = consumerIndex;
      if (before == after) {
        return (int) (produced - after);
      }
    }
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

  /** The longest a thread parks once nothing has changed for a while. */
  static final long MAX_IDLE_PARK_NANOS = TimeUnit.MILLISECONDS.toNanos(20);

  /** The number of rows the row set holds: the configured size, not the rounded-up capacity. */
  final int maxRows;

  final int capacity;
  final int mask;
  final int wakeBatch;
  final int wakeCheckMask;
  final Object[][] slots;
  final int timeoutPut;
  final int timeoutGet;

  LockFreeRowSetFields(int maxSize) {
    maxRows = Math.max(1, maxSize);
    capacity = Integer.highestOneBit(Math.max(2, maxSize) - 1) << 1;
    mask = capacity - 1;
    slots = new Object[capacity][];
    // A quarter of the size measured best for rowset sizes from 8k to 128k rows.
    wakeBatch = Math.max(1, maxRows / 4);
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
  long producerLimit; // cache of consumerIndex + maxRows; a stale value only costs a re-read

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

  /** Set during a long park: the next row wakes the consumer, not a batch. */
  volatile boolean wakeOnAnyRow;

  /** How long getRowWait parks next; only the consumer uses it. */
  long consumerParkNanos = MAX_PARK_NANOS;

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

/** Written by a producer when it parks, read by the consumer on every wake check. */
abstract class LockFreeRowSetParkedProducer extends LockFreeRowSetPad4 {
  volatile Thread parkedProducer;

  /** How long the announced producer parks next. Only a hint, so a plain field is enough. */
  long producerParkNanos = MAX_PARK_NANOS;

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
