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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.vfs.HopVfs;
import org.jspecify.annotations.NonNull;

/**
 * Bounded in-memory rowset that spills excess rows to temporary files instead of blocking the
 * producer when the memory capacity is full.
 *
 * <p>While the in-memory queue has room and nothing is waiting on disk, behaviour matches {@link
 * BlockingRowSet}. When the queue is full (or unread spilled rows already exist, so FIFO order must
 * be preserved), further {@link #putRow} calls serialize rows to a temp file and return {@code
 * true} without waiting on the consumer. {@link #size()} reports the in-memory size only so
 * existing backpressure heuristics on other hops are unchanged.
 *
 * <p>Temp files are created under a configurable directory (default: {@code java.io.tmpdir}) via
 * {@link HopVfs}.
 */
public class SpillingRowSet extends BaseRowSet implements Comparable<IRowSet>, IRowSet {

  private final int capacity;
  private final String directory;
  private final ArrayDeque<Object[]> memory;
  private final Object lock = new Object();

  /** Rows written to spill that have not yet been read back. */
  private long unreadSpilled;

  private final List<SpillSegment> segments = new ArrayList<>();
  private SpillSegment writeSegment;
  private int readSegmentIndex;
  private long readRowsInSegment;

  private boolean firstSpillLogged;
  private volatile boolean spillIoFailed;

  private final int timeoutGet;

  public SpillingRowSet(int maxSize) {
    this(maxSize, null);
  }

  /**
   * @param maxSize in-memory capacity (same role as {@link BlockingRowSet})
   * @param directory spill directory; null or blank uses {@code java.io.tmpdir}
   */
  public SpillingRowSet(int maxSize, String directory) {
    super();
    if (maxSize < 1) {
      throw new IllegalArgumentException("SpillingRowSet capacity must be >= 1");
    }
    this.capacity = maxSize;
    this.directory =
        (directory == null || directory.isBlank())
            ? System.getProperty("java.io.tmpdir")
            : directory;
    this.memory = new ArrayDeque<>(Math.min(maxSize, 1024));
    this.timeoutGet =
        Const.toInt(System.getProperty(Const.HOP_ROWSET_GET_TIMEOUT), Const.TIMEOUT_GET_MILLIS);
  }

  @Override
  public boolean putRow(IRowMeta rowMeta, Object[] rowData) {
    return putRowWait(rowMeta, rowData, 0, TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean putRowWait(IRowMeta rowMeta, Object[] rowData, long time, TimeUnit tu) {
    if (rowMeta == null || rowData == null || spillIoFailed) {
      return false;
    }
    synchronized (lock) {
      try {
        this.rowMeta = rowMeta;
        if (unreadSpilled == 0 && memory.size() < capacity) {
          memory.addLast(rowData);
        } else {
          spillRow(rowMeta, rowData);
          unreadSpilled++;
        }
        lock.notifyAll();
        return true;
      } catch (Exception e) {
        spillIoFailed = true;
        lock.notifyAll();
        return false;
      }
    }
  }

  @Override
  public Object[] getRow() {
    return getRowWait(timeoutGet, TimeUnit.MILLISECONDS);
  }

  @Override
  public Object[] getRowImmediate() {
    synchronized (lock) {
      return takeAvailable();
    }
  }

  @Override
  public Object[] getRowWait(long timeout, TimeUnit tu) {
    long deadlineNanos = System.nanoTime() + tu.toNanos(timeout);
    synchronized (lock) {
      while (true) {
        Object[] row = takeAvailable();
        if (row != null) {
          return row;
        }
        if (spillIoFailed) {
          return null;
        }
        if (isDone() && memory.isEmpty() && unreadSpilled == 0) {
          return null;
        }
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0L) {
          return null;
        }
        try {
          long ms = remaining / 1_000_000L;
          int ns = (int) (remaining % 1_000_000L);
          lock.wait(ms, ns);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
      }
    }
  }

  /**
   * Reported occupancy for {@code BaseTransform} flow-control heuristics.
   *
   * <p>{@link org.apache.hop.pipeline.transform.BaseTransform} sleeps with {@code Thread.sleep(0,
   * 1)} when {@code size() >= 99%} of the rowset capacity (producer) or {@code size() <= 1%}
   * (consumer). On many JVMs/OSes that "1 ns" sleep rounds up to about 1 ms. If we reported a full
   * memory buffer while spilling (put is non-blocking), every put would sleep ~1 ms — near-zero
   * CPU/disk and catastrophic throughput. Likewise, reporting {@code 0} while rows still sit only
   * on disk would make the consumer sleep on every get.
   *
   * <p>So when there is pending work we advertise a mid-level size (neither "full" nor "empty").
   * When idle we report {@code 0}. Use {@link #getUnreadSpilled()} / memory internals for tests.
   */
  @Override
  public int size() {
    synchronized (lock) {
      long pending = memory.size() + unreadSpilled;
      if (pending <= 0L) {
        return 0;
      }
      // Stay strictly below upperBufferBoundary (0.99 * capacity) and above lower (0.01 * capacity)
      // for typical capacities so BaseTransform does not inject per-row sleeps.
      int mid = Math.max(1, capacity / 2);
      if (unreadSpilled == 0 && memory.size() < mid) {
        return memory.size();
      }
      return mid;
    }
  }

  @Override
  public void setDone() {
    super.setDone();
    synchronized (lock) {
      closeWriteSegmentQuietly();
      lock.notifyAll();
    }
  }

  @Override
  public void clear() {
    synchronized (lock) {
      memory.clear();
      closeWriteSegmentQuietly();
      closeReadQuietly();
      deleteAllSegments();
      segments.clear();
      writeSegment = null;
      readSegmentIndex = 0;
      readRowsInSegment = 0;
      unreadSpilled = 0;
      spillIoFailed = false;
      firstSpillLogged = false;
      done.set(false);
    }
  }

  @Override
  public boolean isBlocking() {
    return true;
  }

  /** For tests: rows still only on disk. */
  long getUnreadSpilled() {
    synchronized (lock) {
      return unreadSpilled;
    }
  }

  boolean hasSpilled() {
    synchronized (lock) {
      return firstSpillLogged || !segments.isEmpty();
    }
  }

  private Object[] takeAvailable() {
    Object[] row = memory.pollFirst();
    if (row != null) {
      return row;
    }
    if (unreadSpilled > 0) {
      try {
        row = readSpilledRow();
        if (row != null) {
          unreadSpilled--;
        }
        return row;
      } catch (Exception e) {
        spillIoFailed = true;
        return null;
      }
    }
    return null;
  }

  private void spillRow(IRowMeta meta, Object[] row) throws HopFileException, IOException {
    if (writeSegment == null || writeSegment.closedForWrite) {
      openNewWriteSegment();
    }
    meta.writeData(writeSegment.output, row);
    writeSegment.rowCount++;
    firstSpillLogged = true;
  }

  private void openNewWriteSegment() throws HopFileException, IOException {
    closeWriteSegmentQuietly();
    FileObject file = HopVfs.createTempFile("spilling-rowset", ".tmp", directory);
    OutputStream os = HopVfs.getOutputStream(file, false);
    DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(os, 65536));
    writeSegment = new SpillSegment(file, dos);
    segments.add(writeSegment);
  }

  private Object[] readSpilledRow() throws HopFileException, IOException {
    closeWriteSegmentQuietly();

    while (readSegmentIndex < segments.size()) {
      SpillSegment segment = segments.get(readSegmentIndex);
      if (segment.input == null) {
        InputStream is = HopVfs.getInputStream(segment.file);
        segment.input = new DataInputStream(new BufferedInputStream(is, 65536));
      }
      if (readRowsInSegment < segment.rowCount) {
        Object[] row = rowMeta.readData(segment.input);
        readRowsInSegment++;
        return row;
      }
      // Segment exhausted
      closeSegmentInput(segment);
      deleteSegmentFile(segment);
      readSegmentIndex++;
      readRowsInSegment = 0;
    }
    return null;
  }

  private void closeWriteSegmentQuietly() {
    if (writeSegment != null && !writeSegment.closedForWrite) {
      try {
        writeSegment.output.flush();
        writeSegment.output.close();
      } catch (IOException e) {
        // ignore on close
      }
      writeSegment.closedForWrite = true;
      writeSegment.output = null;
    }
  }

  private void closeReadQuietly() {
    for (SpillSegment segment : segments) {
      closeSegmentInput(segment);
    }
  }

  private static void closeSegmentInput(SpillSegment segment) {
    if (segment.input != null) {
      try {
        segment.input.close();
      } catch (IOException e) {
        // ignore
      }
      segment.input = null;
    }
  }

  private void deleteAllSegments() {
    for (SpillSegment segment : segments) {
      deleteSegmentFile(segment);
    }
  }

  private static void deleteSegmentFile(SpillSegment segment) {
    if (segment.file != null) {
      try {
        if (segment.file.exists()) {
          segment.file.delete();
        }
      } catch (Exception e) {
        // best-effort cleanup
      }
      segment.file = null;
    }
  }

  @Override
  public int compareTo(@NonNull IRowSet rowSet) {
    return super.compareTo(rowSet);
  }

  private static final class SpillSegment {
    private FileObject file;
    private DataOutputStream output;
    private DataInputStream input;
    private long rowCount;
    private boolean closedForWrite;

    private SpillSegment(FileObject file, DataOutputStream output) {
      this.file = file;
      this.output = output;
    }
  }
}
