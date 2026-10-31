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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Rewindable hop buffer for a debug session.
 *
 * <p>Rows stay in the buffer after they are read. {@link #getRow()} returns the row under the
 * cursor and advances it. {@link #setCursor(int)} moves the cursor without dropping rows. {@code
 * size()} is the number of rows the consumer has not taken yet. Past {@code memoryThreshold},
 * further rows spill to a temp file through VFS and remain readable by index.
 */
public class CursorRowSet extends BaseRowSet implements Comparable<IRowSet>, IRowSet {

  private final int memoryThreshold;
  private final String directory;
  private final Object lock = new Object();
  private final List<Slot> slots = new ArrayList<>();
  private final List<Object[]> trialRows = new ArrayList<>();

  private int cursor;
  private int mark = -1;
  private int inMemory;
  private boolean trialMode;
  private boolean spillIoFailed;

  private FileObject spillFile;
  private DataOutputStream spillOut;
  private long spillLength;

  private final int timeoutGet;
  private volatile RowEnteredListener rowEnteredListener;

  /** Notified after a row is stored on this hop, outside the rowset lock. */
  public interface RowEnteredListener {
    void rowEntered(CursorRowSet rowSet, IRowMeta rowMeta, Object[] row);
  }

  public void setRowEnteredListener(RowEnteredListener listener) {
    this.rowEnteredListener = listener;
  }

  /**
   * @param memoryThreshold maximum rows kept as live arrays. Further rows spill to disk. Must be at
   *     least 1.
   * @param directory spill directory. Null or blank uses {@code java.io.tmpdir}.
   */
  public CursorRowSet(int memoryThreshold, String directory) {
    super();
    if (memoryThreshold < 1) {
      throw new IllegalArgumentException("CursorRowSet memory threshold must be >= 1");
    }
    this.memoryThreshold = memoryThreshold;
    this.directory =
        (directory == null || directory.isBlank())
            ? System.getProperty("java.io.tmpdir")
            : directory;
    this.timeoutGet =
        Const.toInt(System.getProperty(Const.HOP_ROWSET_GET_TIMEOUT), Const.TIMEOUT_GET_MILLIS);
  }

  public CursorRowSet(int memoryThreshold) {
    this(memoryThreshold, null);
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
    Object[] stored;
    boolean committed = false;
    synchronized (lock) {
      try {
        this.rowMeta = rowMeta;
        stored = rowMeta.cloneRow(rowData);
        if (trialMode) {
          trialRows.add(stored);
        } else if (inMemory < memoryThreshold) {
          slots.add(new Slot(stored));
          inMemory++;
          committed = true;
        } else {
          slots.add(spill(rowMeta, stored));
          committed = true;
        }
        lock.notifyAll();
      } catch (Exception e) {
        spillIoFailed = true;
        lock.notifyAll();
        return false;
      }
    }
    RowEnteredListener listener = rowEnteredListener;
    if (committed && listener != null) {
      try {
        listener.rowEntered(this, rowMeta, stored);
      } catch (RuntimeException e) {
        // A pause condition must not fail the row that was already stored.
      }
    }
    return true;
  }

  @Override
  public Object[] getRow() {
    return getRowWait(timeoutGet, TimeUnit.MILLISECONDS);
  }

  @Override
  public Object[] getRowImmediate() {
    synchronized (lock) {
      return takeAtCursor();
    }
  }

  @Override
  public Object[] getRowWait(long timeout, TimeUnit tu) {
    long deadlineNanos = System.nanoTime() + tu.toNanos(Math.max(0L, timeout));
    synchronized (lock) {
      while (true) {
        Object[] row = takeAtCursor();
        if (row != null || spillIoFailed) {
          return row;
        }
        if (isDone() && cursor >= slots.size()) {
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
   * Rows the consumer has not taken. This is the occupancy the rest of the engine uses for
   * backpressure.
   */
  @Override
  public int size() {
    synchronized (lock) {
      long unread = (long) slots.size() - cursor;
      if (unread <= 0L) {
        return 0;
      }
      if (unread > Integer.MAX_VALUE) {
        return Integer.MAX_VALUE;
      }
      return (int) unread;
    }
  }

  /** Every retained row, including rows already read and rows on disk. */
  public long storedCount() {
    synchronized (lock) {
      return slots.size();
    }
  }

  public int getCursor() {
    synchronized (lock) {
      return cursor;
    }
  }

  /**
   * Moves the read cursor. {@code index == storedCount()} means the next read waits for a new row.
   * Rows outside the cursor stay in the buffer.
   */
  public void setCursor(int index) {
    synchronized (lock) {
      if (index < 0 || index > slots.size()) {
        throw new IllegalArgumentException("Cursor " + index + " is outside 0.." + slots.size());
      }
      cursor = index;
      lock.notifyAll();
    }
  }

  /** Remembers the cursor so {@link #resetToMark()} can restore it. */
  public void mark() {
    synchronized (lock) {
      mark = cursor;
    }
  }

  /** Restores the cursor saved by {@link #mark()}. Does nothing when no mark was taken. */
  public void resetToMark() {
    synchronized (lock) {
      if (mark >= 0 && mark <= slots.size()) {
        cursor = mark;
        lock.notifyAll();
      }
    }
  }

  /**
   * While set, {@link #putRow} appends to the trial buffer instead of the committed rows. The
   * cursor and {@link #size()} stay unchanged.
   */
  public void setTrialMode(boolean trialMode) {
    synchronized (lock) {
      this.trialMode = trialMode;
    }
  }

  public boolean isTrialMode() {
    synchronized (lock) {
      return trialMode;
    }
  }

  public int getTrialCount() {
    synchronized (lock) {
      return trialRows.size();
    }
  }

  public List<Object[]> getTrialRows() {
    synchronized (lock) {
      return new ArrayList<>(trialRows);
    }
  }

  public void clearTrialRows() {
    synchronized (lock) {
      trialRows.clear();
    }
  }

  /**
   * A copy of the stored row at {@code index}. The cursor does not move. Index 0 is the oldest
   * retained row.
   */
  public Object[] copyStoredRow(int index) {
    synchronized (lock) {
      if (index < 0 || index >= slots.size() || spillIoFailed || rowMeta == null) {
        return null;
      }
      try {
        Object[] raw = readSlot(slots.get(index));
        return raw == null ? null : rowMeta.cloneRow(raw);
      } catch (Exception e) {
        spillIoFailed = true;
        return null;
      }
    }
  }

  /** Rows currently held as live arrays. Spilled rows are not included. */
  public int getInMemoryCount() {
    synchronized (lock) {
      return inMemory;
    }
  }

  public boolean hasSpilled() {
    synchronized (lock) {
      return spillFile != null;
    }
  }

  @Override
  public void setDone() {
    super.setDone();
    synchronized (lock) {
      lock.notifyAll();
    }
  }

  @Override
  public void clear() {
    synchronized (lock) {
      slots.clear();
      trialRows.clear();
      cursor = 0;
      mark = -1;
      inMemory = 0;
      trialMode = false;
      spillIoFailed = false;
      closeSpillQuietly();
      deleteSpillQuietly();
      done.set(false);
      lock.notifyAll();
    }
  }

  @Override
  public boolean isBlocking() {
    return false;
  }

  private Object[] takeAtCursor() {
    if (cursor >= slots.size()) {
      return null;
    }
    Object[] raw;
    try {
      raw = readSlot(slots.get(cursor));
    } catch (Exception e) {
      spillIoFailed = true;
      return null;
    }
    cursor++;
    if (rowMeta == null || raw == null) {
      return raw;
    }
    try {
      return rowMeta.cloneRow(raw);
    } catch (HopValueException e) {
      throw new HopRuntimeException("Unable to copy a debug buffer row", e);
    }
  }

  private Slot spill(IRowMeta meta, Object[] row) throws HopFileException, IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    try (DataOutputStream dataOut = new DataOutputStream(bytesOut)) {
      meta.writeData(dataOut, row);
    }
    byte[] bytes = bytesOut.toByteArray();
    if (spillOut == null) {
      openSpill();
    }
    long offset = spillLength;
    spillOut.writeInt(bytes.length);
    spillOut.write(bytes);
    spillOut.flush();
    spillLength += 4L + bytes.length;
    return new Slot(offset, bytes.length);
  }

  private void openSpill() throws HopFileException, IOException {
    spillFile = HopVfs.createTempFile("cursor-rowset", ".bin", directory);
    spillOut = new DataOutputStream(HopVfs.getOutputStream(spillFile, false));
    spillLength = 0L;
  }

  private Object[] readSlot(Slot slot) throws HopFileException, IOException {
    if (slot.memory != null) {
      return slot.memory;
    }
    return readSpilled(slot);
  }

  private Object[] readSpilled(Slot slot) throws HopFileException, IOException {
    if (spillOut != null) {
      spillOut.flush();
    }
    try (RandomAccessContent content =
        spillFile.getContent().getRandomAccessContent(RandomAccessMode.READ)) {
      content.seek(slot.offset);
      int length = content.readInt();
      if (length != slot.length) {
        throw new IOException(
            "Spilled row length " + length + " does not match the stored length " + slot.length);
      }
      byte[] bytes = new byte[length];
      content.readFully(bytes);
      return rowMeta.readData(new DataInputStream(new ByteArrayInputStream(bytes)));
    }
  }

  private void closeSpillQuietly() {
    if (spillOut != null) {
      try {
        spillOut.close();
      } catch (IOException e) {
        // The buffer is being discarded.
      }
      spillOut = null;
    }
  }

  private void deleteSpillQuietly() {
    if (spillFile != null) {
      try {
        if (spillFile.exists()) {
          spillFile.delete();
        }
      } catch (Exception e) {
        // Best effort. The next session creates its own file.
      }
      spillFile = null;
      spillLength = 0L;
    }
  }

  private static final class Slot {
    private final Object[] memory;
    private final long offset;
    private final int length;

    private Slot(Object[] memory) {
      this.memory = memory;
      this.offset = -1L;
      this.length = 0;
    }

    private Slot(long offset, int length) {
      this.memory = null;
      this.offset = offset;
      this.length = length;
    }
  }
}
