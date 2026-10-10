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

package org.apache.hop.core.vfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.logging.LogChannel;

/**
 * Reads a file from an offset in blocks fetched ahead, several at a time, each by its own ranged
 * request. On object storage one connection is often limited to a few MB/s however fast the network
 * is, and a plain stream fetches its next block only once the reader asks for it. Keeping several
 * blocks in flight gives one reader several connections, and the reader parses one block while the
 * next ones download.
 *
 * <p>Blocks are fetched ahead up to a limit, typically the end of the part of the file the reader
 * needs. Past that limit a block is fetched only when the reader gets there, so a reader that stops
 * shortly after the limit doesn't download blocks it never reads.
 *
 * <p>A block is read with {@link HopVfs#getInputStream(FileObject, long, long)}: one request for
 * exactly its bytes where the storage supports it.
 *
 * <p>A block that fails to download is tried again, up to {@link #MAX_ATTEMPTS} times in all: a
 * block is a bounded range, so reading it again gives the same bytes. When a block still fails,
 * that error and every later read fail: the stream never skips a block and carries on after it.
 *
 * <p>At most {@code blocks} blocks of {@code blockSize} bytes are held in memory per stream.
 */
public final class ReadAheadInputStream extends InputStream {

  /** How many times a block is downloaded before its error is the stream's. */
  public static final int MAX_ATTEMPTS = 3;

  private static final long RETRY_DELAY_MILLIS = 1000;

  private static final AtomicInteger THREAD_NUMBER = new AtomicInteger();

  private static final ExecutorService FETCHERS =
      Executors.newCachedThreadPool(
          task -> {
            Thread thread = new Thread(task, "hop-read-ahead-" + THREAD_NUMBER.incrementAndGet());
            thread.setDaemon(true);
            return thread;
          });

  private final FileObject file;
  private final long size;
  private final long readAheadEnd;
  private final int blockSize;
  private final int blocks;
  private final long retryDelayMillis;

  /** The blocks fetched or being fetched, in file order. */
  private final Deque<Future<byte[]>> inFlight = new ArrayDeque<>();

  /** The offset of the next block to fetch. */
  private long nextBlock;

  private byte[] current = new byte[0];
  private int currentPosition;
  private boolean closed;

  /** The error of a block that couldn't be read: every read after it fails with it too. */
  private IOException failure;

  /**
   * @param file the file to read, on storage that can start reading at an offset
   * @param size the size of the file
   * @param offset the first byte to read
   * @param readAheadEnd how far to fetch ahead: blocks starting at or after it are fetched only
   *     when the reader reaches them
   * @param blockSize the bytes per ranged request
   * @param blocks how many blocks to keep in flight, at least 1
   */
  public ReadAheadInputStream(
      FileObject file, long size, long offset, long readAheadEnd, int blockSize, int blocks) {
    this(file, size, offset, readAheadEnd, blockSize, blocks, RETRY_DELAY_MILLIS);
  }

  /**
   * As above, waiting {@code retryDelayMillis} times the attempt number before trying a failed
   * block again.
   */
  ReadAheadInputStream(
      FileObject file,
      long size,
      long offset,
      long readAheadEnd,
      int blockSize,
      int blocks,
      long retryDelayMillis) {
    if (blockSize <= 0 || blocks <= 0) {
      throw new IllegalArgumentException("The block size and number of blocks must be positive");
    }
    this.file = file;
    this.size = size;
    this.readAheadEnd = Math.min(readAheadEnd, size);
    this.blockSize = blockSize;
    this.blocks = blocks;
    this.retryDelayMillis = retryDelayMillis;
    this.nextBlock = Math.max(0, offset);
    fetchAhead();
  }

  /** Starts fetching blocks until enough are in flight, up to the read-ahead end. */
  private void fetchAhead() {
    while (inFlight.size() < blocks && nextBlock < readAheadEnd) {
      fetchNext();
    }
  }

  private void fetchNext() {
    long start = nextBlock;
    int length = (int) Math.min(blockSize, size - start);
    nextBlock += length;
    inFlight.add(FETCHERS.submit(() -> fetch(start, length)));
  }

  /** Downloads a block, trying again after an error, as long as the stream is not closed. */
  private byte[] fetch(long start, int length) throws IOException {
    for (int attempt = 1; ; attempt++) {
      try {
        return fetchOnce(start, length);
      } catch (FileChangedException | InterruptedIOException e) {
        throw e;
      } catch (IOException | RuntimeException e) {
        if (attempt >= MAX_ATTEMPTS || Thread.currentThread().isInterrupted()) {
          throw new IOException(
              "Unable to read bytes "
                  + start
                  + " to "
                  + (start + length)
                  + " of "
                  + file.getName().getFriendlyURI()
                  + " after "
                  + attempt
                  + " attempts: "
                  + e.getMessage(),
              e);
        }
        logRetry(
            "Reading bytes "
                + start
                + " to "
                + (start + length)
                + " of "
                + file.getName().getFriendlyURI()
                + " failed ("
                + e.getMessage()
                + "), trying again ("
                + (attempt + 1)
                + " of "
                + MAX_ATTEMPTS
                + ")");
        try {
          Thread.sleep(retryDelayMillis * attempt);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("Interrupted while reading " + file.getName());
        }
      }
    }
  }

  /** Logs a retry; a log that can't be written must not change how the read goes. */
  private static void logRetry(String message) {
    try {
      LogChannel.GENERAL.logBasic(message);
    } catch (RuntimeException e) {
      // No log store, for example: the retry goes ahead regardless
    }
  }

  private byte[] fetchOnce(long start, int length) throws IOException {
    try (InputStream in = HopVfs.getInputStream(file, start, length)) {
      byte[] block = in.readNBytes(length);
      if (block.length < length) {
        throw new FileChangedException(
            "The file "
                + file.getName().getFriendlyURI()
                + " ended at byte "
                + (start + block.length)
                + ", before its size of "
                + size
                + " bytes: it changed while it was read");
      }
      return block;
    }
  }

  /** The file is shorter than its size said: reading it again won't help. */
  private static final class FileChangedException extends IOException {
    FileChangedException(String message) {
      super(message);
    }
  }

  /** Makes the next block current. False at the end of the file. */
  private boolean nextBlock() throws IOException {
    if (inFlight.isEmpty()) {
      if (nextBlock >= size) {
        return false;
      }
      // Past the read-ahead end: one block at a time, as the reader gets there
      fetchNext();
    }
    Future<byte[]> next = inFlight.poll();
    try {
      current = next.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw fail(new InterruptedIOException("Interrupted while reading " + file.getName()));
    } catch (CancellationException e) {
      throw fail(
          new IOException("Reading " + file.getName().getFriendlyURI() + " was cancelled", e));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException ioException) {
        throw fail(ioException);
      }
      throw fail(new IOException("Unable to read " + file.getName().getFriendlyURI(), cause));
    }
    currentPosition = 0;
    fetchAhead();
    return true;
  }

  /**
   * Makes an error the stream's: the downloads still running stop, and every later read fails with
   * it. Reading on would skip the bytes of the failed block.
   */
  private IOException fail(IOException error) {
    failure = error;
    for (Future<byte[]> fetch : inFlight) {
      fetch.cancel(true);
    }
    inFlight.clear();
    current = new byte[0];
    currentPosition = 0;
    return error;
  }

  @Override
  public int read() throws IOException {
    byte[] one = new byte[1];
    return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
  }

  @Override
  public int read(byte[] buffer, int offset, int length) throws IOException {
    if (closed) {
      throw new IOException("The stream is closed");
    }
    if (failure != null) {
      throw new IOException(failure.getMessage(), failure);
    }
    if (length == 0) {
      return 0;
    }
    while (currentPosition >= current.length) {
      if (!nextBlock()) {
        return -1;
      }
    }
    int n = Math.min(length, current.length - currentPosition);
    System.arraycopy(current, currentPosition, buffer, offset, n);
    currentPosition += n;
    return n;
  }

  @Override
  public int available() {
    return current.length - currentPosition;
  }

  /** Stops the fetches still running and drops the blocks fetched ahead. */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    for (Future<byte[]> fetch : inFlight) {
      fetch.cancel(true);
    }
    inFlight.clear();
    current = new byte[0];
  }
}
