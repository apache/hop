/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.vfs.gs;

import com.google.cloud.WriteChannel;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

public class WriteChannelOutputStream extends OutputStream {

  /**
   * A {@link WriteChannel} is expected to either consume bytes or throw. If it instead repeatedly
   * accepts zero bytes the original {@code while (hasRemaining())} loop spun forever with no
   * exception and no way to interrupt it. Bail out after this many consecutive no-progress writes
   * so a wedged transfer fails fast instead of hanging the pipeline.
   */
  private static final int MAX_NO_PROGRESS_WRITES = 1024;

  /**
   * Volatile and intentionally NOT guarded by the write monitor: {@link #close()} must be able to
   * tear the channel down from another thread (e.g. a pipeline stop) even while {@code write(...)}
   * is parked inside {@code channel.write(...)} on a stalled network socket.
   */
  private volatile WriteChannel channel;

  /**
   * Guards the shared {@link #bytes} buffer against concurrent writes, but never held by close().
   */
  private final Object writeLock = new Object();

  private final ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);

  /** The object being written, named in the retry log and watched for stalls. */
  private final GoogleStorageStallWatchdog.Transfer transfer;

  public WriteChannelOutputStream(WriteChannel channel) {
    this(channel, GoogleStorageStallWatchdog.Transfer.untracked(null));
  }

  /**
   * @param channel the channel to write to
   * @param transfer the object being written
   */
  WriteChannelOutputStream(WriteChannel channel, GoogleStorageStallWatchdog.Transfer transfer) {
    this.channel = channel;
    this.transfer = transfer;
  }

  @Override
  public void write(int b) throws IOException {
    write(new byte[] {(byte) b}, 0, 1);
  }

  @Override
  public void write(byte[] buf, int off, int len) throws IOException {
    if (channel == null) {
      throw new ClosedChannelException();
    }
    synchronized (writeLock) {
      try (GoogleStorageObjectContext.Scope ignored =
          GoogleStorageObjectContext.enter(transfer.uri())) {
        int count = 0;
        while (count < len) {
          int c = Math.min(len - count, bytes.remaining());
          bytes.put(buf, off + count, c);
          bytes.flip();
          int noProgress = 0;
          while (bytes.hasRemaining()) {
            WriteChannel ch = channel;
            if (ch == null) {
              // close() ran on another thread (e.g. pipeline stop) - stop writing.
              throw new ClosedChannelException();
            }
            int written = 0;
            transfer.begin();
            try {
              written = ch.write(bytes);
            } finally {
              transfer.end(written);
            }
            if (written <= 0) {
              if (++noProgress > MAX_NO_PROGRESS_WRITES) {
                throw new IOException(
                    "Google Storage write channel made no progress after "
                        + MAX_NO_PROGRESS_WRITES
                        + " attempts; aborting to avoid an unbounded loop");
              }
            } else {
              noProgress = 0;
            }
          }
          bytes.compact();
          count += c;
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    // Deliberately not synchronized on writeLock: a thread parked in channel.write(...) holds no
    // lock that this method needs, so close() can always run and break a stalled write.
    WriteChannel ch = channel;
    if (ch != null) {
      channel = null;
      // The last part of the upload happens here.
      transfer.begin();
      try (GoogleStorageObjectContext.Scope ignored =
          GoogleStorageObjectContext.enter(transfer.uri())) {
        ch.close();
      } finally {
        transfer.end(0);
      }
    }
  }
}
