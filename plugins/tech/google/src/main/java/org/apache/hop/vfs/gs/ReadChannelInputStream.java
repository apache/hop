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

import com.google.cloud.ReadChannel;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ReadChannelInputStream extends InputStream {

  /**
   * Volatile and intentionally NOT guarded by the read monitor: {@link #close()} must be able to
   * tear the channel down from another thread (e.g. a pipeline stop) even while {@code read()} is
   * parked inside {@code channel.read(...)} on a stalled network socket. Closing the channel from
   * the other thread aborts the in-flight read, which is the only way to break a stalled transfer.
   */
  private volatile ReadChannel channel;

  /**
   * Guards the shared {@link #bytes} buffer against concurrent reads, but never held by close().
   */
  private final Object readLock = new Object();

  private final ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);

  public ReadChannelInputStream(ReadChannel channel) {
    this.channel = channel;
  }

  @Override
  public int read() throws IOException {
    byte[] b = new byte[1];
    int r = read(b, 0, 1);
    if (r > 0) {
      return b[0] & 0xFF;
    }
    return r;
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    // Snapshot the channel; close() may null it concurrently and that is allowed - it is how a
    // stalled read gets unblocked.
    ReadChannel ch = channel;
    if (ch == null) {
      return -1;
    }
    synchronized (readLock) {
      int limit = bytes.limit();
      if (len < bytes.remaining()) {
        bytes.limit(len);
      }
      int res;
      try {
        res = ch.read(bytes);
      } catch (IOException e) {
        // If close() ran underneath us the read fails - treat that as a normal end-of-stream
        // rather than propagating a spurious error from the deliberate teardown.
        if (channel == null) {
          return -1;
        }
        throw e;
      }
      if (res < 0) {
        close();
        return -1;
      }
      bytes.flip();
      int read = Math.min(bytes.remaining(), len);
      bytes.get(buf, off, read);
      bytes.compact();
      bytes.limit(limit);
      return read;
    }
  }

  @Override
  public void close() {
    // Deliberately not synchronized on readLock: a thread parked in channel.read(...) holds no
    // lock that this method needs, so close() can always run and break a stalled read.
    ReadChannel ch = channel;
    if (ch != null) {
      channel = null;
      ch.close();
    }
  }
}
