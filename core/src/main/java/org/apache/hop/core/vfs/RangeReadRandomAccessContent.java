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
 */

package org.apache.hop.core.vfs;

import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.vfs2.provider.AbstractRandomAccessStreamContent;
import org.apache.commons.vfs2.util.RandomAccessMode;

/**
 * Read-only random access for providers that can open a stream at a byte offset, such as object
 * stores with ranged GET requests. A provider only says how to open that stream; this class keeps
 * the position, opens a stream lazily at the position on the first read, and closes it again when
 * the position is moved with {@link #seek(long)}.
 *
 * <p>Seeking is cheap: nothing is fetched until the next read. Reading forward after a seek costs
 * one new request, so readers that jump around a lot should read in large pieces.
 */
public abstract class RangeReadRandomAccessContent extends AbstractRandomAccessStreamContent {

  private final long length;
  private long filePointer;
  private DataInputStream current;
  private PositionTrackingInputStream tracker;

  /**
   * @param length the size of the file in bytes
   */
  protected RangeReadRandomAccessContent(long length) {
    super(RandomAccessMode.READ);
    this.length = length;
  }

  /**
   * Opens a stream that starts at the given offset and runs to the end of the file. Called only for
   * an offset before the end of the file. Closing the stream before its end must be cheap: it must
   * not read the remaining bytes.
   *
   * @param offset the first byte to read, from 0 to length - 1
   */
  protected abstract InputStream openStream(long offset) throws IOException;

  @Override
  protected DataInputStream getDataInputStream() throws IOException {
    if (current == null) {
      InputStream source =
          filePointer >= length ? InputStream.nullInputStream() : openStream(filePointer);
      tracker = new PositionTrackingInputStream(source);
      current = new DataInputStream(tracker);
    }
    return current;
  }

  @Override
  public long getFilePointer() {
    return filePointer;
  }

  @Override
  public void seek(long position) throws IOException {
    if (position < 0) {
      throw new IOException("Can't seek to a negative position: " + position);
    }
    if (position == filePointer && current != null) {
      return;
    }
    closeCurrent();
    filePointer = position;
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public void close() throws IOException {
    closeCurrent();
  }

  private void closeCurrent() throws IOException {
    if (current != null) {
      DataInputStream closing = current;
      current = null;
      tracker = null;
      closing.close();
    }
  }

  /** Moves the file pointer along with every byte read or skipped from the open stream. */
  private final class PositionTrackingInputStream extends FilterInputStream {

    PositionTrackingInputStream(InputStream in) {
      super(in);
    }

    @Override
    public int read() throws IOException {
      int b = super.read();
      if (b >= 0) {
        filePointer++;
      }
      return b;
    }

    @Override
    public int read(byte[] buffer, int offset, int count) throws IOException {
      int n = super.read(buffer, offset, count);
      if (n > 0) {
        filePointer += n;
      }
      return n;
    }

    @Override
    public long skip(long count) throws IOException {
      long n = super.skip(count);
      if (n > 0) {
        filePointer += n;
      }
      return n;
    }

    @Override
    public boolean markSupported() {
      // A reset would move the source back without moving the file pointer
      return false;
    }

    @Override
    public void close() throws IOException {
      // Closing the stream from getInputStream() ends this range; the next read opens a new one
      if (tracker == this) {
        current = null;
        tracker = null;
      }
      super.close();
    }
  }
}
