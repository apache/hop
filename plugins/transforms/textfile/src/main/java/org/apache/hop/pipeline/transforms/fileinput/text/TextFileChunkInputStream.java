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

package org.apache.hop.pipeline.transforms.fileinput.text;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.vfs2.FileObject;

/**
 * The bytes of the lines that belong to one chunk of a file: a line belongs to the chunk that holds
 * its first byte. Working on bytes, below the character decoding, makes a chunk look like a short
 * file of whole lines to the reader.
 *
 * <ul>
 *   <li>A chunk that starts inside the file skips the line it starts in, unless the byte before the
 *       chunk is a line feed: that line belongs to the chunk before.
 *   <li>Past its end, a chunk finishes the line it is in, up to and including the line feed, and
 *       stops at the next line start. That line belongs to the chunk after.
 * </ul>
 *
 * Every line is read exactly once, whatever the line ends (a CR before the LF stays with its line).
 * The line feed is one byte, or two for UTF-16; offsets are multiples of its length.
 */
final class TextFileChunkInputStream extends InputStream {

  private static final int BUFFER_SIZE = 64 * 1024;

  private final InputStream source;
  private final long end;
  private final byte[] lineFeed;
  private final int unit;

  /** The file offset of the next byte the source returns. */
  private long position;

  /** Still discarding the line the chunk starts in. */
  private boolean skippingFirstLine;

  /**
   * The last unit passed on was a line feed, or nothing was passed yet at the start of the file.
   */
  private boolean atLineStart;

  private boolean ended;

  /** Bytes of a unit not yet handed out, for a read shorter than a unit. */
  private final byte[] pending;

  private int pendingFrom;
  private int pendingTo;

  /**
   * @param source the file from {@code openedAt} on
   * @param openedAt the offset the source starts at: {@code start}, or one unit before it
   * @param start the first byte of the chunk
   * @param end the byte after the chunk
   * @param lineFeed the encoded line feed
   */
  TextFileChunkInputStream(
      InputStream source, long openedAt, long start, long end, byte[] lineFeed) {
    this.source = source;
    this.end = end;
    this.lineFeed = lineFeed.clone();
    this.unit = lineFeed.length;
    this.position = openedAt;
    this.pending = new byte[unit];
    if (start == 0) {
      atLineStart = true;
    } else {
      if (openedAt != start - unit) {
        throw new IllegalArgumentException("Open a chunk one line feed before its start");
      }
      skippingFirstLine = true;
    }
  }

  /** Opens the chunk of a file, reading from one line feed before its start. */
  static TextFileChunkInputStream open(FileObject file, long start, long end, byte[] lineFeed)
      throws IOException {
    return open(file, start, end, lineFeed, TextFileReadAhead.NONE);
  }

  /**
   * Opens the chunk of a file, reading from one line feed before its start and fetching ahead up to
   * its end. The last line, past the end, is fetched when it is reached.
   */
  static TextFileChunkInputStream open(
      FileObject file, long start, long end, byte[] lineFeed, TextFileReadAhead readAhead)
      throws IOException {
    long openedAt = start == 0 ? 0 : start - lineFeed.length;
    InputStream source = new BufferedInputStream(readAhead.open(file, openedAt, end), BUFFER_SIZE);
    return new TextFileChunkInputStream(source, openedAt, start, end, lineFeed);
  }

  @Override
  public int read() throws IOException {
    byte[] one = new byte[1];
    return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
  }

  @Override
  public int read(byte[] buffer, int offset, int length) throws IOException {
    if (length == 0) {
      return 0;
    }
    if (pendingFrom < pendingTo) {
      int n = Math.min(length, pendingTo - pendingFrom);
      System.arraycopy(pending, pendingFrom, buffer, offset, n);
      pendingFrom += n;
      return n;
    }
    if (length < unit) {
      // Read a whole unit aside and hand it out in pieces
      int n = read(pending, 0, unit);
      if (n < 0) {
        return -1;
      }
      pendingFrom = 0;
      pendingTo = n;
      return read(buffer, offset, length);
    }
    if (ended) {
      return -1;
    }
    if (skippingFirstLine) {
      skipFirstLine();
      if (ended) {
        return -1;
      }
    }
    if (position < end) {
      return passUpToEnd(buffer, offset, length);
    }
    if (atLineStart) {
      // A line starting at or after the end belongs to the next chunk
      ended = true;
      return -1;
    }
    return finishLastLine(buffer, offset, length);
  }

  /** Before the end every byte is ours: pass on whole units without looking at them. */
  private int passUpToEnd(byte[] buffer, int offset, int length) throws IOException {
    int count = (int) Math.min(length, end - position);
    count -= count % unit;
    int read = source.readNBytes(buffer, offset, count);
    if (read <= 0) {
      ended = true;
      return -1;
    }
    position += read;
    if (read < count || read % unit != 0) {
      // The file ended
      ended = true;
    } else {
      atLineStart = isLineFeed(buffer, offset + read - unit);
    }
    return read;
  }

  /** Past the end: pass on the rest of the line that started before it, line feed included. */
  private int finishLastLine(byte[] buffer, int offset, int length) throws IOException {
    int copied = 0;
    while (copied + unit <= length) {
      int read = source.readNBytes(buffer, offset + copied, unit);
      if (read < unit) {
        ended = true;
        copied += Math.max(read, 0);
        break;
      }
      position += unit;
      copied += unit;
      if (isLineFeed(buffer, offset + copied - unit)) {
        atLineStart = true;
        break;
      }
    }
    return copied == 0 ? -1 : copied;
  }

  /** Discards up to and including the first line feed, starting one unit before the chunk. */
  private void skipFirstLine() throws IOException {
    byte[] unitBytes = new byte[unit];
    while (true) {
      int read = source.readNBytes(unitBytes, 0, unit);
      if (read < unit) {
        ended = true;
        return;
      }
      position += unit;
      if (isLineFeed(unitBytes, 0)) {
        skippingFirstLine = false;
        atLineStart = true;
        return;
      }
      if (position >= end) {
        // A line feed from here on starts a line past the end: no line starts in this chunk.
        // Stopping here matters for a line much longer than the chunks: otherwise every chunk
        // inside it reads on to its end.
        ended = true;
        return;
      }
    }
  }

  private boolean isLineFeed(byte[] bytes, int at) {
    for (int i = 0; i < unit; i++) {
      if (bytes[at + i] != lineFeed[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    source.close();
  }
}
