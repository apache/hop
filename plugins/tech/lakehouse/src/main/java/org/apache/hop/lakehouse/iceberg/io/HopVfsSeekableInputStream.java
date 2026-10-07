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

package org.apache.hop.lakehouse.iceberg.io;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * Seekable stream over a Hop VFS file. Parquet readers jump to the footer first and then to each
 * column chunk, so seeking has to be cheap. When the VFS provider supports random access we use it
 * directly. Otherwise the stream is reopened and skipped forward, which is slower but works with
 * every provider.
 */
class HopVfsSeekableInputStream extends SeekableInputStream {

  private final FileObject file;
  private RandomAccessContent randomAccess;
  private InputStream stream;
  private long pos;

  HopVfsSeekableInputStream(FileObject file) {
    this.file = file;
  }

  private InputStream stream() throws IOException {
    if (stream == null) {
      if (file.getFileSystem().hasCapability(Capability.RANDOM_ACCESS_READ)) {
        randomAccess = file.getContent().getRandomAccessContent(RandomAccessMode.READ);
        randomAccess.seek(pos);
        stream = randomAccess.getInputStream();
      } else {
        stream = file.getContent().getInputStream();
        skipFully(stream, pos);
      }
    }
    return stream;
  }

  @Override
  public long getPos() {
    return pos;
  }

  @Override
  public void seek(long newPos) throws IOException {
    if (newPos == pos && stream != null) {
      return;
    }
    if (randomAccess != null) {
      randomAccess.seek(newPos);
      stream = randomAccess.getInputStream();
    } else if (stream != null && newPos > pos) {
      skipFully(stream, newPos - pos);
    } else {
      closeStream();
      pos = newPos;
      stream();
    }
    pos = newPos;
  }

  @Override
  public int read() throws IOException {
    int b = stream().read();
    if (b >= 0) {
      pos++;
    }
    return b;
  }

  @Override
  public int read(byte[] buffer, int offset, int length) throws IOException {
    int n = stream().read(buffer, offset, length);
    if (n > 0) {
      pos += n;
    }
    return n;
  }

  @Override
  public void close() throws IOException {
    closeStream();
  }

  private void closeStream() throws IOException {
    if (randomAccess != null) {
      randomAccess.close();
      randomAccess = null;
    } else if (stream != null) {
      stream.close();
    }
    stream = null;
  }

  private static void skipFully(InputStream in, long count) throws IOException {
    long remaining = count;
    while (remaining > 0) {
      long skipped = in.skip(remaining);
      if (skipped <= 0) {
        if (in.read() < 0) {
          throw new java.io.EOFException("Reached the end of the file while seeking");
        }
        skipped = 1;
      }
      remaining -= skipped;
    }
  }
}
