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

package org.apache.hop.parquet.transforms.input;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;

public class ParquetStream implements InputFile, Closeable {

  private final FileObject fileObject;
  private final String filename;
  private final boolean isLocal;
  private final LocalInputFile localInputFile;

  public ParquetStream(FileObject fileObject, String filename) throws IOException {
    this.fileObject = fileObject;
    this.filename = filename;
    // Detect if the file is local by checking the VFS scheme
    // For remote files, localInputFile is null. VfsSeekableInputStream will be used instead
    this.isLocal = "file".equals(fileObject.getName().getScheme());
    this.localInputFile =
        isLocal ? new LocalInputFile(Paths.get(fileObject.getName().getPath())) : null;
  }

  @Override
  public long getLength() throws IOException {
    if (isLocal) {
      return localInputFile.getLength();
    }
    return fileObject.getContent().getSize();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    if (isLocal) {
      // Native Parquet implementation
      return localInputFile.newStream();
    } else {
      // For remote file, DelegatingSeekableInputStream handles basic read/skip operations
      return new DelegatingSeekableInputStream(new VfsSeekableInputStream(fileObject)) {
        @Override
        public void seek(long newPos) throws IOException {
          ((VfsSeekableInputStream) getStream()).seek(newPos);
        }

        @Override
        public long getPos() throws IOException {
          return ((VfsSeekableInputStream) getStream()).getPos();
        }
      };
    }
  }

  @Override
  public void close() throws IOException {
    if (!isLocal && fileObject != null) {
      fileObject.getContent().close();
    }
  }

  @Override
  public String toString() {
    return "ParquetStream of file '" + filename + "'";
  }

  // SeekableInputStream implementation for remote files
  private static class VfsSeekableInputStream extends InputStream {

    private final FileObject fileObject;
    private InputStream currentStream;
    private long pos = 0;

    public VfsSeekableInputStream(FileObject fileObject) throws IOException {
      this.fileObject = fileObject;
      this.currentStream = HopVfs.getInputStream(fileObject);
    }

    // Reads a single byte and advances the position by 1
    @Override
    public int read() throws IOException {
      int b = currentStream.read();
      if (b != -1) {
        pos++;
      }

      return b;
    }

    // Reads up to len bytes into buffer b starting at offset off
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int n = currentStream.read(b, off, len);
      if (n > 0) {
        pos += n;
      }

      return n;
    }

    public long getPos() {
      return pos;
    }

    public void seek(long newPos) throws IOException {
      currentStream.close();
      currentStream = HopVfs.getInputStream(fileObject);
      pos = 0;
      long remaining = newPos;
      while (remaining > 0) {
        long skipped = currentStream.skip(remaining);
        if (skipped > 0) {
          // Advance by the number of bytes skipped
          remaining -= skipped;
          // If skip() returns 0, stream may be blocked
        } else if (skipped == 0) {
          // Try reading one byte manually to advance
          if (currentStream.read() == -1) {
            break;
          }
          // One byte consumed via read(), decrement remaining accordingly
          remaining--;
        } else {
          break;
        }
      }
      // Update position to reflect where we actually ended up
      pos = newPos - remaining;
    }

    @Override
    public void close() throws IOException {
      currentStream.close();
    }
  }
}
