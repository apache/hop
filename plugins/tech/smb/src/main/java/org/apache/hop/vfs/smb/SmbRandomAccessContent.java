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
package org.apache.hop.vfs.smb;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.AbstractRandomAccessStreamContent;
import org.apache.commons.vfs2.util.RandomAccessMode;

/** Random access over an SMB file. Reads and writes go to an absolute file offset. */
final class SmbRandomAccessContent extends AbstractRandomAccessStreamContent {
  private final SmbShare.SmbRandom random;
  private final boolean write;
  private long filePointer;
  private DataInputStream dis;

  SmbRandomAccessContent(SmbShare.SmbRandom random, RandomAccessMode mode) {
    super(mode);
    this.random = random;
    this.write = mode.requestWrite();
  }

  @Override
  public void close() throws IOException {
    if (dis != null) {
      dis.close();
      dis = null;
    }
    random.close();
  }

  @Override
  protected DataInputStream getDataInputStream() throws IOException {
    if (dis == null) {
      dis = new DataInputStream(new OffsetInputStream());
    }
    return dis;
  }

  @Override
  public long getFilePointer() {
    return filePointer;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new FileSystemException(
          "vfs.provider/random-access-invalid-position.error", Long.valueOf(pos));
    }
    if (dis != null) {
      dis.close();
      dis = null;
    }
    filePointer = pos;
  }

  @Override
  public long length() throws IOException {
    return random.length();
  }

  @Override
  public void setLength(long newLength) throws IOException {
    random.setLength(newLength);
  }

  @Override
  public void write(int b) throws IOException {
    write(new byte[] {(byte) b}, 0, 1);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (!write) {
      throw new IOException("SMB file is open for read only");
    }
    random.write(b, off, len, filePointer);
    filePointer += len;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    // The inherited stream tracks the file pointer. A detached copy would not.
    long size = Math.max(0, length() - filePointer);
    if (size > Integer.MAX_VALUE) {
      return super.getInputStream();
    }
    byte[] data = new byte[(int) size];
    int offset = 0;
    while (offset < data.length) {
      int read = random.read(data, offset, data.length - offset, filePointer + offset);
      if (read < 0) {
        break;
      }
      offset += read;
    }
    return new ByteArrayInputStream(data, 0, offset);
  }

  private final class OffsetInputStream extends InputStream {
    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      int n = read(one, 0, 1);
      return n < 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int n = random.read(b, off, len, filePointer);
      if (n > 0) {
        filePointer += n;
      }
      return n;
    }
  }
}
