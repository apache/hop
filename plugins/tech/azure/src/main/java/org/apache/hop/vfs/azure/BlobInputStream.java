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

package org.apache.hop.vfs.azure;

import com.azure.storage.file.datalake.models.DataLakeFileOpenInputStreamResult;
import com.azure.storage.file.datalake.models.PathProperties;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reads an Azure file and never returns more than {@code fileSize} bytes: a blob padded to a page
 * boundary reads as zeroes past its real size. Every read and skip advances one position counter,
 * which drives the clamp and {@link #available()}.
 */
public class BlobInputStream extends InputStream {

  private final InputStream inputStream;
  private final long fileSize;
  private long position = 0;
  private long markedPosition = 0;

  /**
   * @param fileSize the size known to the caller, used only when the opened stream reports none; it
   *     can be stale (list cache), while the stream's own properties describe the version read
   */
  public BlobInputStream(DataLakeFileOpenInputStreamResult inputStream, long fileSize) {
    this.inputStream = inputStream.getInputStream();
    PathProperties properties = inputStream.getProperties();
    this.fileSize = properties != null ? properties.getFileSize() : fileSize;
  }

  private long remaining() {
    return Math.max(0L, fileSize - position);
  }

  @Override
  public int read() throws IOException {
    if (remaining() == 0) {
      return -1;
    }
    int c = inputStream.read();
    if (c >= 0) {
      position++;
    }
    return c;
  }

  @Override
  public int read(byte[] bytes) throws IOException {
    return read(bytes, 0, bytes.length);
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    if (length == 0) {
      return 0;
    }
    long remaining = remaining();
    if (remaining == 0) {
      return -1;
    }
    int readSize = inputStream.read(bytes, offset, (int) Math.min(length, remaining));
    if (readSize > 0) {
      position += readSize;
    }
    return readSize;
  }

  @Override
  public long skip(long length) throws IOException {
    if (length <= 0) {
      return 0;
    }
    long skipped = inputStream.skip(Math.min(length, remaining()));
    if (skipped > 0) {
      position += skipped;
    }
    return Math.max(0L, skipped);
  }

  @Override
  public int available() throws IOException {
    return (int) Math.min(Integer.MAX_VALUE, remaining());
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }

  @Override
  public synchronized void mark(int readLimit) {
    inputStream.mark(readLimit);
    markedPosition = position;
  }

  @Override
  public synchronized void reset() throws IOException {
    inputStream.reset();
    position = markedPosition;
  }

  @Override
  public boolean markSupported() {
    return inputStream.markSupported();
  }
}
