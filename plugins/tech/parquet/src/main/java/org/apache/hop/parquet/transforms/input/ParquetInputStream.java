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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.hop.core.exception.HopException;
import org.apache.parquet.io.SeekableInputStream;

public class ParquetInputStream extends SeekableInputStream {

  private final byte[] bytes;
  private final ByteArrayInputStream inputStream;
  private long position;

  /**
   * Read the input stream into memory to get a seekable input stream...
   *
   * @param inputStream
   * @throws HopException
   */
  public ParquetInputStream(InputStream inputStream) throws IOException {

    try {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      int bytesRead;
      byte[] chunk = new byte[64 * 1024];
      while ((bytesRead = inputStream.read(chunk, 0, chunk.length)) != -1) {
        buffer.write(chunk, 0, bytesRead);
      }
      this.bytes = buffer.toByteArray();
      this.inputStream = new ByteArrayInputStream(bytes);
    } catch (IOException e) {
      throw new IOException("Unable to read input stream data into memory", e);
    }

    position = 0L;
  }

  @Override
  public int read(byte[] buffer) throws IOException {
    int bytesRead = inputStream.read(buffer);
    position += bytesRead;
    return bytesRead;
  }

  @Override
  public int read(byte[] bytes, int i, int i1) throws IOException {
    int bytesRead = super.read(bytes, i, i1);
    position += bytesRead;
    return bytesRead;
  }

  @Override
  public long skip(long l) throws IOException {
    long skipped = super.skip(l);
    position += skipped;
    return skipped;
  }

  @Override
  public int available() throws IOException {
    return inputStream.available();
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }

  @Override
  public synchronized void mark(int pos) {
    inputStream.mark(pos);
    position = pos;
  }

  @Override
  public synchronized void reset() throws IOException {
    inputStream.reset();
    position = 0;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public long getPos() throws IOException {
    return position;
  }

  @Override
  public void seek(long pos) throws IOException {
    inputStream.reset();
    inputStream.skip(pos);
    position = pos;
  }

  @Override
  public void readFully(byte[] buffer) throws IOException {
    int read = inputStream.read(buffer);
    position += read;
  }

  @Override
  public void readFully(byte[] buffer, int i, int i1) throws IOException {
    int read = inputStream.read(buffer, i, i1);
    position += read;
  }

  @Override
  public int read(ByteBuffer byteBuffer) throws IOException {
    int read = inputStream.read(byteBuffer.array());
    position += read;
    return read;
  }

  @Override
  public void readFully(ByteBuffer byteBuffer) throws IOException {
    int read = inputStream.read(byteBuffer.array());
    position += read;
  }

  @Override
  public int read() throws IOException {
    int c = inputStream.read();
    position++;
    return c;
  }
}
