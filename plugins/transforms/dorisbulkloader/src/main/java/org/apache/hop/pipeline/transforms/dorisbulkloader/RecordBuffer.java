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
 *
 */

package org.apache.hop.pipeline.transforms.dorisbulkloader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/** Channel of record stream and HTTP data stream. */
public class RecordBuffer {

  private final BlockingQueue<ByteBuffer> writeQueue;
  private final BlockingQueue<ByteBuffer> readQueue;
  private ByteBuffer currentWriteBuffer;
  private ByteBuffer currentReadBuffer;
  private final int bufferSize; // A buffer's capacity, in bytes.
  /** BufferSize * BufferCount is the max capacity to buffer data before doing real stream load */
  private final int bufferCount;
  /** write length in bytes to recordBuffer */
  private long writeLength = 0;

  public RecordBuffer(int bufferSize, int bufferCount) {
    this.writeQueue = new ArrayBlockingQueue<>(bufferCount);
    for (int index = 0; index < bufferCount; index++) {
      this.writeQueue.add(ByteBuffer.allocate(bufferSize));
    }
    readQueue = new LinkedBlockingDeque<>();
    this.bufferSize = bufferSize;
    this.bufferCount = bufferCount;
  }

  /**
   * Get recordBuffer's writting length in bytes
   *
   * @return
   */
  public long getWriteLength() {
    return writeLength;
  }

  /** This method will be called at the beginning of each stream load batch */
  public void startBufferData() {
    writeLength = 0;
  }

  /**
   * write data into buffer for a specified stream load batch
   *
   * @param buf
   * @throws InterruptedException
   */
  public void write(byte[] buf) throws InterruptedException {
    writeLength += buf.length;

    int wPos = 0;
    do {
      if (currentWriteBuffer == null) {
        currentWriteBuffer = writeQueue.take();
      }
      int available = currentWriteBuffer.remaining();
      int nWrite = Math.min(available, buf.length - wPos);
      currentWriteBuffer.put(buf, wPos, nWrite);
      wPos += nWrite;
      if (currentWriteBuffer.remaining() == 0) {
        currentWriteBuffer.flip();
        readQueue.put(currentWriteBuffer);
        currentWriteBuffer = null;
      }
    } while (wPos != buf.length);
  }

  /**
   * if true then could write into buffer successfully
   *
   * @param writeLength
   * @return
   */
  public boolean canWrite(long writeLength) {
    return this.writeLength + writeLength <= bufferSize * bufferCount;
  }

  /**
   * read data from buffer into buff variable
   *
   * @param buf
   * @param start the start position in buff
   * @param length
   * @return the number in bytes to read
   * @throws InterruptedException
   */
  public int read(byte[] buf, int start, int length) throws InterruptedException {
    if (currentReadBuffer == null) {
      currentReadBuffer = readQueue.take();
    }

    int available = currentReadBuffer.remaining();
    int nRead = Math.min(available, length);
    currentReadBuffer.get(buf, start, nRead);
    if (currentReadBuffer.remaining() == 0) {
      recycleBuffer(currentReadBuffer);
      currentReadBuffer = null;
    }
    return nRead;
  }

  /**
   * recycle read buffer into write buffer queue
   *
   * @param buffer
   * @throws InterruptedException
   */
  private void recycleBuffer(ByteBuffer buffer) throws InterruptedException {
    buffer.clear();
    writeQueue.put(buffer);
  }

  /**
   * This method will be called at the end of each stream load batch
   *
   * @throws IOException
   */
  public void stopBufferData() throws InterruptedException {
    if (currentWriteBuffer != null) {
      currentWriteBuffer.flip();
      readQueue.put(currentWriteBuffer);
      currentWriteBuffer = null;
    }
  }

  /**
   * get write queue size
   *
   * @return
   */
  public int getWriteQueueSize() {
    return writeQueue.size();
  }

  /**
   * get read queue size
   *
   * @return
   */
  public int getReadQueueSize() {
    return readQueue.size();
  }

  /** Support unit test */
  public void clearRecordBuffer() throws InterruptedException {
    byte[] buffer = new byte[4096];
    int readLen;
    for (long remaining = this.writeLength; remaining > 0L; remaining -= (long) readLen) {
      readLen = read(buffer, 0, (int) Math.min(4096L, remaining));
      if (readLen == -1) {
        break;
      }
    }
  }
}
