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

  WriteChannel channel;
  ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);

  public WriteChannelOutputStream(WriteChannel channel) {
    this.channel = channel;
  }

  @Override
  public void write(int b) throws IOException {
    if (channel == null) {
      throw new ClosedChannelException();
    }
    bytes.put((byte) b);
    bytes.flip();
    channel.write(bytes);
    bytes.compact();
  }

  @Override
  public synchronized void write(byte[] buf, int off, int len) throws IOException {
    if (channel == null) {
      throw new ClosedChannelException();
    }
    int count = 0;
    while (count < len) {
      int c = Math.min(len - count, bytes.remaining());
      bytes.put(buf, off + count, c);
      bytes.flip();
      while (bytes.hasRemaining()) {
        channel.write(bytes);
      }
      bytes.compact();
      count += c;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (channel != null) {
      channel.close();
      channel = null;
    }
  }
}
