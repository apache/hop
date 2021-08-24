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

  ReadChannel channel;
  ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);

  public ReadChannelInputStream(ReadChannel channel) {
    this.channel = channel;
  }

  @Override
  public int read() throws IOException {
    if (channel == null) {
      return -1;
    }
    byte[] b = new byte[1];
    int r = read(b);
    if (r > 0) {
      return b[0] & 0xFF;
    }
    return r;
  }

  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    if (channel == null) {
      return -1;
    }
    int limit = bytes.limit();
    if (len < bytes.remaining()) {
      bytes.limit(len);
    }
    int res = channel.read(bytes);
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

  public synchronized void close() {
    if (channel != null) {
      channel.close();
      channel = null;
    }
  }
}
