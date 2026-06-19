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

package org.apache.hop.core.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class CountingStreamsTest {

  @Test
  void countingInputStreamSingleByteRead() throws IOException {
    byte[] data = {1, 2, 3};
    try (CountingInputStream in = new CountingInputStream(new ByteArrayInputStream(data))) {
      assertEquals(1, in.read());
      assertEquals(2, in.read());
      assertEquals(3, in.read());
      assertEquals(-1, in.read());
      assertEquals(3, in.getCount());
    }
  }

  @Test
  void countingInputStreamBulkRead() throws IOException {
    byte[] data = {9, 8, 7, 6};
    try (CountingInputStream in = new CountingInputStream(new ByteArrayInputStream(data))) {
      byte[] buf = new byte[4];
      int n = in.read(buf, 0, 4);
      assertEquals(4, n);
      assertArrayEquals(data, buf);
      assertEquals(4, in.getCount());
      assertEquals(-1, in.read(buf, 0, 1));
      assertEquals(4, in.getCount());
    }
  }

  @Test
  void countingOutputStreamWrites() throws IOException {
    ByteArrayOutputStream raw = new ByteArrayOutputStream();
    try (CountingOutputStream out = new CountingOutputStream(raw)) {
      out.write(42);
      out.write(new byte[] {1, 2, 3}, 1, 2);
      assertEquals(3, out.getCount());
    }
    assertArrayEquals(new byte[] {42, 2, 3}, raw.toByteArray());
  }

  @Test
  void countingInputStreamEofDoesNotIncrementCount() throws IOException {
    try (CountingInputStream in = new CountingInputStream(new ByteArrayInputStream(new byte[0]))) {
      assertEquals(-1, in.read());
      assertEquals(0, in.getCount());
    }
  }

  @Test
  void countingInputStreamZeroLengthReadDoesNotIncrement() throws IOException {
    try (CountingInputStream in =
        new CountingInputStream(new ByteArrayInputStream(new byte[] {1}))) {
      byte[] buf = new byte[1];
      assertEquals(0, in.read(buf, 0, 0));
      assertEquals(0, in.getCount());
      assertEquals(1, in.read(buf, 0, 1));
      assertEquals(1, in.getCount());
    }
  }
}
