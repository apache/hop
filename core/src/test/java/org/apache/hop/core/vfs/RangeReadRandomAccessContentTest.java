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

package org.apache.hop.core.vfs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class RangeReadRandomAccessContentTest {

  private static final byte[] DATA = new byte[1000];

  static {
    for (int i = 0; i < DATA.length; i++) {
      DATA[i] = (byte) (i * 7);
    }
  }

  /** Serves DATA from an offset and records every stream it opened. */
  private static final class Recording extends RangeReadRandomAccessContent {
    final List<Long> opened = new ArrayList<>();
    final List<RecordingStream> streams = new ArrayList<>();

    Recording() {
      super(DATA.length);
    }

    @Override
    protected InputStream openStream(long offset) {
      opened.add(offset);
      RecordingStream stream = new RecordingStream((int) offset);
      streams.add(stream);
      return stream;
    }
  }

  private static final class RecordingStream extends ByteArrayInputStream {
    boolean closed;

    RecordingStream(int offset) {
      super(DATA, offset, DATA.length - offset);
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }
  }

  private static byte[] range(int from, int to) {
    return Arrays.copyOfRange(DATA, from, to);
  }

  @Test
  void opensNothingUntilTheFirstRead() throws IOException {
    try (Recording content = new Recording()) {
      content.seek(500);
      content.seek(10);
      assertEquals(List.of(), content.opened);
      assertEquals(DATA[10], content.readByte());
      assertEquals(List.of(10L), content.opened);
    }
  }

  @Test
  void readsMoveTheFilePointer() throws IOException {
    try (Recording content = new Recording()) {
      byte[] buffer = new byte[20];
      content.seek(100);
      content.readFully(buffer);
      assertArrayEquals(range(100, 120), buffer);
      assertEquals(120, content.getFilePointer());
      assertEquals(DATA[120], content.readByte());
      assertEquals(121, content.getFilePointer());
      assertEquals(4, content.skipBytes(4));
      assertEquals(125, content.getFilePointer());
      assertEquals(List.of(100L), content.opened, "one stream for one forward run");
    }
  }

  @Test
  void seekClosesTheOpenStreamAndReopensAtTheNewPosition() throws IOException {
    try (Recording content = new Recording()) {
      content.seek(800);
      content.readByte();
      content.seek(5);

      assertTrue(content.streams.get(0).closed, "the stream at 800 is released on seek");
      assertEquals(DATA[5], content.readByte());
      assertEquals(List.of(800L, 5L), content.opened);
    }
  }

  @Test
  void seekToTheCurrentPositionKeepsTheStream() throws IOException {
    try (Recording content = new Recording()) {
      content.seek(300);
      content.readByte();
      content.seek(301);
      content.readByte();
      assertEquals(List.of(300L), content.opened);
    }
  }

  @Test
  void streamFromThePositionRunsToTheEnd() throws IOException {
    try (Recording content = new Recording()) {
      content.seek(950);
      InputStream in = content.getInputStream();
      assertArrayEquals(range(950, 1000), in.readAllBytes());
      assertEquals(1000, content.getFilePointer());
    }
  }

  @Test
  void closingTheStreamEndsTheRangeAndTheNextReadReopens() throws IOException {
    try (Recording content = new Recording()) {
      content.seek(10);
      InputStream in = content.getInputStream();
      assertEquals(DATA[10], (byte) in.read());
      in.close();

      assertTrue(content.streams.get(0).closed);
      assertEquals(DATA[11], content.readByte());
      assertEquals(List.of(10L, 11L), content.opened);
    }
  }

  @Test
  void atOrPastTheEndNothingIsOpenedAndNothingRead() throws IOException {
    try (Recording content = new Recording()) {
      content.seek(1000);
      assertEquals(-1, content.getInputStream().read());
      content.seek(5000);
      assertEquals(-1, content.getInputStream().read());
      assertEquals(List.of(), content.opened);
    }
  }

  @Test
  void closeReleasesTheOpenStream() throws IOException {
    Recording content = new Recording();
    content.readByte();
    content.close();
    assertTrue(content.streams.get(0).closed);
  }

  @Test
  void negativeSeekIsRefused() throws IOException {
    try (Recording content = new Recording()) {
      assertThrows(IOException.class, () -> content.seek(-1));
    }
  }

  @Test
  void lengthIsTheFileSize() throws IOException {
    try (Recording content = new Recording()) {
      assertEquals(DATA.length, content.length());
    }
  }
}
