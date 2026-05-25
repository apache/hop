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

package org.apache.hop.parquet.transforms.output;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import org.junit.jupiter.api.Test;

/** Unit test for {@link ParquetOutputStream} */
class ParquetOutputStreamTest {

  @Test
  void testWriteSingleByte() throws Exception {
    ByteArrayOutputStream delegate = new ByteArrayOutputStream();
    ParquetOutputStream stream = new ParquetOutputStream(delegate);

    stream.write(42);
    stream.flush();

    assertEquals(1, stream.getPos());
    assertArrayEquals(new byte[] {42}, delegate.toByteArray());
  }

  @Test
  void testWriteByteArray() throws Exception {
    ByteArrayOutputStream delegate = new ByteArrayOutputStream();
    ParquetOutputStream stream = new ParquetOutputStream(delegate);
    byte[] data = new byte[] {1, 2, 3, 4, 5};

    stream.write(data);
    stream.flush();

    assertEquals(5, stream.getPos());
    assertArrayEquals(data, delegate.toByteArray());
  }

  @Test
  void testWriteByteArrayRange() throws Exception {
    ByteArrayOutputStream delegate = new ByteArrayOutputStream();
    ParquetOutputStream stream = new ParquetOutputStream(delegate);
    byte[] data = new byte[] {9, 1, 2, 3, 8};

    stream.write(data, 1, 3);
    stream.flush();

    assertEquals(3, stream.getPos());
    assertArrayEquals(new byte[] {1, 2, 3}, delegate.toByteArray());
  }
}
