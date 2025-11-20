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

package org.apache.hop.core.hash;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowMeta;
import org.junit.Test;

public class ByteArrayHashIndexTest {

  @Test
  public void testArraySizeConstructor() {
    ByteArrayHashIndex obj = new ByteArrayHashIndex(new RowMeta(), 1);
    assertEquals(1, obj.getSize());

    obj = new ByteArrayHashIndex(new RowMeta(), 2);
    assertEquals(2, obj.getSize());

    obj = new ByteArrayHashIndex(new RowMeta(), 3);
    assertEquals(4, obj.getSize());

    obj = new ByteArrayHashIndex(new RowMeta(), 12);
    assertEquals(16, obj.getSize());

    obj = new ByteArrayHashIndex(new RowMeta(), 99);
    assertEquals(128, obj.getSize());

    obj = new ByteArrayHashIndex(new RowMeta(), 9);
    assertEquals(0, obj.getCount());
  }

  @Test
  public void testGetAndPut() throws HopValueException {
    ByteArrayHashIndex obj = new ByteArrayHashIndex(new RowMeta(), 10);
    assertNull(obj.get(new byte[] {10}));

    obj.put(new byte[] {10}, new byte[] {53, 12});
    assertNotNull(obj.get(new byte[] {10}));
    assertArrayEquals(new byte[] {53, 12}, obj.get(new byte[] {10}));
  }
}
