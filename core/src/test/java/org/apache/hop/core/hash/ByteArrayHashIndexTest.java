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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.junit.Test;

public class ByteArrayHashIndexTest {

  @Test
  public void testArraySizeConstructor() {
    // Test default constructor (uses STANDARD_INDEX_SIZE = 512)
    ByteArrayHashIndex obj = new ByteArrayHashIndex(new RowMeta());
    assertEquals(512, obj.getSize());
    assertEquals(0, obj.getCount());

    // Test with explicit sizes - should round up to power of 2
    obj = new ByteArrayHashIndex(new RowMeta(), 1);
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

  /**
   * Regression test: verify that count is incremented when inserting into an empty home slot. This
   * ensures resize() is called and prevents infinite loops when the table fills up.
   */
  @Test
  public void testCountIncrementedOnEmptySlotInsert() throws HopValueException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));

    ByteArrayHashIndex hashIndex = new ByteArrayHashIndex(rowMeta, 4);

    assertEquals("Initial count should be 0", 0, hashIndex.getCount());

    // Insert first entry - goes into empty home slot
    byte[] key1 = RowMeta.extractData(rowMeta, new Object[] {1L});
    hashIndex.put(key1, new byte[] {1});
    assertEquals("Count should be 1 after first insert", 1, hashIndex.getCount());

    // Insert second entry
    byte[] key2 = RowMeta.extractData(rowMeta, new Object[] {2L});
    hashIndex.put(key2, new byte[] {2});
    assertEquals("Count should be 2 after second insert", 2, hashIndex.getCount());

    // Insert more entries to trigger resize
    for (int i = 3; i <= 10; i++) {
      byte[] key = RowMeta.extractData(rowMeta, new Object[] {(long) i});
      hashIndex.put(key, new byte[] {(byte) i});
    }
    assertEquals("Count should be 10 after all inserts", 10, hashIndex.getCount());

    // Verify table was resized (started at 4, should be larger now)
    assertTrue(hashIndex.getSize() > 4);

    // Verify all entries are retrievable
    for (int i = 1; i <= 10; i++) {
      byte[] key = RowMeta.extractData(rowMeta, new Object[] {(long) i});
      byte[] value = hashIndex.get(key);
      assertNotNull("Entry " + i + " should be retrievable", value);
      assertEquals("Entry " + i + " should have correct value", (byte) i, value[0]);
    }
  }

  /**
   * Tests that put() does not hang when filling up the table. Before the fix, inserting into an
   * empty home slot didn't call resize(), causing infinite loops when the table became full.
   */
  @Test
  public void testPutDoesNotHangWhenTableFills() throws Exception {
    final int TIMEOUT_SECONDS = 2;
    final CountDownLatch done = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<>();

    Thread testThread =
        new Thread(
            () -> {
              try {
                RowMeta rowMeta = new RowMeta();
                rowMeta.addValueMeta(new ValueMetaInteger("id"));

                // Start with a small table to trigger resize quickly
                ByteArrayHashIndex hashIndex = new ByteArrayHashIndex(rowMeta, 2);

                // Insert enough entries to fill and resize multiple times
                for (int i = 0; i < 100; i++) {
                  byte[] key = RowMeta.extractData(rowMeta, new Object[] {(long) i});
                  hashIndex.put(key, new byte[] {(byte) i});
                }

                done.countDown();
              } catch (Throwable t) {
                error.set(t);
                done.countDown();
              }
            });

    testThread.start();

    boolean completed = done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    if (!completed) {
      testThread.interrupt();
      fail(
          "put() appears to hang - possible infinite loop (did not complete within "
              + TIMEOUT_SECONDS
              + " seconds)");
    }

    if (error.get() != null) {
      fail("Test failed with exception: " + error.get().getMessage());
    }
  }
}
