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
package org.apache.hop.pipeline.transforms.randomvalue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import javax.crypto.KeyGenerator;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Uuid4Util;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.junit.jupiter.api.Test;

/** Unit test for {@link RandomValueData}. */
class RandomValueDataTest {

  @Test
  void testDefaultState() {
    RandomValueData data = new RandomValueData();

    assertFalse(data.readsRows);
    assertNull(data.outputRowMeta);
    assertNull(data.u4);
    assertNull(data.keyGenHmacMD5);
    assertNull(data.keyGenHmacSHA1);
    assertNull(data.keyGenHmacSHA256);
    assertNull(data.keyGenHmacSHA512);
    assertNull(data.keyGenHmacSHA384);
    assertNull(data.randomGenerator);
  }

  @Test
  void testFieldAssignment() {
    RandomValueData data = new RandomValueData();
    RowMeta rowMeta = new RowMeta();
    Random random = new Random(42);
    Uuid4Util uuid4Util = new Uuid4Util();

    data.readsRows = true;
    data.outputRowMeta = rowMeta;
    data.randomGenerator = random;
    data.u4 = uuid4Util;

    assertTrue(data.readsRows);
    assertSame(rowMeta, data.outputRowMeta);
    assertSame(random, data.randomGenerator);
    assertSame(uuid4Util, data.u4);
  }

  @Test
  void testKeyGeneratorAssignment() throws Exception {
    RandomValueData data = new RandomValueData();

    data.keyGenHmacMD5 = KeyGenerator.getInstance("HmacMD5");
    data.keyGenHmacSHA1 = KeyGenerator.getInstance("HmacSHA1");

    assertNotNull(data.keyGenHmacMD5);
    assertNotNull(data.keyGenHmacSHA1);
    assertEquals("HmacMD5", data.keyGenHmacMD5.getAlgorithm());
    assertEquals("HmacSHA1", data.keyGenHmacSHA1.getAlgorithm());
  }

  @Test
  void testInheritance() {
    RandomValueData data = new RandomValueData();

    assertNotNull(data);
    assertInstanceOf(BaseTransformData.class, data);
  }

  @Test
  void testThreadSafetyBasic() throws InterruptedException {
    RandomValueData data = new RandomValueData();

    Thread t = Thread.startVirtualThread(() -> data.readsRows = true);
    t.join();

    assertTrue(data.readsRows);
  }
}
