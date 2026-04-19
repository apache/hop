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

package org.apache.hop.pipeline.transforms.systemdata;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.junit.jupiter.api.Test;

/** Unit test for {@link SystemDataData} */
class SystemDataDataTests {

  @Test
  void testDefaultState() {
    SystemDataData data = new SystemDataData();

    assertFalse(data.readsRows, "readsRows should default to false");
    assertNull(data.outputRowMeta, "outputRowMeta should default to null");
  }

  @Test
  void testFieldAssignment() {
    SystemDataData data = new SystemDataData();

    data.readsRows = true;

    RowMeta rowMeta = new RowMeta();
    data.outputRowMeta = rowMeta;

    assertTrue(data.readsRows);
    assertSame(rowMeta, data.outputRowMeta);
  }

  @Test
  void testInheritance() {
    SystemDataData data = new SystemDataData();

    assertNotNull(data);
    assertInstanceOf(BaseTransformData.class, data);
  }

  @Test
  void testThreadSafetyBasic() throws InterruptedException {
    SystemDataData data = new SystemDataData();

    Thread t = Thread.startVirtualThread(() -> data.readsRows = true);
    t.start();
    t.join();

    assertTrue(data.readsRows);
  }
}
