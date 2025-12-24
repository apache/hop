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

package org.apache.hop.pipeline.transforms.mongodbdelete;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class ComparatorTest {

  @Test
  void testGetValue() {
    assertEquals("=", Comparator.EQUAL.getValue());
    assertEquals("<>", Comparator.NOT_EQUAL.getValue());
    assertEquals("<", Comparator.LESS_THAN.getValue());
    assertEquals("<=", Comparator.LESS_THAN_EQUAL.getValue());
    assertEquals(">", Comparator.GREATER_THAN.getValue());
    assertEquals(">=", Comparator.GREATER_THAN_EQUAL.getValue());
    assertEquals("BETWEEN", Comparator.BETWEEN.getValue());
    assertEquals("IS NULL", Comparator.IS_NULL.getValue());
    assertEquals("IS NOT NULL", Comparator.IS_NOT_NULL.getValue());
  }

  @Test
  void testAsLabel() {
    String[] labels = Comparator.asLabel();

    assertNotNull(labels);
    assertEquals(9, labels.length);

    // Verify all values are present
    assertArrayEquals(
        new String[] {"=", "<>", "<", "<=", ">", ">=", "BETWEEN", "IS NULL", "IS NOT NULL"},
        labels);
  }

  @Test
  void testValuesCount() {
    assertEquals(9, Comparator.values().length);
  }

  @Test
  void testValueOf() {
    assertEquals(Comparator.EQUAL, Comparator.valueOf("EQUAL"));
    assertEquals(Comparator.NOT_EQUAL, Comparator.valueOf("NOT_EQUAL"));
    assertEquals(Comparator.LESS_THAN, Comparator.valueOf("LESS_THAN"));
    assertEquals(Comparator.LESS_THAN_EQUAL, Comparator.valueOf("LESS_THAN_EQUAL"));
    assertEquals(Comparator.GREATER_THAN, Comparator.valueOf("GREATER_THAN"));
    assertEquals(Comparator.GREATER_THAN_EQUAL, Comparator.valueOf("GREATER_THAN_EQUAL"));
    assertEquals(Comparator.BETWEEN, Comparator.valueOf("BETWEEN"));
    assertEquals(Comparator.IS_NULL, Comparator.valueOf("IS_NULL"));
    assertEquals(Comparator.IS_NOT_NULL, Comparator.valueOf("IS_NOT_NULL"));
  }
}
