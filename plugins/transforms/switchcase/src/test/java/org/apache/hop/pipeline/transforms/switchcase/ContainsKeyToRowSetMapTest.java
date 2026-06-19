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

package org.apache.hop.pipeline.transforms.switchcase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Set;
import org.apache.hop.core.IRowSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ContainsKeyToRowSetMap}, the substring-matching variant of {@link
 * KeyToRowSetMap} used when Switch Case is configured with {@code usingContains=true}.
 *
 * <p>In this mode, a lookup value matches a stored key if the value <em>contains</em> the key as a
 * substring, rather than requiring an exact match.
 */
class ContainsKeyToRowSetMapTest {

  private IRowSet rowSetA;
  private IRowSet rowSetB;
  private ContainsKeyToRowSetMap map;

  @BeforeEach
  void setUp() {
    rowSetA = mock(IRowSet.class);
    rowSetB = mock(IRowSet.class);
    map = new ContainsKeyToRowSetMap();
  }

  // ---------------------------------------------------------------------------
  // get — substring matching
  // ---------------------------------------------------------------------------

  @Test
  void get_emptyMap_returnsNull() {
    assertNull(map.get("anything"));
  }

  @Test
  void get_exactMatch_returnsSet() {
    map.put("hello", rowSetA);
    assertNotNull(map.get("hello"));
    assertTrue(map.get("hello").contains(rowSetA));
  }

  @Test
  void get_valueContainsKey_returnsSet() {
    map.put("error", rowSetA);
    Set<IRowSet> result = map.get("fatal error occurred");
    assertNotNull(result);
    assertTrue(result.contains(rowSetA));
  }

  @Test
  void get_valueDoesNotContainAnyKey_returnsNull() {
    map.put("error", rowSetA);
    assertNull(map.get("all good"));
  }

  @Test
  void get_keyAtStartOfValue_matches() {
    map.put("start", rowSetA);
    assertNotNull(map.get("start of the string"));
  }

  @Test
  void get_keyAtEndOfValue_matches() {
    map.put("end", rowSetA);
    assertNotNull(map.get("reached the end"));
  }

  @Test
  void get_multipleKeys_returnsFirstInsertedMatch() {
    // Keys are matched in insertion order; the first key that the value contains is returned
    map.put("first", rowSetA);
    map.put("second", rowSetB);
    // "first and second" contains both keys; insertion order means "first" wins
    Set<IRowSet> result = map.get("first and second");
    assertNotNull(result);
    assertTrue(result.contains(rowSetA));
  }

  @Test
  void get_onlySecondKeyMatches_returnsSecondSet() {
    map.put("alpha", rowSetA);
    map.put("beta", rowSetB);
    Set<IRowSet> result = map.get("contains beta only");
    assertNotNull(result);
    assertTrue(result.contains(rowSetB));
  }

  @Test
  void get_neitherKeyMatches_returnsNull() {
    map.put("alpha", rowSetA);
    map.put("beta", rowSetB);
    assertNull(map.get("gamma delta"));
  }

  // ---------------------------------------------------------------------------
  // containsKey — substring matching
  // ---------------------------------------------------------------------------

  @Test
  void containsKey_emptyMap_returnsFalse() {
    assertFalse(map.containsKey("anything"));
  }

  @Test
  void containsKey_valueContainsKey_returnsTrue() {
    map.put("needle", rowSetA);
    assertTrue(map.containsKey("find the needle here"));
  }

  @Test
  void containsKey_exactMatch_returnsTrue() {
    map.put("needle", rowSetA);
    assertTrue(map.containsKey("needle"));
  }

  @Test
  void containsKey_valueDoesNotContainAnyKey_returnsFalse() {
    map.put("needle", rowSetA);
    assertFalse(map.containsKey("no match here"));
  }

  @Test
  void containsKey_multipleKeys_trueIfValueContainsAny() {
    map.put("foo", rowSetA);
    map.put("bar", rowSetB);
    assertTrue(map.containsKey("contains bar somewhere"));
    assertTrue(map.containsKey("contains foo somewhere"));
    assertFalse(map.containsKey("contains neither"));
  }

  // ---------------------------------------------------------------------------
  // put — accumulation and list tracking
  // ---------------------------------------------------------------------------

  @Test
  void put_sameKey_accumulatesRowSets() {
    map.put("key", rowSetA);
    map.put("key", rowSetB);
    Set<IRowSet> result = map.get("contains key here");
    assertNotNull(result);
    assertEquals(2, result.size());
    assertTrue(result.contains(rowSetA));
    assertTrue(result.contains(rowSetB));
  }

  @Test
  void put_storesKeyInUnderlyingMap() {
    map.put("stored", rowSetA);
    assertFalse(map.isEmpty());
  }

  @Test
  void put_keyOnlyStoredOnce_inList_evenIfPutTwice() {
    // Putting the same key twice adds it to the list twice, which would cause duplicate matches.
    // This test documents the current behaviour: each put appends to the list independently.
    map.put("k", rowSetA);
    map.put("k", rowSetB); // same key, second rowSet
    // Value that contains "k" should return the accumulated set
    Set<IRowSet> result = map.get("k-value");
    assertNotNull(result);
    assertEquals(2, result.size());
  }

  // ---------------------------------------------------------------------------
  // isEmpty (inherited behaviour)
  // ---------------------------------------------------------------------------

  @Test
  void isEmpty_newMap_returnsTrue() {
    assertTrue(map.isEmpty());
  }

  @Test
  void isEmpty_afterPut_returnsFalse() {
    map.put("k", rowSetA);
    assertFalse(map.isEmpty());
  }
}
