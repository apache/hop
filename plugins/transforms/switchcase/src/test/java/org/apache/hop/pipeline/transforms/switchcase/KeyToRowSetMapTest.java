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

class KeyToRowSetMapTest {

  private IRowSet rowSetA;
  private IRowSet rowSetB;
  private KeyToRowSetMap map;

  @BeforeEach
  void setUp() {
    rowSetA = mock(IRowSet.class);
    rowSetB = mock(IRowSet.class);
    map = new KeyToRowSetMap();
  }

  // ---------------------------------------------------------------------------
  // isEmpty
  // ---------------------------------------------------------------------------

  @Test
  void isEmpty_newMap_returnsTrue() {
    assertTrue(map.isEmpty());
  }

  @Test
  void isEmpty_afterPut_returnsFalse() {
    map.put("key", rowSetA);
    assertFalse(map.isEmpty());
  }

  // ---------------------------------------------------------------------------
  // get
  // ---------------------------------------------------------------------------

  @Test
  void get_missingKey_returnsNull() {
    assertNull(map.get("missing"));
  }

  @Test
  void get_existingKey_returnsSetContainingRowSet() {
    map.put("k", rowSetA);
    Set<IRowSet> result = map.get("k");
    assertNotNull(result);
    assertTrue(result.contains(rowSetA));
  }

  @Test
  void get_differentKeys_returnIndependentSets() {
    map.put("k1", rowSetA);
    map.put("k2", rowSetB);
    assertFalse(map.get("k1").contains(rowSetB));
    assertFalse(map.get("k2").contains(rowSetA));
  }

  // ---------------------------------------------------------------------------
  // put
  // ---------------------------------------------------------------------------

  @Test
  void put_sameKey_accumulatesMultipleRowSets() {
    map.put("k", rowSetA);
    map.put("k", rowSetB);
    Set<IRowSet> result = map.get("k");
    assertEquals(2, result.size());
    assertTrue(result.contains(rowSetA));
    assertTrue(result.contains(rowSetB));
  }

  @Test
  void put_sameKeyAndSameRowSet_deduplicates() {
    // The backing collection is a Set — duplicate rowsets for the same key must not be stored twice
    map.put("k", rowSetA);
    map.put("k", rowSetA);
    assertEquals(1, map.get("k").size());
  }

  @Test
  void put_multipleKeys_eachKeyHasOwnSet() {
    map.put("k1", rowSetA);
    map.put("k2", rowSetB);
    assertEquals(1, map.get("k1").size());
    assertEquals(1, map.get("k2").size());
  }

  // ---------------------------------------------------------------------------
  // containsKey
  // ---------------------------------------------------------------------------

  @Test
  void containsKey_existingKey_returnsTrue() {
    map.put("k", rowSetA);
    assertTrue(map.containsKey("k"));
  }

  @Test
  void containsKey_missingKey_returnsFalse() {
    assertFalse(map.containsKey("missing"));
  }

  @Test
  void containsKey_emptyMap_returnsFalse() {
    assertFalse(map.containsKey("anything"));
  }

  // ---------------------------------------------------------------------------
  // keySet / entrySet
  // ---------------------------------------------------------------------------

  @Test
  void keySet_containsAllInsertedKeys() {
    map.put("k1", rowSetA);
    map.put("k2", rowSetB);
    assertEquals(Set.of("k1", "k2"), map.keySet());
  }

  @Test
  void entrySet_sizeMatchesNumberOfDistinctKeys() {
    map.put("k1", rowSetA);
    map.put("k1", rowSetB); // same key, different rowset
    map.put("k2", rowSetA);
    // 2 distinct keys
    assertEquals(2, map.entrySet().size());
  }

  @Test
  void entrySet_eachEntryHoldsCorrectRowSets() {
    map.put("k", rowSetA);
    map.put("k", rowSetB);
    map.entrySet()
        .forEach(
            e -> {
              assertEquals("k", e.getKey());
              assertEquals(2, e.getValue().size());
            });
  }

  // ---------------------------------------------------------------------------
  // non-String keys (the map is typed Object, used with pre-processed values)
  // ---------------------------------------------------------------------------

  @Test
  void put_integerKey_worksLikeStringKey() {
    map.put(42, rowSetA);
    assertTrue(map.containsKey(42));
    assertNotNull(map.get(42));
  }

  @Test
  void put_byteArrayHashCode_worksAsKey() {
    // SwitchCase converts byte[] via Arrays.hashCode before storing
    int hashCode = java.util.Arrays.hashCode(new byte[] {1, 2, 3});
    map.put(hashCode, rowSetA);
    assertTrue(map.containsKey(hashCode));
    assertEquals(rowSetA, map.get(hashCode).iterator().next());
  }
}
