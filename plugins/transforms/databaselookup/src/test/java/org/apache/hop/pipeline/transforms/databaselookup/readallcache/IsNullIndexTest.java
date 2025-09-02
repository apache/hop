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

package org.apache.hop.pipeline.transforms.databaselookup.readallcache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IsNullIndexTest {

  static List<Object[]> createSampleData() {
    Long[][] sample1 = new Long[][] {{null}, {null}, {1L}, {null}, {null}};
    Long[][] sample2 = new Long[][] {{1L}, {null}, {null}, {1L}, {1L}};
    Long[][] sample3 = new Long[][] {{null}, {1L}, {null}, {1L}, {null}};
    Long[][] sample4 = new Long[][] {{null}, {null}, {null}, {null}, {null}};
    Long[][] sample5 = new Long[][] {{1L}, {1L}, {1L}, {1L}, {1L}};
    return Arrays.asList(
        new Object[] {sample1},
        new Object[] {sample2},
        new Object[] {sample3},
        new Object[] {sample4},
        new Object[] {sample5});
  }

  private final Long[][] rows;
  private final int amountOfNulls;

  private IsNullIndex matchingNulls;
  private IsNullIndex matchingNonNulls;
  private SearchingContext context;

  public IsNullIndexTest() {
    // Default constructor for JUnit 5 - use sample data
    this.rows = new Long[][] {{null}, {null}, {1L}, {null}, {null}};

    int cnt = 0;
    for (Long[] row : rows) {
      for (Long value : row) {
        if (value == null) {
          cnt++;
        }
      }
    }
    this.amountOfNulls = cnt;
  }

  @BeforeEach
  void setUp() {
    matchingNulls = new IsNullIndex(0, new ValueMetaInteger(), 5, true);
    matchingNulls.performIndexingOf(rows);

    matchingNonNulls = new IsNullIndex(0, new ValueMetaInteger(), 5, false);
    matchingNonNulls.performIndexingOf(rows);

    context = new SearchingContext();
    context.init(5);
  }

  @AfterEach
  void tearDown() {
    matchingNulls = null;
    matchingNonNulls = null;
    context = null;
  }

  @Test
  void lookupFor_Null() {
    testFindsCorrectly(matchingNulls, true);
  }

  @Test
  void lookupFor_One() {
    testFindsCorrectly(matchingNonNulls, false);
  }

  private void testFindsCorrectly(IsNullIndex index, boolean isLookingForNull) {
    final int expectedAmount = isLookingForNull ? amountOfNulls : 5 - amountOfNulls;

    assertFalse(context.isEmpty());
    // the index ignores lookup value - pass null there
    index.applyRestrictionsTo(context, new ValueMetaInteger(), null);

    if (expectedAmount == 0) {
      assertTrue(context.isEmpty());
      return;
    }

    assertFalse(context.isEmpty(), String.format("Expected to find %d values", expectedAmount));

    BitSet actual = context.getCandidates();
    int cnt = expectedAmount;
    int lastSetBit = 0;
    while (cnt > 0) {
      lastSetBit = actual.nextSetBit(lastSetBit);
      if (lastSetBit < 0) {
        fail("Expected to find " + expectedAmount + " values, but got: " + actual);
      }

      Long actualValue = rows[lastSetBit][0];
      if (isLookingForNull) {
        assertNull(actualValue);
      } else {
        assertEquals(Long.valueOf(1), actualValue);
      }

      lastSetBit++;
      cnt--;
    }
  }
}
