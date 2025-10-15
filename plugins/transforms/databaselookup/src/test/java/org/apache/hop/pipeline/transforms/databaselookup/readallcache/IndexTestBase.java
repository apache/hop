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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This is a base class for several similar cases. All of them are checking how indexes work with
 * the same tuple of data: [0, 1, 2, 2, 3]. Since the data set is known, each subclass show
 * implement tests for the following values:
 *
 * <ul>
 *   <li>-1
 *   <li>0
 *   <li>1
 *   <li>2
 *   <li>3
 *   <li>100
 * </ul>
 */
public abstract class IndexTestBase<T extends Index> {

  private static Long[][] toMatrix(long... values) {
    Long[][] result = new Long[values.length][];
    for (int i = 0; i < values.length; i++) {
      result[i] = new Long[] {values[i]};
    }
    return result;
  }

  static List<Object[]> createSampleData() {
    // sorted, reversely sorted, and shuffled data
    return Arrays.asList(
        new Object[] {toMatrix(0, 1, 2, 2, 3)},
        new Object[] {toMatrix(3, 2, 2, 1, 0)},
        new Object[] {toMatrix(1, 3, 2, 0, 2)});
  }

  final Long[][] rows;
  private final Class<T> clazz;

  T index;
  SearchingContext context;

  public IndexTestBase(Class<T> clazz, Long[][] rows) {
    this.rows = rows;
    this.clazz = clazz;
  }

  @BeforeEach
  void setUp() throws Exception {
    index = createIndexInstance(0, new ValueMetaInteger(), 5);
    index.performIndexingOf(rows);

    context = new SearchingContext();
    context.init(5);
  }

  T createIndexInstance(int column, IValueMeta meta, int rowsAmount) throws Exception {
    return clazz
        .getDeclaredConstructor(int.class, IValueMeta.class, int.class)
        .newInstance(column, meta, rowsAmount);
  }

  @AfterEach
  void tearDown() {
    index = null;
    context = null;
  }

  void testFindsNothing(long value) {
    assertFalse(context.isEmpty());
    index.applyRestrictionsTo(context, new ValueMetaInteger(), value);
    assertTrue(context.isEmpty(), "Expected not to find anything matching " + value);
  }

  void testFindsCorrectly(long lookupValue, int expectedAmount) {
    assertFalse(context.isEmpty());
    index.applyRestrictionsTo(context, new ValueMetaInteger(), lookupValue);

    assertFalse(context.isEmpty(), "Expected to find something");

    BitSet actual = context.getCandidates();
    int cnt = expectedAmount;
    int lastSetBit = 0;
    while (cnt > 0) {
      lastSetBit = actual.nextSetBit(lastSetBit);
      if (lastSetBit < 0) {
        fail("Expected to find " + expectedAmount + " values, but got: " + actual.toString());
      }

      doAssertMatches(actual, lookupValue, rows[lastSetBit][0]);

      lastSetBit++;
      cnt--;
    }
  }

  abstract void doAssertMatches(BitSet candidates, long lookupValue, long actualValue);

  @Test
  abstract void lookupFor_MinusOne();

  @Test
  abstract void lookupFor_Zero();

  @Test
  abstract void lookupFor_One();

  @Test
  abstract void lookupFor_Two();

  @Test
  abstract void lookupFor_Three();

  @Test
  abstract void lookupFor_Hundred();
}
