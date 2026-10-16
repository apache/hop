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

package org.apache.hop.pipeline.transforms.fuzzymatch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link BoundedLevenshtein}. */
class BoundedLevenshteinTest {

  @Test
  void testKnownDistances() {
    BoundedLevenshtein levenshtein = new BoundedLevenshtein();
    assertEquals(0, levenshtein.distance("hop", "hop", 2));
    assertEquals(1, levenshtein.distance("hop", "hip", 2));
    assertEquals(2, levenshtein.distance("smith", "smyth2", 2));
    assertEquals(3, levenshtein.distance("kitten", "sitting", 3));
    assertEquals(Integer.MAX_VALUE, levenshtein.distance("kitten", "sitting", 2));
  }

  @Test
  void testEmptyStrings() {
    BoundedLevenshtein levenshtein = new BoundedLevenshtein();
    assertEquals(0, levenshtein.distance("", "", 0));
    assertEquals(2, levenshtein.distance("", "ab", 2));
    assertEquals(2, levenshtein.distance("ab", "", 5));
    assertEquals(Integer.MAX_VALUE, levenshtein.distance("", "abc", 2));
  }

  @Test
  void testNegativeMaximumMatchesNothing() {
    assertEquals(Integer.MAX_VALUE, new BoundedLevenshtein().distance("a", "a", -1));
  }

  @Test
  void testHugeMaximumDoesNotOverflow() {
    BoundedLevenshtein levenshtein = new BoundedLevenshtein();
    assertEquals(3, levenshtein.distance("kitten", "sitting", Integer.MAX_VALUE));
    assertEquals(7, levenshtein.distance("", "sitting", Integer.MAX_VALUE));
  }

  /** Same result as the unbounded commons-lang3 distance whenever that one is within the max. */
  @Test
  void testMatchesUnboundedDistance() {
    Random random = new Random(20261016L);
    BoundedLevenshtein levenshtein = new BoundedLevenshtein();
    String alphabet = "abcde";
    for (int run = 0; run < 20_000; run++) {
      String s = randomString(random, alphabet, random.nextInt(12));
      String t = randomString(random, alphabet, random.nextInt(12));
      int max = random.nextInt(8);
      int expected = StringUtils.getLevenshteinDistance(s, t);
      int actual = levenshtein.distance(s, t, max);
      assertEquals(
          expected <= max ? expected : Integer.MAX_VALUE,
          actual,
          () -> "distance(" + s + ", " + t + ", " + max + ")");
    }
  }

  private static String randomString(Random random, String alphabet, int length) {
    StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      builder.append(alphabet.charAt(random.nextInt(alphabet.length())));
    }
    return builder.toString();
  }
}
