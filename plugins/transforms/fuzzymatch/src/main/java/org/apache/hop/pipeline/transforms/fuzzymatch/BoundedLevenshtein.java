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

/**
 * Levenshtein distance that gives up as soon as the result can no longer be within a maximum. Fuzzy
 * match drops every distance above its maximal value, so only a band of {@code 2 * max + 1} cells
 * per row of the matrix is needed, and a row whose cells are all above the maximum ends the
 * calculation. The two matrix rows are kept between calls. Not thread-safe: one per transform copy.
 */
public class BoundedLevenshtein {
  private int[] previous = new int[0];
  private int[] current = new int[0];

  /**
   * @param s the first string
   * @param t the second string
   * @param max the largest distance of interest
   * @return the Levenshtein distance of s and t (counted in UTF-16 code units, like commons-lang3),
   *     or {@link Integer#MAX_VALUE} when it is larger than max
   */
  public int distance(CharSequence s, CharSequence t, int max) {
    if (max < 0) {
      return Integer.MAX_VALUE;
    }
    int n = s.length();
    int m = t.length();
    if (Math.abs(n - m) > max) {
      return Integer.MAX_VALUE;
    }
    if (n == 0 || m == 0) {
      // the other one is at most max long, checked above
      return Math.max(n, m);
    }

    // The distance is never above the longer length: cap the band so nothing overflows
    int limit = Math.min(max, Math.max(n, m));
    int tooFar = limit + 1;

    if (previous.length < m + 1) {
      previous = new int[m + 1];
      current = new int[m + 1];
    }
    int[] prev = previous;
    int[] curr = current;

    for (int j = 0; j <= m; j++) {
      prev[j] = j;
    }

    for (int i = 1; i <= n; i++) {
      char sc = s.charAt(i - 1);
      int from = Math.max(1, i - limit);
      int to = Math.min(m, i + limit);

      curr[0] = i;
      int rowMin = from == 1 ? i : tooFar;
      if (from > 1) {
        curr[from - 1] = tooFar;
      }
      for (int j = from; j <= to; j++) {
        int cost = sc == t.charAt(j - 1) ? 0 : 1;
        int value = Math.min(Math.min(curr[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
        if (value > tooFar) {
          value = tooFar;
        }
        curr[j] = value;
        if (value < rowMin) {
          rowMin = value;
        }
      }
      if (to < m) {
        curr[to + 1] = tooFar;
      }
      if (rowMin > limit) {
        return Integer.MAX_VALUE;
      }

      int[] swap = prev;
      prev = curr;
      curr = swap;
    }

    return prev[m] <= limit ? prev[m] : Integer.MAX_VALUE;
  }
}
