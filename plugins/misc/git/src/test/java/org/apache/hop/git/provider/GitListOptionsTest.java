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

package org.apache.hop.git.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** How the page size and max pages settings turn into a row cap. */
class GitListOptionsTest {

  private static final int GITHUB_MAX_PER_PAGE = 100;

  private GitListOptions options(int pageSize, int maxPages) {
    return new GitListOptions("all", null, null, pageSize, maxPages);
  }

  @Test
  void aPageCountCapsTheRowsAtPageSizeTimesPages() {
    assertEquals(500, options(50, 10).getMaxRecords(GITHUB_MAX_PER_PAGE));
    assertFalse(options(50, 10).isUnlimited());
  }

  @Test
  void zeroPagesMeansEveryPageTheProviderWillServe() {
    GitListOptions unlimited = options(50, GitListOptions.UNLIMITED_MAX_PAGES);

    assertTrue(unlimited.isUnlimited());
    assertEquals(
        GitListOptions.UNLIMITED_MAX_RECORDS, unlimited.getMaxRecords(GITHUB_MAX_PER_PAGE));
  }

  @Test
  void zeroPagesDoesNotChangeTheRequestedPageSize() {
    // Page size still controls how many rows come back per call, only the number of calls is open.
    assertEquals(
        25,
        options(25, GitListOptions.UNLIMITED_MAX_PAGES).getEffectivePageSize(GITHUB_MAX_PER_PAGE));
  }

  @Test
  void zeroPagesLeavesEveryPageWithinBudget() {
    GitListOptions unlimited = options(50, GitListOptions.UNLIMITED_MAX_PAGES);

    // Regression: comparing page > getMaxPages() directly made page 1 out of budget when the
    // stored value is 0, so an unlimited read returned no rows at all.
    assertTrue(unlimited.hasPageBudget(1));
    assertTrue(unlimited.hasPageBudget(9999));
  }

  @Test
  void aPageCountBudgetsExactlyThatManyPages() {
    GitListOptions three = options(50, 3);

    assertTrue(three.hasPageBudget(1));
    assertTrue(three.hasPageBudget(3));
    assertFalse(three.hasPageBudget(4));
  }

  @Test
  void aNegativePageCountFallsBackToTheDefaultRatherThanReadingEverything() {
    GitListOptions negative = options(50, -5);

    assertFalse(negative.isUnlimited());
    assertEquals(GitListOptions.DEFAULT_MAX_PAGES, negative.getMaxPages());
  }

  @Test
  void theRowCapIsDerivedFromThePageSizeTheProviderWillHonour() {
    // Asking for 500 per page cannot yield more than the provider's 100 per page x 10 pages.
    assertEquals(1000, options(500, 10).getMaxRecords(GITHUB_MAX_PER_PAGE));
  }

  @Test
  void anEnormousPageCountClampsInsteadOfOverflowingToANegativeCap() {
    // (int) 100 * 25_000_000 overflows and would otherwise stop the read on the first row.
    int cap = options(100, 25_000_000).getMaxRecords(GITHUB_MAX_PER_PAGE);

    assertTrue(cap > 0, "row cap overflowed to " + cap);
    assertEquals(GitListOptions.UNLIMITED_MAX_RECORDS, cap);
  }
}
