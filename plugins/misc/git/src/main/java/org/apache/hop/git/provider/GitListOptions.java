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

import lombok.Getter;

@Getter
public class GitListOptions {

  public static final int DEFAULT_PAGE_SIZE = 50;
  public static final int DEFAULT_MAX_PAGES = 20;

  /** Max pages value asking for every page the provider will serve. */
  public static final int UNLIMITED_MAX_PAGES = 0;

  /** Row cap standing for "no cap": a local JGit walk, or an unlimited remote read. */
  public static final int UNLIMITED_MAX_RECORDS = Integer.MAX_VALUE;

  private final String state;
  private final String since;
  private final String branch;
  private final int pageSize;
  private final int maxPages;

  public GitListOptions(String state, String since, String branch, int pageSize, int maxPages) {
    this.state = state;
    this.since = since;
    this.branch = branch;
    this.pageSize = pageSize > 0 ? pageSize : DEFAULT_PAGE_SIZE;
    // 0 means every page; only a negative value is meaningless and falls back to the default.
    this.maxPages = maxPages < 0 ? DEFAULT_MAX_PAGES : maxPages;
  }

  /**
   * Upper bound on rows returned for one transform run, based on the page size the provider will
   * actually honour.
   *
   * <p>Providers cap how many items a single page may hold, so a configured page size above that
   * cap cannot be met. Deriving the row cap from the configured size instead would promise more
   * rows than {@code maxPages} requests can ever deliver, and the run would stop early without
   * explanation.
   *
   * <p>A {@code maxPages} of {@link #UNLIMITED_MAX_PAGES} lifts the cap altogether: the read then
   * stops only when the provider runs out of pages or applies a documented limit of its own.
   *
   * @param providerMaxPageSize the provider's per-page maximum, or {@code 0} when it has none
   */
  public int getMaxRecords(int providerMaxPageSize) {
    if (isUnlimited()) {
      return UNLIMITED_MAX_RECORDS;
    }
    // Widened before multiplying: a large page size and max pages overflow an int, and a negative
    // cap would stop the read on the very first row instead of raising the ceiling.
    long cap = (long) getEffectivePageSize(providerMaxPageSize) * maxPages;
    return cap >= UNLIMITED_MAX_RECORDS ? UNLIMITED_MAX_RECORDS : (int) cap;
  }

  /** Whether this run reads every page the provider will serve, rather than a fixed number. */
  public boolean isUnlimited() {
    return maxPages == UNLIMITED_MAX_PAGES;
  }

  /**
   * Whether page number {@code page} is still within the configured budget.
   *
   * <p>Page loaders must ask this rather than comparing against {@code getMaxPages()} directly: an
   * unlimited run stores 0, so {@code page > getMaxPages()} is true for the very first page and the
   * read would return nothing at all.
   *
   * @param page the 1-based page about to be requested
   */
  public boolean hasPageBudget(int page) {
    return isUnlimited() || page <= maxPages;
  }

  /** Page size capped to what the provider API allows per request. */
  public int getEffectivePageSize(int providerMaxPageSize) {
    if (providerMaxPageSize <= 0) {
      return pageSize;
    }
    return Math.min(pageSize, providerMaxPageSize);
  }

  public boolean isLastPage(int returnedCount, int providerMaxPageSize) {
    return returnedCount < getEffectivePageSize(providerMaxPageSize);
  }
}
