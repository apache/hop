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

import java.util.List;
import org.apache.hop.core.exception.HopException;

/**
 * Loads activity rows (comments or events) by paging through issues, then paging activity on each
 * issue.
 */
abstract class IssueNestedPageLoader implements GitResourcePageLoader {

  protected final GitListOptions options;

  private List<IssueRef> issueBuffer = List.of();
  private int issueIndex;
  private int issuePage = 1;
  private int activityPage = 1;
  private boolean issuesExhausted;
  private boolean exhausted;
  private String truncationNote;

  protected IssueNestedPageLoader(GitListOptions options) {
    this.options = options;
  }

  protected record IssueRef(String id, long number, String title, String state) {}

  /**
   * Reads one page of issues.
   *
   * <p>An implementation that drops entries while mapping - GitHub's issues feed carries pull
   * requests - must call {@link #issuesRunOut()} when the provider itself returned nothing, so an
   * all-filtered page is not mistaken for the end of the issues.
   */
  protected abstract List<IssueRef> fetchIssuePage(int page) throws HopException;

  /** Called by an implementation when the provider returned no issues at all. */
  protected void issuesRunOut() {
    issuesExhausted = true;
  }

  protected abstract List<GitResourceRecord> fetchActivityPage(IssueRef issue, int page)
      throws HopException;

  protected abstract int activityPageSize();

  protected abstract boolean issuesLimitedByMaxPages();

  @Override
  public List<GitResourceRecord> loadNextPage() throws HopException {
    if (exhausted) {
      return List.of();
    }

    while (true) {
      if (issueIndex >= issueBuffer.size()) {
        if (issuesExhausted || (issuesLimitedByMaxPages() && !options.hasPageBudget(issuePage))) {
          exhausted = true;
          return List.of();
        }
        try {
          issueBuffer = fetchIssuePage(issuePage++);
        } catch (GitProviderCapException e) {
          return stopAtProviderCap(e);
        }
        issueIndex = 0;
        activityPage = 1;
        if (issueBuffer.isEmpty()) {
          // An issues page can map to no issues while more pages remain: GitHub's feed carries
          // pull requests, which are filtered out, so a page of nothing but those is not the end
          // of the issues. Keep asking until a page comes back genuinely empty.
          if (issuesExhausted) {
            exhausted = true;
            return List.of();
          }
          continue;
        }
      }

      IssueRef issue = issueBuffer.get(issueIndex);
      List<GitResourceRecord> records;
      try {
        records = fetchActivityPage(issue, activityPage++);
      } catch (GitProviderCapException e) {
        // The cap applies to this issue's activity only; the remaining issues are still readable.
        truncationNote = e.getMessage();
        issueIndex++;
        activityPage = 1;
        continue;
      }
      if (records.isEmpty()) {
        issueIndex++;
        activityPage = 1;
        continue;
      }

      if (records.size() < activityPageSize()) {
        issueIndex++;
        activityPage = 1;
      }
      return records;
    }
  }

  @Override
  public boolean isExhausted() {
    return exhausted;
  }

  private List<GitResourceRecord> stopAtProviderCap(GitProviderCapException e) {
    exhausted = true;
    truncationNote = e.getMessage();
    return List.of();
  }

  @Override
  public String getTruncationNote() {
    return truncationNote;
  }
}
