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

import org.json.simple.JSONObject;
import org.junit.jupiter.api.Test;

class GitHubResourceClientTest {

  @Test
  void mapGithubMergedUsesMergedAtWhenMergedBooleanAbsent() {
    JSONObject listResponse = new JSONObject();
    listResponse.put("merged_at", "2026-05-01T12:00:00Z");
    listResponse.put("state", "closed");

    assertEquals("Y", GitHubResourceClient.mapGithubMerged(listResponse));
  }

  @Test
  void mapGithubMergedUsesExplicitBooleanWhenPresent() {
    JSONObject merged = new JSONObject();
    merged.put("merged", Boolean.TRUE);
    JSONObject notMerged = new JSONObject();
    notMerged.put("merged", Boolean.FALSE);
    notMerged.put("merged_at", "2026-05-01T12:00:00Z");

    assertEquals("Y", GitHubResourceClient.mapGithubMerged(merged));
    assertEquals("N", GitHubResourceClient.mapGithubMerged(notMerged));
  }

  @Test
  void mapGithubMergedIsNoWhenClosedButNotMerged() {
    JSONObject closedUnmerged = new JSONObject();
    closedUnmerged.put("state", "closed");
    closedUnmerged.put("closed_at", "2026-05-01T12:00:00Z");

    assertEquals("N", GitHubResourceClient.mapGithubMerged(closedUnmerged));
  }
}
