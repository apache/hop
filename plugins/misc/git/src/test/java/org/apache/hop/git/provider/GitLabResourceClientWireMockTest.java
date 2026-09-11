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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Behaviour of {@link GitLabResourceClient} against a stubbed GitLab API. */
class GitLabResourceClientWireMockTest {

  /**
   * One server for the whole class, reset between tests. Starting and stopping a server per test
   * churns through dynamic ports, and a recycled port can collide with a pooled connection in the
   * shared {@link GitApiHttp} HTTP client.
   */
  private static WireMockServer wireMock;

  private GitLabResourceClient client;

  @BeforeAll
  static void startServer() {
    wireMock = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    wireMock.start();
  }

  @AfterAll
  static void stopServer() {
    if (wireMock != null) {
      wireMock.stop();
      wireMock = null;
    }
  }

  @BeforeEach
  void setUp() {
    wireMock.resetAll();
    client = new GitLabResourceClient(wireMock.baseUrl(), GitAuth.forToken("glpat-test"));
  }

  private static GitListOptions options(int pageSize, int maxPages) {
    return new GitListOptions("all", null, null, pageSize, maxPages);
  }

  private static List<GitResourceRecord> drain(GitResourceReader reader) throws Exception {
    List<GitResourceRecord> records = new ArrayList<>();
    while (reader.hasNext()) {
      records.add(reader.next());
    }
    return records;
  }

  @Test
  void readsCommitsAndMapsTheGitLabFieldNames() throws Exception {
    wireMock.stubFor(
        get(urlPathMatching("/projects/.*/repository/commits"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody(
                        "[{\"id\":\"abc123\",\"title\":\"fix things\","
                            + "\"message\":\"fix things\\n\\nwith detail\","
                            + "\"author_name\":\"Ada\","
                            + "\"created_at\":\"2026-05-01T12:00:00.000Z\","
                            + "\"web_url\":\"https://gitlab.com/apache/hop/-/commit/abc123\"}]")));
    wireMock.stubFor(
        get(urlPathMatching("/projects/.*/repository/commits"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(aResponse().withStatus(200).withBody("[]")));

    List<GitResourceRecord> records =
        drain(client.openReader(GitResourceType.COMMITS, "apache", "hop", options(1, 10)));

    assertEquals(1, records.size());
    GitResourceRecord record = records.get(0);
    assertEquals("gitlab", record.getProvider());
    assertEquals("abc123", record.getSha());
    assertEquals("Ada", record.getAuthor());

    // GitLab's millisecond form still has to reach the row as a real Date.
    Object[] row = record.toRow(true);
    int createdAt = List.of(GitInputFields.FIELD_NAMES).indexOf("created_at");
    assertEquals(Date.from(Instant.parse("2026-05-01T12:00:00Z")), row[createdAt]);
  }

  @Test
  void mergeRequestsMapToPullRequestRows() throws Exception {
    wireMock.stubFor(
        get(urlPathMatching("/projects/.*/merge_requests"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody(
                        "[{\"id\":9,\"iid\":42,\"title\":\"a merge request\",\"state\":\"merged\","
                            + "\"author\":{\"username\":\"ada\"},"
                            + "\"created_at\":\"2026-05-01T12:00:00.000Z\","
                            + "\"source_branch\":\"feature\",\"target_branch\":\"main\"}]")));
    wireMock.stubFor(
        get(urlPathMatching("/projects/.*/merge_requests"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(aResponse().withStatus(200).withBody("[]")));

    List<GitResourceRecord> records =
        drain(client.openReader(GitResourceType.PULL_REQUESTS, "apache", "hop", options(1, 10)));

    assertEquals(1, records.size());
    GitResourceRecord record = records.get(0);
    assertEquals("pull_requests", record.getEntityType());
    assertEquals(42L, record.getNumber(), "GitLab numbers rows by iid, not id");
    assertEquals("feature", record.getSourceBranch());
    assertEquals("main", record.getTargetBranch());
    assertEquals("Y", record.getMerged());
  }

  @Test
  void aFailedRequestMidPaginationAbortsInsteadOfTruncating() {
    wireMock.stubFor(
        get(urlPathMatching("/projects/.*/issues"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody("[{\"id\":1,\"iid\":1,\"title\":\"one\",\"state\":\"opened\"}]")));
    wireMock.stubFor(
        get(urlPathMatching("/projects/.*/issues"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(aResponse().withStatus(504)));

    GitPaginationException e =
        assertThrows(
            GitPaginationException.class,
            () ->
                drain(client.openReader(GitResourceType.ISSUES, "apache", "hop", options(1, 10))));

    assertTrue(e.getMessage().contains("GitLab pagination failed at page 2"));
  }

  @Test
  void sendsThePrivateTokenHeader() throws Exception {
    wireMock.stubFor(
        get(urlPathMatching("/projects/.*/repository/commits"))
            .willReturn(aResponse().withStatus(200).withBody("[]")));

    drain(client.openReader(GitResourceType.COMMITS, "apache", "hop", options(1, 1)));

    wireMock.verify(
        getRequestedFor(urlPathMatching("/projects/.*/repository/commits"))
            .withHeader("PRIVATE-TOKEN", equalTo("glpat-test")));
  }
}
