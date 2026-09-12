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
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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

/**
 * Behaviour of {@link GitHubResourceClient} against a stubbed GitHub API.
 *
 * <p>The cases that matter most are the pagination edges: an empty page ends the walk, a provider
 * cap ends it with an explanation, and a failed request must abort rather than be mistaken for the
 * end of the data.
 */
class GitHubResourceClientWireMockTest {

  /**
   * One server for the whole class, reset between tests. Starting and stopping a server per test
   * churns through dynamic ports, and a recycled port can collide with a pooled connection in the
   * shared {@link GitApiHttp} HTTP client.
   */
  private static WireMockServer wireMock;

  private GitHubResourceClient client;

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
    client = new GitHubResourceClient(wireMock.baseUrl(), GitAuth.forToken("test-token"));
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

  private void stubCommits(int page, String body) {
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/commits"))
            .withQueryParam("page", equalTo(String.valueOf(page)))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(body)));
  }

  private static String commit(String sha, String message, String author, String date) {
    return "{\"sha\":\""
        + sha
        + "\",\"html_url\":\"https://github.com/apache/hop/commit/"
        + sha
        + "\",\"commit\":{\"message\":\""
        + message
        + "\",\"author\":{\"name\":\""
        + author
        + "\",\"date\":\""
        + date
        + "\"}}}";
  }

  @Test
  void readsCommitsAcrossPagesUntilAnEmptyPage() throws Exception {
    stubCommits(1, "[" + commit("a1", "first", "Ada", "2026-05-01T12:00:00Z") + "]");
    stubCommits(2, "[]");

    List<GitResourceRecord> records =
        drain(client.openReader(GitResourceType.COMMITS, "apache", "hop", options(1, 10)));

    assertEquals(1, records.size());
    GitResourceRecord record = records.get(0);
    assertEquals("github", record.getProvider());
    assertEquals("commits", record.getEntityType());
    assertEquals("a1", record.getSha());
    assertEquals("first", record.getTitle());
    assertEquals("Ada", record.getAuthor());
  }

  @Test
  void emitsCreatedAtAsADateInTheOutputRow() throws Exception {
    stubCommits(1, "[" + commit("a1", "first", "Ada", "2026-05-01T12:00:00Z") + "]");
    stubCommits(2, "[]");

    List<GitResourceRecord> records =
        drain(client.openReader(GitResourceType.COMMITS, "apache", "hop", options(1, 10)));

    Object[] row = records.get(0).toRow(true);
    assertEquals(
        Date.from(Instant.parse("2026-05-01T12:00:00Z")),
        row[GitInputFields.FIELD_NAMES.length - 10]);
    assertEquals(Date.from(Instant.parse("2026-05-01T12:00:00Z")), row[9]);
    assertNull(row[10], "updated_at is absent for a commit");
  }

  @Test
  void multiLineCommitMessageBecomesASingleLineTitle() throws Exception {
    stubCommits(
        1, "[" + commit("a1", "subject line\\nbody line", "Ada", "2026-05-01T12:00:00Z") + "]");
    stubCommits(2, "[]");

    List<GitResourceRecord> records =
        drain(client.openReader(GitResourceType.COMMITS, "apache", "hop", options(1, 10)));

    assertEquals("subject line", records.get(0).getTitle());
    assertTrue(records.get(0).getBody().contains("body line"));
  }

  @Test
  void aFailedRequestMidPaginationAbortsInsteadOfTruncating() {
    stubCommits(1, "[" + commit("a1", "first", "Ada", "2026-05-01T12:00:00Z") + "]");
    // 504 is not retryable, so this exercises the pagination failure path without retry backoff.
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/commits"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(aResponse().withStatus(504)));

    Exception e =
        assertThrows(
            Exception.class,
            () ->
                drain(client.openReader(GitResourceType.COMMITS, "apache", "hop", options(1, 10))));

    // The pipeline must fail rather than report a successful run over a partial result set.
    assertTrue(e.getMessage().contains("504") || e.getMessage().contains("pagination"));
  }

  @Test
  void issuesPaginationFailureAbortsInsteadOfTruncating() {
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody(
                        "[{\"node_id\":\"n1\",\"number\":1,\"title\":\"one\",\"state\":\"open\"}]")));
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(aResponse().withStatus(504)));

    GitPaginationException e =
        assertThrows(
            GitPaginationException.class,
            () ->
                drain(client.openReader(GitResourceType.ISSUES, "apache", "hop", options(1, 10))));

    assertEquals(2, e.getPage());
    assertTrue(e.getMessage().contains("partial result set"));
  }

  @Test
  void providerPaginationCapStopsCleanlyAndIsReported() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody(
                        "[{\"node_id\":\"n1\",\"number\":1,\"title\":\"one\",\"state\":\"open\"}]")));
    // GitHub answers deep page-based pagination with 422; that is a documented cap, not a failure.
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(aResponse().withStatus(422)));

    GitResourceReader reader =
        client.openReader(GitResourceType.ISSUES, "apache", "hop", options(1, 10));
    List<GitResourceRecord> records = drain(reader);

    assertEquals(1, records.size());
    assertNotNull(reader.getTruncationNote());
    assertTrue(reader.getTruncationNote().contains("1,000 items"));
  }

  @Test
  void pullRequestsAreExcludedFromTheIssuesFeed() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody(
                        "["
                            + "{\"node_id\":\"n1\",\"number\":1,\"title\":\"a real issue\",\"state\":\"open\"},"
                            + "{\"node_id\":\"n2\",\"number\":2,\"title\":\"a pull request\",\"state\":\"open\","
                            + "\"pull_request\":{\"url\":\"https://api.github.com/pulls/2\"}}"
                            + "]")));
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(aResponse().withStatus(200).withBody("[]")));

    List<GitResourceRecord> records =
        drain(client.openReader(GitResourceType.ISSUES, "apache", "hop", options(2, 10)));

    assertEquals(1, records.size());
    assertEquals("a real issue", records.get(0).getTitle());
  }

  @Test
  void theRowCapReflectsTheProviderPageSizeLimit() throws Exception {
    // A configured page size of 500 is capped to GitHub's 100, so 100 x 2 pages is the real cap.
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues"))
            .willReturn(aResponse().withStatus(200).withBody(issuePage(100))));

    GitResourceReader reader =
        client.openReader(GitResourceType.ISSUES, "apache", "hop", options(500, 2));
    List<GitResourceRecord> records = drain(reader);

    assertEquals(200, records.size());
    assertNotNull(reader.getTruncationNote());
    assertTrue(reader.getTruncationNote().contains("row cap"));
  }

  @Test
  void sendsABearerTokenAndTheHopUserAgent() throws Exception {
    stubCommits(1, "[]");
    drain(client.openReader(GitResourceType.COMMITS, "apache", "hop", options(1, 1)));

    wireMock.verify(
        getRequestedFor(urlPathEqualTo("/repos/apache/hop/commits"))
            .withHeader("Authorization", equalTo("Bearer test-token"))
            .withHeader("User-Agent", equalTo("Apache-Hop-GitInput"))
            .withHeader("Accept", equalTo("application/vnd.github+json")));
  }

  @Test
  void reAnchoredCommitChunksDoNotRepeatTheCursorCommit() throws Exception {
    // Ten pages of one commit each fills a chunk; the walk then re-anchors on the oldest sha, and
    // GitHub replays that commit as the first entry of the new chunk.
    for (int page = 1; page <= 10; page++) {
      stubCommits(
          page, "[" + commit("sha" + page, "m" + page, "Ada", "2026-05-01T12:00:00Z") + "]");
    }
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/commits"))
            .withQueryParam("sha", equalTo("sha10"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody("[" + commit("sha10", "m10", "Ada", "2026-05-01T12:00:00Z") + "]")));

    List<GitResourceRecord> records =
        drain(client.openReader(GitResourceType.COMMITS, "apache", "hop", options(1, 100)));

    List<String> shas = records.stream().map(GitResourceRecord::getSha).toList();
    assertEquals(shas.size(), shas.stream().distinct().count(), "duplicate commits: " + shas);
  }

  private static String issuePage(int count) {
    StringBuilder body = new StringBuilder("[");
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        body.append(',');
      }
      body.append("{\"node_id\":\"n")
          .append(i)
          .append("\",\"number\":")
          .append(i)
          .append(",\"title\":\"issue ")
          .append(i)
          .append("\",\"state\":\"open\"}");
    }
    return body.append(']').toString();
  }
}
