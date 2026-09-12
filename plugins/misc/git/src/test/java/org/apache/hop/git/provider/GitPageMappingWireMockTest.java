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
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** How a page that maps to no rows, and how nested payload fields, are handled. */
class GitPageMappingWireMockTest {

  private static WireMockServer wireMock;

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
  void reset() {
    wireMock.resetAll();
  }

  private List<GitResourceRecord> drain(GitResourceReader reader) throws Exception {
    List<GitResourceRecord> records = new ArrayList<>();
    while (reader.hasNext()) {
      records.add(reader.next());
    }
    return records;
  }

  private static String issue(int number, String title) {
    return "{\"id\":\"i"
        + number
        + "\",\"number\":"
        + number
        + ",\"title\":\""
        + title
        + "\",\"state\":\"open\",\"user\":{\"login\":\"ada\"},"
        + "\"created_at\":\"2026-05-01T12:00:00Z\",\"html_url\":\"u\",\"body\":\"b\"}";
  }

  /** A pull request in the issues feed: filtered out, so a page of these maps to nothing. */
  private static String pullRequestInIssuesFeed(int number) {
    return "{\"id\":\"p"
        + number
        + "\",\"number\":"
        + number
        + ",\"title\":\"pr\","
        + "\"state\":\"open\",\"user\":{\"login\":\"ada\"},\"pull_request\":{\"url\":\"x\"},"
        + "\"created_at\":\"2026-05-01T12:00:00Z\",\"html_url\":\"u\",\"body\":\"b\"}";
  }

  /**
   * Regression: GitHub's issues feed carries pull requests too and the mapper drops them. A page
   * that is entirely pull requests used to end the read, silently reporting success over a
   * truncated result set.
   */
  @Test
  @Timeout(30)
  void aGitHubIssuesPageOfOnlyPullRequestsDoesNotEndTheRead() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "["
                            + pullRequestInIssuesFeed(1)
                            + ","
                            + pullRequestInIssuesFeed(2)
                            + "]")));
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("[" + issue(3, "a real issue") + "," + issue(4, "another") + "]")));
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues"))
            .withQueryParam("page", equalTo("3"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("[]")));

    GitHubResourceClient client =
        new GitHubResourceClient(wireMock.baseUrl(), GitAuth.forToken("t"));
    List<GitResourceRecord> records =
        drain(
            client.openReader(
                GitResourceType.ISSUES,
                "apache",
                "hop",
                new GitListOptions("all", null, null, 2, 5)));

    assertEquals(
        List.of("a real issue", "another"),
        records.stream().map(GitResourceRecord::getTitle).toList(),
        "the issues behind the filtered page must still be read");
  }

  @Test
  @Timeout(30)
  void aGiteaPageThatMapsToNothingDoesNotEndTheRead() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/putki-io/putki-test/issues"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("[" + pullRequestInIssuesFeed(1) + "]")));
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/putki-io/putki-test/issues"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("[" + issue(2, "gitea issue") + "]")));
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/putki-io/putki-test/issues"))
            .withQueryParam("page", equalTo("3"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("[]")));

    GiteaResourceClient client = new GiteaResourceClient(wireMock.baseUrl(), GitAuth.forToken("t"));
    List<GitResourceRecord> records =
        drain(
            client.openReader(
                GitResourceType.ISSUES,
                "putki-io",
                "putki-test",
                new GitListOptions("all", null, null, 1, 5)));

    assertTrue(
        records.stream().anyMatch(r -> "gitea issue".equals(r.getTitle())),
        "expected the issue on page 2, got "
            + records.stream().map(GitResourceRecord::getTitle).toList());
  }

  /** Gitea and Forgejo nest pull request branches under head.ref / base.ref. */
  @Test
  @Timeout(30)
  void giteaPullRequestBranchesComeFromTheNestedRefs() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/putki-io/putki-test/pulls"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "[{\"id\":\"1\",\"number\":7,\"title\":\"a pr\",\"state\":\"open\","
                            + "\"user\":{\"login\":\"ada\"},\"created_at\":\"2026-05-01T12:00:00Z\","
                            + "\"html_url\":\"u\",\"body\":\"b\","
                            + "\"head\":{\"ref\":\"docs-url-helper\"},"
                            + "\"base\":{\"ref\":\"forgejo\"}}]")));
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/putki-io/putki-test/pulls"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("[]")));

    GiteaResourceClient client = new GiteaResourceClient(wireMock.baseUrl(), GitAuth.forToken("t"));
    List<GitResourceRecord> records =
        drain(
            client.openReader(
                GitResourceType.PULL_REQUESTS,
                "putki-io",
                "putki-test",
                new GitListOptions("all", null, null, 1, 5)));

    assertEquals(1, records.size());
    assertEquals("docs-url-helper", records.get(0).getSourceBranch());
    assertEquals("forgejo", records.get(0).getTargetBranch());
  }
}
