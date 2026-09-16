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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Per-provider payload and protocol quirks that generic handling gets wrong. */
class GitProviderQuirksWireMockTest {

  private static WireMockServer wireMock;

  @BeforeAll
  static void startServer() throws Exception {
    // Encr decodes the optionally encrypted credential in GitConnection.
    org.apache.hop.core.HopClientEnvironment.init();
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

  private static String json(String body) {
    return body;
  }

  private static String commits(String... shas) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < shas.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"sha\":\"")
          .append(shas[i])
          .append("\",\"html_url\":\"u\",\"commit\":{\"message\":\"m\",")
          .append("\"author\":{\"name\":\"ada\",\"date\":\"2026-05-01T12:00:00Z\"}}}");
    }
    return sb.append(']').toString();
  }

  /** Bitbucket wraps every paginated result in {page, size, values: [...]}, never a bare array. */
  @Test
  @Timeout(30)
  void bitbucketWorkspacesAreReadFromThePaginationEnvelope() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/workspaces"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        json(
                            "{\"page\":1,\"pagelen\":100,\"size\":1,\"values\":["
                                + "{\"slug\":\"putki-io\",\"name\":\"Putki\"}]}"))));

    List<GitOrganizationInfo> orgs =
        new BitbucketRepositoryBrowser()
            .listOrganizations(wireMock.baseUrl(), GitAuth.forBasic("ada", "app-pass"));

    assertEquals(List.of("putki-io"), orgs.stream().map(GitOrganizationInfo::getSlug).toList());
  }

  /**
   * Bitbucket has no CLOSED or "all" pull request state and does not reject an unknown one - it
   * silently returns everything, so asking for closed used to include the open ones.
   */
  @Test
  @Timeout(30)
  void bitbucketClosedPullRequestsAskForTheStatesThatActuallyExist() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repositories/putki-io/putki-test/pullrequests"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(json("{\"values\":[]}"))));

    BitbucketResourceClient client =
        new BitbucketResourceClient(wireMock.baseUrl(), GitAuth.forBasic("ada", "app-pass"));
    GitResourceReader reader =
        client.openReader(
            GitResourceType.PULL_REQUESTS,
            "putki-io",
            "putki-test",
            new GitListOptions("closed", null, null, 10, 1));
    while (reader.hasNext()) {
      reader.next();
    }

    wireMock.verify(
        getRequestedFor(urlPathEqualTo("/repositories/putki-io/putki-test/pullrequests"))
            .withQueryParam("state", equalTo("MERGED")));
    String requested = wireMock.getAllServeEvents().get(0).getRequest().getUrl();
    assertTrue(requested.contains("state=MERGED"), requested);
    assertTrue(requested.contains("state=DECLINED"), requested);
    assertFalse(requested.contains("state=CLOSED"), requested);
    assertFalse(requested.contains("state=OPEN"), requested);
  }

  /** GitHub answers a spent primary rate limit with 403, so it must be waited out, not failed. */
  @Test
  @Timeout(30)
  void aRateLimited403IsRetriedRatherThanReportedAsAScopeProblem() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/thing"))
            .inScenario("ratelimit")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(
                aResponse()
                    .withStatus(403)
                    .withHeader("X-RateLimit-Remaining", "0")
                    .withHeader("Retry-After", "0")
                    .withBody("{\"message\":\"API rate limit exceeded\"}"))
            .willSetStateTo("recovered"));
    wireMock.stubFor(
        get(urlPathEqualTo("/thing"))
            .inScenario("ratelimit")
            .whenScenarioStateIs("recovered")
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{}")));

    String body =
        GitApiHttp.get(
            wireMock.baseUrl() + "/thing",
            GitAuth.forToken("t"),
            GitProvider.AuthStyle.BEARER,
            "application/json",
            "GitHub");

    assertEquals("{}", body);
  }

  /** A 403 without rate limit headers is still a credential problem and must fail at once. */
  @Test
  @Timeout(30)
  void aPlain403StillFailsImmediately() {
    wireMock.stubFor(
        get(urlPathEqualTo("/thing"))
            .willReturn(aResponse().withStatus(403).withBody("{\"message\":\"Forbidden\"}")));

    HopException e =
        assertThrows(
            HopException.class,
            () ->
                GitApiHttp.get(
                    wireMock.baseUrl() + "/thing",
                    GitAuth.forToken("t"),
                    GitProvider.AuthStyle.BEARER,
                    "application/json",
                    "GitHub"));
    assertTrue(e.getMessage().contains("access denied"), e.getMessage());
  }

  /** GitHub's issues feed carries pull requests; an events run must not report them as issues. */
  @Test
  @Timeout(30)
  void gitHubIssueEventsSkipPullRequests() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "[{\"node_id\":\"p1\",\"number\":1,\"title\":\"a pr\",\"state\":\"open\","
                            + "\"pull_request\":{\"url\":\"x\"}},"
                            + "{\"node_id\":\"i2\",\"number\":2,\"title\":\"an issue\","
                            + "\"state\":\"open\"}]")));
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("[]")));
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues/2/events"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "[{\"id\":9,\"event\":\"closed\",\"actor\":{\"login\":\"ada\"},"
                            + "\"created_at\":\"2026-05-01T12:00:00Z\"}]")));

    GitHubResourceClient client =
        new GitHubResourceClient(wireMock.baseUrl(), GitAuth.forToken("t"));
    GitResourceReader reader =
        client.openReader(
            GitResourceType.ISSUE_EVENTS,
            "apache",
            "hop",
            new GitListOptions("all", null, null, 2, 2));
    List<GitResourceRecord> records = new ArrayList<>();
    while (reader.hasNext()) {
      records.add(reader.next());
    }

    assertTrue(
        records.stream().allMatch(r -> r.getNumber() == 2L),
        "pull request events leaked in: "
            + records.stream().map(GitResourceRecord::getNumber).toList());
    wireMock.verify(0, getRequestedFor(urlPathEqualTo("/repos/apache/hop/issues/1/events")));
  }

  /**
   * Some regions of a large repository's history make GitHub answer 500 for a given anchor and page
   * size, reproducibly and on the upstream repository too. The same anchor succeeds with a smaller
   * window, so a long commit read must narrow rather than abandon what it has.
   */
  @Test
  @Timeout(30)
  void aCommitWindowThatGitHubCannotWalkIsNarrowedRatherThanFailingTheRun() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/commits"))
            .withQueryParam("per_page", equalTo("50"))
            .willReturn(aResponse().withStatus(500).withBody("{\"message\":\"Server Error\"}")));
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/commits"))
            .withQueryParam("per_page", equalTo("25"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "[{\"sha\":\"aaa1\",\"html_url\":\"u\",\"commit\":{\"message\":\"m\","
                            + "\"author\":{\"name\":\"ada\",\"date\":\"2026-05-01T12:00:00Z\"}}}]")));

    GitHubResourceClient client =
        new GitHubResourceClient(wireMock.baseUrl(), GitAuth.forToken("t"));
    GitResourceReader reader =
        client.openReader(
            GitResourceType.COMMITS, "apache", "hop", new GitListOptions("all", null, null, 50, 2));

    List<GitResourceRecord> records = new ArrayList<>();
    while (reader.hasNext()) {
      records.add(reader.next());
    }

    assertEquals(List.of("aaa1"), records.stream().map(GitResourceRecord::getSha).toList());
    wireMock.verify(
        getRequestedFor(urlPathEqualTo("/repos/apache/hop/commits"))
            .withQueryParam("per_page", equalTo("25")));
  }

  /**
   * The real failure: GitHub walks history fine from a fresh anchor but fails on a deeper page from
   * that same anchor, so narrowing the window only moves the failure a few pages along. Recovery is
   * to re-anchor on the newest commit already read and start again at page 1.
   */
  @Test
  @Timeout(30)
  void aPageGitHubCannotTraverseIsRecoveredByReAnchoring() throws Exception {
    String path = "/repos/apache/hop/commits";
    // page 1 from the initial anchor works
    wireMock.stubFor(
        get(urlPathEqualTo(path))
            .withQueryParam("page", equalTo("1"))
            .withQueryParam("sha", equalTo("HEAD"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(commits("c1", "c2"))));
    // any deeper page from that anchor is the region GitHub cannot traverse
    wireMock.stubFor(
        get(urlPathEqualTo(path))
            .withQueryParam("page", equalTo("2"))
            .withQueryParam("sha", equalTo("HEAD"))
            .willReturn(aResponse().withStatus(500).withBody("{\"message\":\"Server Error\"}")));
    // re-anchored on the last commit read, page 1 works again and finishes the history
    wireMock.stubFor(
        get(urlPathEqualTo(path))
            .withQueryParam("page", equalTo("1"))
            .withQueryParam("sha", equalTo("c2"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(commits("c2", "c3"))));
    wireMock.stubFor(
        get(urlPathEqualTo(path))
            .withQueryParam("page", equalTo("2"))
            .withQueryParam("sha", equalTo("c2"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("[]")));

    GitHubResourceClient client =
        new GitHubResourceClient(wireMock.baseUrl(), GitAuth.forToken("t"));
    GitResourceReader reader =
        client.openReader(
            GitResourceType.COMMITS,
            "apache",
            "hop",
            new GitListOptions("all", null, "HEAD", 2, 20));

    List<GitResourceRecord> records = new ArrayList<>();
    while (reader.hasNext()) {
      records.add(reader.next());
    }

    // c2 is the re-anchor point and must not be emitted twice.
    assertEquals(
        List.of("c1", "c2", "c3"), records.stream().map(GitResourceRecord::getSha).toList());
  }

  /** A 500 that persists even at the smallest window must still fail, not silently truncate. */
  @Test
  @Timeout(30)
  void aCommitReadThatKeepsFailingAtTheSmallestWindowStillFails() {
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/commits"))
            .willReturn(aResponse().withStatus(500).withBody("{\"message\":\"Server Error\"}")));

    GitHubResourceClient client =
        new GitHubResourceClient(wireMock.baseUrl(), GitAuth.forToken("t"));

    assertThrows(
        HopException.class,
        () -> {
          GitResourceReader reader =
              client.openReader(
                  GitResourceType.COMMITS,
                  "apache",
                  "hop",
                  new GitListOptions("all", null, null, 50, 2));
          while (reader.hasNext()) {
            reader.next();
          }
        });
  }

  private static String commentFeed() {
    // GitHub's repository-wide comment feed carries both kinds. Only the comment's own URL tells
    // them apart - issue_url says /issues/ for a pull request comment too.
    return "[{\"id\":\"1\",\"issue_url\":\"https://api.github.com/repos/apache/hop/issues/7\","
        + "\"html_url\":\"https://github.com/apache/hop/pull/7#issuecomment-1\","
        + "\"user\":{\"login\":\"ada\"},\"created_at\":\"2026-05-01T12:00:00Z\",\"body\":\"on a pr\"},"
        + "{\"id\":\"2\",\"issue_url\":\"https://api.github.com/repos/apache/hop/issues/8\","
        + "\"html_url\":\"https://github.com/apache/hop/issues/8#issuecomment-2\","
        + "\"user\":{\"login\":\"bob\"},\"created_at\":\"2026-05-01T12:00:00Z\",\"body\":\"on an issue\"}]";
  }

  private List<GitResourceRecord> readComments(GitResourceType type) throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues/comments"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(commentFeed())));
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/apache/hop/issues/comments"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("[]")));

    GitHubResourceClient client =
        new GitHubResourceClient(wireMock.baseUrl(), GitAuth.forToken("t"));
    GitResourceReader reader =
        client.openReader(type, "apache", "hop", new GitListOptions("all", null, null, 2, 5));
    List<GitResourceRecord> records = new ArrayList<>();
    while (reader.hasNext()) {
      records.add(reader.next());
    }
    return records;
  }

  @Test
  @Timeout(30)
  void issueCommentsExcludePullRequestComments() throws Exception {
    List<GitResourceRecord> records = readComments(GitResourceType.ISSUE_COMMENTS);

    assertEquals(List.of("on an issue"), records.stream().map(GitResourceRecord::getBody).toList());
    assertEquals("issue_comments", records.get(0).getEntityType());
  }

  @Test
  @Timeout(30)
  void prCommentsReturnOnlyThePullRequestSideOfTheSameFeed() throws Exception {
    List<GitResourceRecord> records = readComments(GitResourceType.PR_COMMENTS);

    assertEquals(List.of("on a pr"), records.stream().map(GitResourceRecord::getBody).toList());
    assertEquals("pr_comments", records.get(0).getEntityType());
  }

  /** A credential must never reach a log, even when it is the thing being complained about. */
  @Test
  void anUnresolvedVariableErrorDoesNotQuoteTheSecret() {
    GitConnection connection = new GitConnection();
    connection.setProvider(GitProvider.GITHUB_CLOUD);
    connection.setAuthType(GitAuthType.TOKEN);
    connection.setToken("${NOT_SET_ANYWHERE}");

    HopException e =
        assertThrows(
            HopException.class,
            () -> connection.toAuth(new org.apache.hop.core.variables.Variables()));
    assertFalse(e.getMessage().contains("NOT_SET_ANYWHERE"), e.getMessage());
    assertTrue(e.getMessage().contains("unresolved variable"), e.getMessage());
  }
}
