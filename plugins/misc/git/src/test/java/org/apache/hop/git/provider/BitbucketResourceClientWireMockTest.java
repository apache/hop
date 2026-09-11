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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behaviour of {@link BitbucketResourceClient} against a stubbed Bitbucket API. Bitbucket differs
 * from the other providers in two ways worth pinning down: it wraps every list in a {@code values}
 * envelope, and it authenticates with a username and app password rather than a token.
 */
class BitbucketResourceClientWireMockTest {

  /**
   * One server for the whole class, reset between tests. Starting and stopping a server per test
   * churns through dynamic ports, and a recycled port can collide with a pooled connection in the
   * shared {@link GitApiHttp} HTTP client.
   */
  private static WireMockServer wireMock;

  private BitbucketResourceClient client;

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
    client = new BitbucketResourceClient(wireMock.baseUrl(), GitAuth.forBasic("ada", "app-pass"));
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

  private static String envelope(String values) {
    return "{\"values\":[" + values + "]}";
  }

  /** Bitbucket advertises another page with a {@code next} link rather than by filling the page. */
  private static String envelopeWithNext(String values) {
    return "{\"next\":\"http://example.invalid/next\",\"values\":[" + values + "]}";
  }

  @Test
  void readsCommitsFromTheValuesEnvelope() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repositories/team/repo/commits"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody(
                        envelopeWithNext(
                            "{\"hash\":\"deadbeef\",\"message\":\"a commit\\n\\nwith a body\","
                                + "\"date\":\"2026-05-01T12:00:00.000000+00:00\","
                                + "\"author\":{\"user\":{\"display_name\":\"Ada Lovelace\"}},"
                                + "\"links\":{\"html\":{\"href\":\"https://bitbucket.org/team/repo/commits/deadbeef\"}}}"))));
    wireMock.stubFor(
        get(urlPathEqualTo("/repositories/team/repo/commits"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(aResponse().withStatus(200).withBody(envelope(""))));

    List<GitResourceRecord> records =
        drain(client.openReader(GitResourceType.COMMITS, "team", "repo", options(1, 10)));

    assertEquals(1, records.size());
    GitResourceRecord record = records.get(0);
    assertEquals("bitbucket", record.getProvider());
    assertEquals("deadbeef", record.getSha());
    assertEquals("a commit", record.getTitle());
    assertEquals("Ada Lovelace", record.getAuthor());

    // Bitbucket's microsecond precision must still parse to a Date.
    Object[] row = record.toRow(true);
    int createdAt = List.of(GitInputFields.FIELD_NAMES).indexOf("created_at");
    assertEquals(Date.from(Instant.parse("2026-05-01T12:00:00Z")), row[createdAt]);
  }

  @Test
  void aFailedRequestMidPaginationAbortsInsteadOfTruncating() {
    wireMock.stubFor(
        get(urlPathEqualTo("/repositories/team/repo/issues"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody(envelopeWithNext("{\"id\":1,\"title\":\"one\",\"state\":\"new\"}"))));
    wireMock.stubFor(
        get(urlPathEqualTo("/repositories/team/repo/issues"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(aResponse().withStatus(504)));

    GitPaginationException e =
        assertThrows(
            GitPaginationException.class,
            () -> drain(client.openReader(GitResourceType.ISSUES, "team", "repo", options(1, 10))));

    assertTrue(e.getMessage().contains("Bitbucket pagination failed at page 2"));
  }

  @Test
  void sendsBasicAuthentication() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repositories/team/repo/commits"))
            .willReturn(aResponse().withStatus(200).withBody(envelope(""))));

    drain(client.openReader(GitResourceType.COMMITS, "team", "repo", options(1, 1)));

    String expected =
        "Basic "
            + Base64.getEncoder().encodeToString("ada:app-pass".getBytes(StandardCharsets.UTF_8));
    wireMock.verify(
        getRequestedFor(urlPathEqualTo("/repositories/team/repo/commits"))
            .withHeader("Authorization", equalTo(expected)));
  }

  @Test
  void aConfiguredPageSizeIsCappedAtTheBitbucketMaximum() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repositories/team/repo/commits"))
            .willReturn(aResponse().withStatus(200).withBody(envelope(""))));

    drain(client.openReader(GitResourceType.COMMITS, "team", "repo", options(500, 1)));

    wireMock.verify(
        getRequestedFor(urlPathEqualTo("/repositories/team/repo/commits"))
            .withQueryParam("pagelen", equalTo("100")));
  }
}
