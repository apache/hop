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
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Behaviour of {@link GiteaResourceClient}, which also serves Forgejo, against a stubbed API. */
class GiteaResourceClientWireMockTest {

  /**
   * One server for the whole class, reset between tests. Starting and stopping a server per test
   * churns through dynamic ports, and a recycled port can collide with a pooled connection in the
   * shared {@link GitApiHttp} HTTP client.
   */
  private static WireMockServer wireMock;

  private GiteaResourceClient client;

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
    client = new GiteaResourceClient(wireMock.baseUrl(), GitAuth.forToken("gitea-token"));
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
  void readsCommitsAndMapsTheNestedCommitObject() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/bart/putki/commits"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody(
                        "[{\"sha\":\"cafe01\",\"html_url\":\"https://codeberg.org/bart/putki/commit/cafe01\","
                            + "\"commit\":{\"message\":\"a commit\","
                            + "\"author\":{\"name\":\"Ada\",\"date\":\"2026-05-01T12:00:00Z\"}}}]")));
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/bart/putki/commits"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(aResponse().withStatus(200).withBody("[]")));

    List<GitResourceRecord> records =
        drain(client.openReader(GitResourceType.COMMITS, "bart", "putki", options(1, 10)));

    assertEquals(1, records.size());
    assertEquals("gitea", records.get(0).getProvider());
    assertEquals("cafe01", records.get(0).getSha());
    assertEquals("Ada", records.get(0).getAuthor());
  }

  @Test
  void aFailedRequestMidPaginationAbortsInsteadOfTruncating() {
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/bart/putki/issues"))
            .withQueryParam("page", equalTo("1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody("[{\"id\":1,\"number\":1,\"title\":\"one\",\"state\":\"open\"}]")));
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/bart/putki/issues"))
            .withQueryParam("page", equalTo("2"))
            .willReturn(aResponse().withStatus(504)));

    GitPaginationException e =
        assertThrows(
            GitPaginationException.class,
            () ->
                drain(client.openReader(GitResourceType.ISSUES, "bart", "putki", options(1, 10))));

    assertTrue(e.getMessage().contains("Gitea pagination failed at page 2"));
  }

  @Test
  void sendsTheTokenHeaderStyleGiteaExpects() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repos/bart/putki/commits"))
            .willReturn(aResponse().withStatus(200).withBody("[]")));

    drain(client.openReader(GitResourceType.COMMITS, "bart", "putki", options(1, 1)));

    wireMock.verify(
        getRequestedFor(urlPathEqualTo("/repos/bart/putki/commits"))
            .withHeader("Authorization", equalTo("token gitea-token")));
  }

  @Test
  void unsupportedResourceTypesFailFastWithAClearMessage() {
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                client.openReader(GitResourceType.ISSUE_COMMENTS, "bart", "putki", options(10, 1)));

    assertTrue(e.getMessage().contains("not yet supported"));
  }
}
