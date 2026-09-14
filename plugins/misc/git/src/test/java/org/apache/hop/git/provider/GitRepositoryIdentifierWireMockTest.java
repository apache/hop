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
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The Browse dialog must hand back the identifier the API addresses a repository by, not the label
 * it shows a human. Picking "Putki Test" and then requesting {@code
 * /projects/putki-io%2FPutki+Test/repository/branches} is a 404, and the Browse button is precisely
 * the feature meant to stop a user guessing the identifier.
 */
class GitRepositoryIdentifierWireMockTest {

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

  @Test
  @Timeout(30)
  void gitLabBrowseReturnsTheProjectPathNotItsDisplayName() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/groups/putki-io/projects"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "[{\"name\":\"Putki Test\",\"path\":\"putki-test\","
                            + "\"path_with_namespace\":\"putki-io/putki-test\","
                            + "\"description\":\"\",\"visibility\":\"private\","
                            + "\"last_activity_at\":\"2026-05-01T12:00:00Z\"}]")));

    GitRepositoryPage page =
        new GitLabRepositoryBrowser()
            .listRepositories(
                wireMock.baseUrl(),
                GitAuth.forToken("t"),
                new GitOrganizationInfo("putki-io", "putki-io", false),
                null,
                1);

    assertEquals(
        List.of("putki-test"),
        page.getRepositories().stream().map(GitRepositoryInfo::getName).toList());
  }

  @Test
  @Timeout(30)
  void bitbucketBrowseReturnsTheRepositorySlugNotItsDisplayName() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/repositories/putki-io"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "{\"values\":[{\"name\":\"Putki Test\",\"slug\":\"putki-test\","
                            + "\"description\":\"\",\"is_private\":true,"
                            + "\"updated_on\":\"2026-05-01T12:00:00Z\"}]}")));

    GitRepositoryPage page =
        new BitbucketRepositoryBrowser()
            .listRepositories(
                wireMock.baseUrl(),
                GitAuth.forBasic("ada", "app-pass"),
                new GitOrganizationInfo("putki-io", "putki-io", false),
                null,
                1);

    assertEquals(
        List.of("putki-test"),
        page.getRepositories().stream().map(GitRepositoryInfo::getName).toList());
  }

  /**
   * A space in a path segment is %20; {@code +} is form encoding and a server reads it literally.
   */
  @Test
  void urlEncodingIsPathSafeNotFormEncoded() {
    assertEquals("Putki%20Test", GitApiHttp.urlEncode("Putki Test"));
    assertEquals("putki-io%2Fputki-test", GitApiHttp.urlEncode("putki-io/putki-test"));
  }
}
