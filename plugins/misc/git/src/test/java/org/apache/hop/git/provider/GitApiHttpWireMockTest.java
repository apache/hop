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
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Transport-level behaviour shared by every provider client. */
class GitApiHttpWireMockTest {

  /** One server for the whole class, reset between tests. See the provider tests for why. */
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

  private String fetch() throws HopException {
    return GitApiHttp.get(
        wireMock.baseUrl() + "/thing",
        GitAuth.forToken("t"),
        GitProvider.AuthStyle.BEARER,
        "application/json",
        "GitHub");
  }

  @Test
  @Timeout(30)
  void retriesATransientServerErrorAndThenSucceeds() throws Exception {
    wireMock.stubFor(
        get(urlPathEqualTo("/thing"))
            .inScenario("retry")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse().withStatus(500))
            .willSetStateTo("recovered"));
    wireMock.stubFor(
        get(urlPathEqualTo("/thing"))
            .inScenario("retry")
            .whenScenarioStateIs("recovered")
            .willReturn(aResponse().withStatus(200).withBody("[]")));

    assertEquals("[]", fetch());
    wireMock.verify(2, getRequestedFor(urlPathEqualTo("/thing")));
  }

  @Test
  void doesNotRetryAStatusThatWillNotRecover() {
    wireMock.stubFor(get(urlPathEqualTo("/thing")).willReturn(aResponse().withStatus(404)));

    assertThrows(GitApiException.class, this::fetch);
    wireMock.verify(1, getRequestedFor(urlPathEqualTo("/thing")));
  }

  @Test
  void unauthorizedIsReportedWithActionableAdvice() {
    wireMock.stubFor(get(urlPathEqualTo("/thing")).willReturn(aResponse().withStatus(401)));

    HopException e = assertThrows(HopException.class, this::fetch);

    assertTrue(e.getMessage().contains("authentication failed"));
    assertTrue(e.getMessage().contains("scopes"));
    wireMock.verify(1, getRequestedFor(urlPathEqualTo("/thing")));
  }

  @Test
  void forbiddenQuotesTheProviderThatActuallyAnswered() {
    wireMock.stubFor(
        get(urlPathEqualTo("/thing"))
            .willReturn(
                aResponse()
                    .withStatus(403)
                    .withBody("{\"message\":\"Repository access blocked\"}")));

    HopException e =
        assertThrows(
            HopException.class,
            () ->
                GitApiHttp.get(
                    wireMock.baseUrl() + "/thing",
                    GitAuth.forToken("t"),
                    GitProvider.AuthStyle.GITLAB_PRIVATE_TOKEN,
                    "application/json",
                    "GitLab"));

    // The message used to name GitHub no matter which provider had answered.
    assertTrue(e.getMessage().contains("GitLab says: Repository access blocked"));
    assertFalse(e.getMessage().contains("GitHub"));
  }

  @Test
  void malformedJsonIsReportedAsAParseFailure() {
    wireMock.stubFor(
        get(urlPathEqualTo("/thing")).willReturn(aResponse().withStatus(200).withBody("not json")));

    HopException e =
        assertThrows(HopException.class, () -> GitApiHttp.parseArray(fetch(), "GitHub"));

    assertTrue(e.getMessage().contains("Failed to parse GitHub API response"));
  }
}
