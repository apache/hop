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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Testing a connection must not demand a credential. Reading a public repository works anonymously
 * on every provider except Bitbucket, so a token requirement here would refuse connections the Git
 * Input transform runs against perfectly well.
 */
class GitConnectionVerifyWireMockTest {

  private static WireMockServer wireMock;
  private final IVariables variables = new Variables();

  @BeforeAll
  static void startServer() throws Exception {
    // Needed by Encr, which decodes the optionally encrypted token.
    HopClientEnvironment.init();
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

  private GitConnection connection(GitProvider provider) {
    GitConnection connection = new GitConnection();
    connection.setName("test");
    connection.setProvider(provider);
    connection.setApiBaseUrl(wireMock.baseUrl());
    return connection;
  }

  @Test
  @Timeout(30)
  void gitHubWithoutATokenVerifiesAgainstThePublicRateLimitEndpoint() {
    wireMock.stubFor(
        get(urlPathEqualTo("/rate_limit"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"resources\":{}}")));

    assertDoesNotThrow(() -> connection(GitProvider.GITHUB_CLOUD).test(variables));
    wireMock.verify(getRequestedFor(urlPathEqualTo("/rate_limit")));
  }

  @Test
  @Timeout(30)
  void gitHubWithATokenVerifiesByListingTheAccountOrganizations() {
    wireMock.stubFor(
        get(urlPathEqualTo("/user"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"login\":\"someone\"}")));
    wireMock.stubFor(
        get(urlPathEqualTo("/user/orgs"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("[]")));

    GitConnection connection = connection(GitProvider.GITHUB_CLOUD);
    connection.setToken("a-token");

    assertDoesNotThrow(() -> connection.test(variables));
    wireMock.verify(getRequestedFor(urlPathEqualTo("/user")));
  }

  @Test
  @Timeout(30)
  void gitHubReportsAnUnreachableApiRatherThanPassingSilently() {
    wireMock.stubFor(get(urlPathEqualTo("/rate_limit")).willReturn(aResponse().withStatus(404)));

    assertThrows(HopException.class, () -> connection(GitProvider.GITHUB_CLOUD).test(variables));
  }

  @Test
  @Timeout(30)
  void gitLabWithoutATokenVerifiesAgainstThePublicProjectList() {
    wireMock.stubFor(
        get(urlPathEqualTo("/projects"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("[]")));

    assertDoesNotThrow(() -> connection(GitProvider.GITLAB).test(variables));
    wireMock.verify(getRequestedFor(urlPathEqualTo("/projects")));
  }

  @Test
  @Timeout(30)
  void giteaWithoutATokenVerifiesAgainstThePublicVersionEndpoint() {
    wireMock.stubFor(
        get(urlPathEqualTo("/version"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"version\":\"1.22.0\"}")));

    assertDoesNotThrow(() -> connection(GitProvider.GITEA).test(variables));
    wireMock.verify(getRequestedFor(urlPathEqualTo("/version")));
  }

  @Test
  @Timeout(30)
  void bitbucketStillNeedsCredentialsBecauseItsApiHasNoAnonymousEntryPoint() {
    HopException e =
        assertThrows(HopException.class, () -> connection(GitProvider.BITBUCKET).test(variables));
    assertTrue(e.getMessage().contains("username and a password"), e.getMessage());
  }

  /**
   * Organizations are the one thing a token really is needed for: every provider reports the
   * organizations of the signed-in account, and anonymously there is no such account.
   */
  @Test
  @Timeout(30)
  void listingOrganizationsWithoutATokenExplainsWhatToDoInstead() {
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                GitRepositoryBrowser.forProvider(GitProvider.GITHUB_CLOUD)
                    .listOrganizations(wireMock.baseUrl(), null));
    assertTrue(e.getMessage().contains("personal access token"), e.getMessage());
    assertTrue(e.getMessage().contains("owner and name"), e.getMessage());
  }
}
