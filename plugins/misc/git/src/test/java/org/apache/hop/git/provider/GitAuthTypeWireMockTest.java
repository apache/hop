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
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Which header a credential ends up in. The mechanism belongs to the connection, not the provider:
 * a personal access token and an OAuth 2 token go to the same GitLab API in different headers, and
 * sending one through the other's header silently fails to authenticate.
 */
class GitAuthTypeWireMockTest {

  private static WireMockServer wireMock;
  private final IVariables variables = new Variables();

  @BeforeAll
  static void startServer() throws Exception {
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
    wireMock.stubFor(
        get(urlPathEqualTo("/thing"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{}")));
  }

  private GitConnection connection(GitProvider provider, GitAuthType authType) {
    GitConnection connection = new GitConnection();
    connection.setName("test");
    connection.setProvider(provider);
    connection.setAuthType(authType);
    connection.setApiBaseUrl(wireMock.baseUrl());
    return connection;
  }

  /** Sends a request the way a resource client does, with the caller's usual hardcoded style. */
  private void call(GitConnection connection, GitProvider.AuthStyle callerStyle) throws Exception {
    GitApiHttp.get(
        wireMock.baseUrl() + "/thing",
        connection.toAuth(variables),
        callerStyle,
        "application/json",
        "test");
  }

  @Test
  @Timeout(30)
  void aGitLabPersonalAccessTokenGoesInThePrivateTokenHeader() throws Exception {
    GitConnection connection = connection(GitProvider.GITLAB, GitAuthType.TOKEN);
    connection.setToken("glpat-xyz");

    call(connection, GitProvider.AuthStyle.GITLAB_PRIVATE_TOKEN);

    wireMock.verify(
        getRequestedFor(urlPathEqualTo("/thing"))
            .withHeader("PRIVATE-TOKEN", equalTo("glpat-xyz"))
            .withHeader("Authorization", absent()));
  }

  @Test
  @Timeout(30)
  void aGitLabOauthTokenGoesInTheAuthorizationHeaderAsABearer() throws Exception {
    GitConnection connection = connection(GitProvider.GITLAB, GitAuthType.OAUTH2);
    connection.setToken("oauth-xyz");

    // The client still passes its usual PRIVATE-TOKEN style; the credential must override it.
    call(connection, GitProvider.AuthStyle.GITLAB_PRIVATE_TOKEN);

    wireMock.verify(
        getRequestedFor(urlPathEqualTo("/thing"))
            .withHeader("Authorization", equalTo("Bearer oauth-xyz"))
            .withHeader("PRIVATE-TOKEN", absent()));
  }

  @Test
  @Timeout(30)
  void aForgejoTokenGoesInTheAuthorizationTokenHeader() throws Exception {
    GitConnection connection = connection(GitProvider.FORGEJO, GitAuthType.TOKEN);
    connection.setToken("forgejo-pat");

    call(connection, GitProvider.AuthStyle.TOKEN_HEADER);

    wireMock.verify(
        getRequestedFor(urlPathEqualTo("/thing"))
            .withHeader("Authorization", equalTo("token forgejo-pat")));
  }

  @Test
  @Timeout(30)
  void forgejoAlsoAcceptsBasicAuthForAnAccountThatHasALocalPassword() throws Exception {
    GitConnection connection = connection(GitProvider.FORGEJO, GitAuthType.BASIC);
    connection.setUsername("ada");
    connection.setPassword("s3cret");

    call(connection, GitProvider.AuthStyle.TOKEN_HEADER);

    String expected =
        "Basic "
            + Base64.getEncoder().encodeToString("ada:s3cret".getBytes(StandardCharsets.UTF_8));
    wireMock.verify(
        getRequestedFor(urlPathEqualTo("/thing")).withHeader("Authorization", equalTo(expected)));
  }

  /** Regression: a blank password used to go out as Basic base64("user:null"). */
  @Test
  @Timeout(30)
  void aBlankPasswordSendsNoCredentialRatherThanTheStringNull() throws Exception {
    GitConnection connection = connection(GitProvider.BITBUCKET, GitAuthType.BASIC);
    connection.setUsername("ada");
    connection.setPassword("");

    assertNull(connection.toAuth(variables), "a half credential must not be built");

    call(connection, GitProvider.AuthStyle.BASIC);

    wireMock.verify(
        getRequestedFor(urlPathEqualTo("/thing")).withHeader("Authorization", absent()));
  }

  @Test
  void aBlankPasswordIsNotAUsableBasicCredential() {
    assertNull(GitAuth.forBasic("ada", ""));
    assertNull(GitAuth.forBasic("ada", null));
    assertNull(GitAuth.forBasic("", "s3cret"));
    assertTrue(GitAuth.forBasic("ada", "s3cret").isBasicAuth());
  }

  /**
   * The label is looked up with a key built from the constant name, so a missing entry cannot be
   * caught by searching for the literal - it shows up in the dropdown as !GitAuthType.X.Label!.
   */
  @Test
  void everyAuthTypeHasATranslatedLabel() {
    for (GitAuthType type : GitAuthType.values()) {
      String label = type.getDisplayName();
      assertFalse(label.startsWith("!"), "no bundle entry for " + type + ": " + label);
      assertEquals(type, GitAuthType.fromDisplayName(label, null), "label must round-trip");
    }
  }

  @Test
  void providersOfferOnlyTheMechanismsTheirApiAccepts() {
    assertEquals(
        java.util.List.of(GitAuthType.TOKEN, GitAuthType.OAUTH2),
        GitProvider.GITLAB.getSupportedAuthTypes());
    assertEquals(
        java.util.List.of(GitAuthType.BASIC), GitProvider.BITBUCKET.getSupportedAuthTypes());
    assertTrue(GitProvider.FORGEJO.supports(GitAuthType.BASIC));
    assertTrue(GitProvider.FORGEJO.supports(GitAuthType.TOKEN));
    assertFalse(GitProvider.BITBUCKET.supports(GitAuthType.TOKEN));
  }

  /** A connection stored before auth types existed must keep authenticating as it did. */
  @Test
  void aConnectionWithNoStoredAuthTypeFallsBackToTheProviderDefault() {
    GitConnection github = new GitConnection();
    github.setProvider(GitProvider.GITHUB_CLOUD);
    assertEquals(GitAuthType.TOKEN, github.getAuthType());

    GitConnection bitbucket = new GitConnection();
    bitbucket.setProvider(GitProvider.BITBUCKET);
    assertEquals(GitAuthType.BASIC, bitbucket.getAuthType());
  }

  /** A stored type the provider does not accept must not leave the connection unusable. */
  @Test
  void anUnsupportedStoredAuthTypeFallsBackToTheProviderDefault() {
    GitConnection bitbucket = new GitConnection();
    bitbucket.setProvider(GitProvider.BITBUCKET);
    bitbucket.setAuthType(GitAuthType.OAUTH2);

    assertEquals(GitAuthType.BASIC, bitbucket.getAuthType());
  }
}
