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

package org.apache.hop.git.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.git.provider.GitAuth;
import org.apache.hop.git.provider.GitAuthType;
import org.apache.hop.git.provider.GitConnection;
import org.apache.hop.git.provider.GitProvider;
import org.eclipse.jgit.transport.CredentialItem;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GitCredentialsTest {

  private static final String URL = "https://example.com/repo.git";

  private final Map<String, GitConnection> gitConnections = new HashMap<>();
  private GitCredentials credentials;

  @BeforeClass
  public static void setUpEnvironment() throws Exception {
    HopLogStore.init();
    if (!HopClientEnvironment.isInitialized()) {
      HopClientEnvironment.init();
    }
  }

  @Before
  public void setUp() {
    credentials = new GitCredentials(gitConnections::get, Variables::new);
  }

  @Test
  public void testNoCredentialsAreSentOverAnotherProtocolThanHttp() throws Exception {
    gitConnections.put("conn", basicConnection("conn", "user", "pw"));
    credentials.setUserCredentials("user", "pw");

    assertNull(credentials.get("git@example.com:repo.git", "conn"));
    assertNull(credentials.get("ssh://example.com/repo.git", null));
    assertNull(credentials.get(null, null));
  }

  @Test
  public void testTheGitConnectionIsTheOnlySource() throws Exception {
    gitConnections.put("conn", basicConnection("conn", "user", "pw"));
    credentials.setUserCredentials("typed", "pw");

    CredentialsProvider sent = credentials.get(URL, "conn");
    assertTrue(GitCredentials.isGitConnection(sent));
    assertEquals("user:pw", credentials(sent));

    // Refused, it is an error, even with a 401
    assertEquals(GitCredentials.Next.FAIL, credentials.refused(sent, false));
    assertTrue(GitCredentials.isGitConnection(credentials.get(URL, "conn")));
  }

  @Test
  public void testAGitConnectionThatCanNotBeUsedIsAnError() {
    assertThrows(HopException.class, () -> credentials.get(URL, "deleted"));

    gitConnections.put("empty", basicConnection("empty", "user", null));
    assertThrows(HopException.class, () -> credentials.get(URL, "empty"));
  }

  @Test
  public void testWithoutGitConnectionTheUserCredentialsAreSent() throws Exception {
    assertNull(credentials.get(URL, null));

    credentials.setUserCredentials("typed", "pw");
    assertEquals("typed:pw", credentials(credentials.get(URL, null)));
  }

  @Test
  public void testRefusedUserCredentials() throws Exception {
    credentials.setUserCredentials("typed", "pw");

    // A 401: the user is asked again
    CredentialsProvider sent = credentials.get(URL, null);
    assertEquals(GitCredentials.Next.ASK_USER, credentials.refused(sent, false));
    assertFalse(credentials.hasUserCredentials());

    // A 403: an error, and the credentials are forgotten for the next operation
    credentials.setUserCredentials("typed", "pw");
    sent = credentials.get(URL, null);
    assertEquals(GitCredentials.Next.FAIL, credentials.refused(sent, true));
    assertFalse(credentials.hasUserCredentials());

    // Nothing was sent: the user is asked
    assertEquals(GitCredentials.Next.ASK_USER, credentials.refused(null, true));
  }

  @Test
  public void testRefusedCredentialsDoNotForgetTheOnesEnteredSince() throws Exception {
    credentials.setUserCredentials("old", "old");
    CredentialsProvider sent = credentials.get(URL, null);
    credentials.setUserCredentials("new", "new");

    assertEquals(GitCredentials.Next.RETRY, credentials.refused(sent, true));
    assertEquals("new:new", credentials(credentials.get(URL, null)));
  }

  @Test
  public void testResetForgetsTheUser() throws Exception {
    credentials.setUserCredentials("typed", "pw");

    credentials.reset();
    assertFalse(credentials.hasUserCredentials());
    assertNull(credentials.get(URL, null));
  }

  @Test
  public void testATokenIsSentAsThePassword() {
    GitAuth token = GitAuth.forToken("secret");
    CredentialsProvider gitlab = GitCredentials.toCredentialsProvider(token, GitProvider.GITLAB);
    assertEquals("oauth2:secret", credentials(gitlab));

    assertEquals("x-token-auth", GitCredentials.getTokenUsername(GitProvider.BITBUCKET));
    assertEquals("x-access-token", GitCredentials.getTokenUsername(GitProvider.GITHUB_CLOUD));
    assertNull(GitCredentials.toCredentialsProvider(null, GitProvider.GITEA));
  }

  static String credentials(CredentialsProvider provider) {
    CredentialItem.Username username = new CredentialItem.Username();
    CredentialItem.Password password = new CredentialItem.Password();
    provider.get(null, username, password);
    return username.getValue() + ":" + new String(password.getValue());
  }

  /** A Git connection using basic authentication. */
  static GitConnection basicConnection(String name, String username, String password) {
    GitConnection connection = new GitConnection();
    connection.setName(name);
    connection.setProvider(GitProvider.GITEA);
    connection.setAuthType(GitAuthType.BASIC);
    connection.setUsername(username);
    connection.setPassword(password);
    return connection;
  }
}
