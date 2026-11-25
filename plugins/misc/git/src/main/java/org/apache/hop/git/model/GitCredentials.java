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

import java.util.function.Supplier;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.git.provider.GitAuth;
import org.apache.hop.git.provider.GitConnection;
import org.apache.hop.git.provider.GitProvider;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

/**
 * The credentials sent to a remote reached over http(s), and what to do when the remote refuses
 * them. Credentials are never sent over another protocol: over ssh they could be taken for the
 * passphrase of a key.
 *
 * <ol>
 *   <li>When a Git connection is associated with the remote, its credentials are the only ones
 *       sent. Refused, they are an error: it is the Git connection that has to be corrected.
 *   <li>Otherwise those entered by the user, kept in memory only. Refused with a 401 the user is
 *       asked again, up to {@link #MAX_USER_ATTEMPTS} times for one operation. Refused with a 403
 *       they are an error: the account may lack the rights for the operation, asking again would
 *       not help.
 * </ol>
 *
 * <p>The automatic fetch reads the credentials from its timer thread, so they are only replaced or
 * forgotten while holding the lock of this object.
 */
public class GitCredentials {

  /** How many times the user is asked for credentials during one operation. */
  public static final int MAX_USER_ATTEMPTS = 3;

  /** What to do after the remote refused credentials. */
  public enum Next {
    /** Try again with the credentials now available. */
    RETRY,
    /** Ask the user for a username and password. */
    ASK_USER,
    /** Give up: the refusal is an error. */
    FAIL
  }

  /** Loads a Git connection from the metadata. */
  @FunctionalInterface
  public interface GitConnectionLoader {
    /**
     * @return the Git connection, null if it doesn't exist
     */
    GitConnection load(String name) throws HopException;
  }

  private final GitConnectionLoader gitConnectionLoader;
  private final Supplier<IVariables> variables;

  private volatile CredentialsProvider userCredentials;

  /** Credentials from the metadata and the variables of the Hop GUI. */
  public GitCredentials() {
    this(
        name ->
            HopGui.getInstance()
                .getMetadataProvider()
                .getSerializer(GitConnection.class)
                .load(name),
        () -> HopGui.getInstance().getVariables());
  }

  /**
   * @param gitConnectionLoader loads the Git connection associated with a remote
   * @param variables the variables used to resolve the Git connection
   */
  public GitCredentials(GitConnectionLoader gitConnectionLoader, Supplier<IVariables> variables) {
    this.gitConnectionLoader = gitConnectionLoader;
    this.variables = variables;
  }

  /** Whether credentials can be sent to a remote with this URL. */
  public static boolean isHttp(String url) {
    return url != null && (url.startsWith("https://") || url.startsWith("http://"));
  }

  /**
   * The credentials to send to a remote.
   *
   * @param url the URL of the remote
   * @param gitConnection the name of the Git connection associated with the remote, null if none
   * @return the credentials, null if there are none: the user is asked if the remote wants some
   * @throws HopException if the Git connection doesn't exist or has no usable credentials
   */
  public synchronized CredentialsProvider get(String url, String gitConnection)
      throws HopException {
    if (!isHttp(url)) {
      return null;
    }
    if (gitConnection != null) {
      return getGitConnectionCredentials(gitConnection);
    }
    return userCredentials;
  }

  /**
   * Takes note that the remote refused credentials.
   *
   * <p>Credentials entered by the user are only forgotten if they are still the current ones: those
   * entered since, while the refused operation was running, are kept.
   *
   * @param sent the credentials sent with the refused operation, null if none
   * @param forbidden whether the remote answered 403 rather than 401
   * @return what to do next
   */
  public synchronized Next refused(CredentialsProvider sent, boolean forbidden) {
    if (sent instanceof GitConnectionCredentials) {
      return Next.FAIL;
    }
    if (sent == null || sent != userCredentials) {
      // Credentials entered by the user since are tried first
      return userCredentials != null ? Next.RETRY : Next.ASK_USER;
    }
    userCredentials = null;
    return forbidden ? Next.FAIL : Next.ASK_USER;
  }

  /** Keeps the credentials entered by the user, for the next operations. */
  public synchronized void setUserCredentials(String username, String password) {
    userCredentials = new UserCredentials(username, password);
  }

  public boolean hasUserCredentials() {
    return userCredentials != null;
  }

  /** Forgets every credential, for another repository or another Git connection. */
  public synchronized void reset() {
    userCredentials = null;
  }

  /**
   * Whether these credentials come from a Git connection.
   *
   * @param credentials credentials returned by {@link #get(String, String)}
   */
  public static boolean isGitConnection(CredentialsProvider credentials) {
    return credentials instanceof GitConnectionCredentials;
  }

  private CredentialsProvider getGitConnectionCredentials(String name) throws HopException {
    GitConnection connection = gitConnectionLoader.load(name);
    if (connection == null) {
      throw new HopException(
          "The Git connection '"
              + name
              + "' of the remote doesn't exist. Choose another one in Edit remote.");
    }
    CredentialsProvider credentials =
        toCredentialsProvider(connection.toAuth(variables.get()), connection.getProvider());
    if (credentials == null) {
      throw new HopException("The Git connection '" + name + "' has no credentials.");
    }
    return credentials;
  }

  /**
   * The credentials of a Git connection, for git over http(s).
   *
   * @return the credentials, null if the connection has none
   */
  static CredentialsProvider toCredentialsProvider(GitAuth auth, GitProvider provider) {
    if (auth == null) {
      return null;
    }
    if (auth.isBasicAuth()) {
      return new GitConnectionCredentials(auth.getUsername(), auth.getPassword());
    }
    if (auth.isTokenBased()) {
      return new GitConnectionCredentials(getTokenUsername(provider), auth.getToken());
    }
    return null;
  }

  /**
   * The username git over http(s) expects with a token, which is sent as the password. GitHub,
   * Gitea and Forgejo accept any username, GitLab and Bitbucket expect these ones.
   */
  static String getTokenUsername(GitProvider provider) {
    return switch (provider) {
      case GITLAB -> "oauth2";
      case BITBUCKET -> "x-token-auth";
      default -> "x-access-token";
    };
  }

  /** Credentials from a Git connection. */
  static final class GitConnectionCredentials extends UsernamePasswordCredentialsProvider {
    GitConnectionCredentials(String username, String password) {
      super(username, password);
    }
  }

  /** Credentials entered by the user. */
  static final class UserCredentials extends UsernamePasswordCredentialsProvider {
    UserCredentials(String username, String password) {
      super(username, password);
    }
  }
}
