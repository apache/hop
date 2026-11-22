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
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.git.provider.GitAuth;
import org.apache.hop.git.provider.GitConnection;
import org.apache.hop.git.provider.GitProvider;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.jgit.transport.CredentialItem;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.NetRCCredentialsProvider;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

/**
 * The credentials sent to a remote reached over http(s), and what to do when the remote refuses
 * them. Credentials are never sent over another protocol: over ssh they could be taken for the
 * passphrase of a key.
 *
 * <ol>
 *   <li>When a Git connection is associated with the remote, its credentials are the only ones
 *       sent. Refused, they are an error: it is the Git connection that has to be corrected.
 *   <li>Otherwise those of the host of the remote in the .netrc file of the user, _netrc on
 *       Windows, or the file named by the NETRC environment variable. Refused, they are no longer
 *       sent and the user is asked instead.
 *   <li>Otherwise those entered by the user. Refused with a 401 the user is asked again, up to
 *       {@link #MAX_USER_ATTEMPTS} times for one operation. Refused with a 403 they are an error:
 *       the account may lack the rights for the operation, asking again would not help.
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
  private final Supplier<CredentialsProvider> netRcFactory;

  private volatile CredentialsProvider userCredentials;
  private volatile boolean netRcRefused;

  /** Credentials from the metadata and the variables of the Hop GUI. */
  public GitCredentials() {
    this(
        name ->
            HopGui.getInstance()
                .getMetadataProvider()
                .getSerializer(GitConnection.class)
                .load(name),
        () -> HopGui.getInstance().getVariables(),
        NetRCCredentialsProvider::new);
  }

  /**
   * @param gitConnectionLoader loads the Git connection associated with a remote
   * @param variables the variables used to resolve the Git connection
   * @param netRcFactory creates a provider reading the .netrc file
   */
  public GitCredentials(
      GitConnectionLoader gitConnectionLoader,
      Supplier<IVariables> variables,
      Supplier<CredentialsProvider> netRcFactory) {
    this.gitConnectionLoader = gitConnectionLoader;
    this.variables = variables;
    this.netRcFactory = netRcFactory;
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
    if (!netRcRefused) {
      CredentialsProvider netRc = getNetRcCredentials(url);
      if (netRc != null) {
        return netRc;
      }
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
    if (sent instanceof NetRcCredentials) {
      netRcRefused = true;
      LogChannel.UI.logBasic(
          "The remote refused the credentials of the .netrc file, they are no longer used");
      return userCredentials != null ? Next.RETRY : Next.ASK_USER;
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
    netRcRefused = false;
  }

  /**
   * Whether these credentials come from a Git connection.
   *
   * @param credentials credentials returned by {@link #get(String, String)}
   */
  public static boolean isGitConnection(CredentialsProvider credentials) {
    return credentials instanceof GitConnectionCredentials;
  }

  /**
   * Whether these credentials come from the .netrc file.
   *
   * @param credentials credentials returned by {@link #get(String, String)}
   */
  public static boolean isNetRc(CredentialsProvider credentials) {
    return credentials instanceof NetRcCredentials;
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

  private CredentialsProvider getNetRcCredentials(String url) {
    try {
      URIish uri = new URIish(url);
      CredentialsProvider netRc = netRcFactory.get();
      CredentialItem.Username username = new CredentialItem.Username();
      CredentialItem.Password password = new CredentialItem.Password();
      boolean found = netRc.get(uri, username, password);
      password.clear();
      return found ? new NetRcCredentials(netRc) : null;
    } catch (Exception e) {
      LogChannel.UI.logError("Unable to read the credentials of the .netrc file", e);
      return null;
    }
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

  /**
   * Credentials from the .netrc file. The file is read again each time they are sent, a change in
   * it is taken into account.
   */
  static final class NetRcCredentials extends CredentialsProvider {
    private final CredentialsProvider netRc;

    NetRcCredentials(CredentialsProvider netRc) {
      this.netRc = netRc;
    }

    @Override
    public boolean isInteractive() {
      return false;
    }

    @Override
    public boolean supports(CredentialItem... items) {
      return netRc.supports(items);
    }

    @Override
    public boolean get(URIish uri, CredentialItem... items) {
      return netRc.get(uri, items);
    }
  }
}
