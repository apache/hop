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
package org.apache.hop.vfs.git;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.git.metadata.GitConnection;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LsRemoteCommand;
import org.eclipse.jgit.api.TransportCommand;
import org.eclipse.jgit.api.TransportConfigCallback;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.HttpTransport;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.SshTransport;
import org.eclipse.jgit.transport.TagOpt;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.eclipse.jgit.transport.http.apache.HttpClientConnectionFactory;
import org.eclipse.jgit.transport.sshd.KeyPasswordProvider;
import org.eclipse.jgit.transport.sshd.ServerKeyDatabase;
import org.eclipse.jgit.transport.sshd.SshdSessionFactory;
import org.eclipse.jgit.transport.sshd.SshdSessionFactoryBuilder;

/**
 * Puts the revision a {@link GitConnection} asks for on disk, so the rest of the VFS driver can
 * read it as an ordinary folder.
 *
 * <p>This is the whole trick behind the driver, and it is worth being explicit about why it is
 * needed: a git repository is not a folder. There is no way to ask a remote for "the bytes of
 * {@code workflows/daily.hwf} at revision {@code abc123}" - those bytes only exist once git has
 * resolved the revision into a tree and checked it out. So the driver checks it out, once, and
 * serves the working copy.
 *
 * <p>The checkout is cached under a folder derived from the URL, the revision and the base path, so
 * a second read of the same revision costs nothing and two connections never collide. Concurrent
 * callers asking for the same checkout are serialized: whichever gets there first clones, the rest
 * wait and then find the finished checkout.
 */
public class GitCheckout {

  /**
   * Marks a checkout as complete, inside {@code .git} so it is not a file of the working tree.
   *
   * <p>Without it a crash halfway through a clone leaves a folder which looks like a repository to
   * git - {@code .git} is written early - and the next run would happily serve half a repository.
   * The file is written last, and only once the checkout is known to be good.
   */
  static final String READY_MARKER = ".git/hop-git-vfs-ready";

  /** A checkout being written right now, so a second thread waits for it rather than racing. */
  private static final Object CHECKOUT_LOCK = new Object();

  static {
    // The JDK HTTP client picks up a JVM-wide Authenticator other libraries install, and then an
    // HTTPS clone fails before it ever sends the password. The Apache client does not.
    HttpTransport.setConnectionFactory(new HttpClientConnectionFactory());
  }

  private final IVariables variables;
  private final GitConnection connection;

  public GitCheckout(IVariables variables, GitConnection connection) {
    this.variables = variables == null ? new Variables() : variables;
    this.connection = connection;
  }

  /**
   * The working copy holding the revision of this connection, fetching it first when that is what
   * the connection asks for.
   *
   * @return the folder to read files from, never null
   * @throws GitCheckoutException when the repository cannot be fetched, or the revision is not in
   *     it
   */
  public Path getWorkingCopy() throws GitCheckoutException {
    String url = resolvedUrl();
    if (StringUtils.isEmpty(url)) {
      throw new GitCheckoutException(
          "The git connection '" + connectionName() + "' has no repository URL");
    }

    Path checkout = checkoutFolder(url);
    synchronized (CHECKOUT_LOCK) {
      try {
        if (needsFetch(checkout)) {
          materialize(checkout, url);
        }
      } catch (GitCheckoutException e) {
        throw e;
      } catch (Exception e) {
        throw new GitCheckoutException(
            "Unable to check out '"
                + url
                + "' at revision '"
                + resolvedRevision()
                + "': "
                + e.getMessage(),
            e);
      }
    }
    return checkout;
  }

  /**
   * The folder inside the working copy which this connection serves as its root.
   *
   * @return the base path, or the root of the working copy when the connection names none
   */
  public Path getBasePath() throws GitCheckoutException {
    Path root = getWorkingCopy();
    String base = basePathKey();
    if (base.isEmpty() || "/".equals(base) || ".".equals(base)) {
      return root;
    }
    // A base path in the metadata is written the way a person writes it, with either separator and
    // possibly a leading one. The working copy is a real folder, so it wants a real relative path.
    String relative = base.replace('\\', '/');
    while (relative.startsWith("/")) {
      relative = relative.substring(1);
    }
    Path resolved = root.resolve(relative).normalize();
    if (!resolved.startsWith(root)) {
      throw new GitCheckoutException(
          "The base path '"
              + base
              + "' of git connection '"
              + connectionName()
              + "' points outside the repository");
    }
    return resolved;
  }

  /** Whether this connection accepts a write. A read only one fails the write instead. */
  public boolean isReadOnly() {
    return connection.isReadOnly();
  }

  /**
   * Ask the remote which refs it has, without checking anything out.
   *
   * <p>This is what the Test button does: a clone would prove the same thing and then throw the
   * result away, and a large repository makes that a long way to find out the password is wrong.
   *
   * @return a sentence naming what was found
   */
  public String probe() throws GitCheckoutException {
    String url = transportUrl(resolvedUrl());
    if (StringUtils.isEmpty(url)) {
      throw new GitCheckoutException(
          "The git connection '" + connectionName() + "' has no repository URL");
    }
    try {
      LsRemoteCommand command =
          Git.lsRemoteRepository().setRemote(url).setHeads(true).setTags(true);
      configureTransport(command);
      Map<String, Ref> refs = command.callAsMap();
      String revision = resolvedRevision();
      if (revision.isEmpty()) {
        return "The repository answered, with " + refs.size() + " branches and tags.";
      }
      if (looksLikeCommitId(revision)) {
        return "The repository answered. Commit '"
            + revision
            + "' is checked when the files are first read.";
      }
      if (refs.containsKey(Constants.R_HEADS + revision)
          || refs.containsKey(Constants.R_TAGS + revision)) {
        return "The repository has '" + revision + "'.";
      }
      throw new GitCheckoutException(
          "The repository " + url + " has no branch or tag called '" + revision + "'");
    } catch (GitCheckoutException e) {
      throw e;
    } catch (Exception e) {
      throw new GitCheckoutException("Unable to reach '" + url + "': " + e.getMessage(), e);
    }
  }

  // --- the cache ------------------------------------------------------------------------------

  /**
   * Where the working copy of this connection lives.
   *
   * <p>Keyed by URL, revision and base path together: those three decide what is in the folder, so
   * a change to any of them has to be a different folder. A digest of them rather than the strings
   * themselves keeps a long URL or a deep base path from turning into a path too long for the file
   * system, and keeps credentials out of the folder name.
   */
  Path checkoutFolder(String url) {
    String key = url + ' ' + resolvedRevision() + ' ' + basePathKey();
    return cacheRoot().resolve(safeFolderName(connectionName())).resolve(digest(key));
  }

  /**
   * The connection name, as a single folder name.
   *
   * <p>The name of a connection is whatever the user typed - it becomes the VFS scheme, and VFS
   * schemes are not restricted enough to promise there is no separator in one. Resolving it
   * straight into the cache would let a connection called {@code ../../etc} write its checkout
   * outside the cache folder, so anything which is not a plain name character is replaced.
   */
  static String safeFolderName(String name) {
    String safe =
        name.replaceAll("[^A-Za-z0-9._-]", "_")
            // A leading dot hides the folder and, on Windows, a name ending in a dot is the same
            // file as the name without it.
            .replaceAll("^\\.+", "_");
    return safe.isEmpty() ? "_" : safe;
  }

  private String basePathKey() {
    return Const.NVL(variables.resolve(connection.getBasePath()), "").trim();
  }

  private Path cacheRoot() {
    String configured = Const.NVL(variables.resolve(connection.getCacheFolder()), "").trim();
    if (!configured.isEmpty()) {
      return Paths.get(configured);
    }
    return Paths.get(System.getProperty("java.io.tmpdir"), "hop-git-vfs");
  }

  /**
   * Whether the checkout has to be (re)built.
   *
   * <p>Absent, or left half written by a crash, it has to be. A finished one only has to be fetched
   * again when the connection says so, or when it has been sitting around longer than it is allowed
   * to.
   */
  private boolean needsFetch(Path checkout) throws IOException {
    if (!isComplete(checkout)) {
      return true;
    }
    if (connection.isAlwaysFetch()) {
      return true;
    }
    Integer minutes = cacheMinutes();
    if (minutes == null) {
      return false;
    }
    long ageMillis =
        System.currentTimeMillis()
            - Files.getLastModifiedTime(checkout.resolve(READY_MARKER)).toMillis();
    return ageMillis > minutes * 60_000L;
  }

  private boolean isComplete(Path checkout) {
    return Files.isRegularFile(checkout.resolve(READY_MARKER));
  }

  private Integer cacheMinutes() {
    return positiveInt(connection.getCacheMinutes());
  }

  // --- fetching -------------------------------------------------------------------------------

  /**
   * Clone the repository and check the revision out, into a temporary folder which only becomes the
   * checkout once it is complete.
   */
  private void materialize(Path checkout, String url) throws Exception {
    Path parent = checkout.getParent();
    Files.createDirectories(parent);

    // Built beside the checkout rather than in it: a half written clone must never be mistaken for
    // a finished one, and the move below is what makes it finished.
    Path staging = Files.createTempDirectory(parent, checkout.getFileName() + "-staging");
    boolean moved = false;
    try {
      fetchInto(staging, url);
      deleteQuietly(checkout);
      try {
        Files.move(staging, checkout, StandardCopyOption.ATOMIC_MOVE);
      } catch (java.nio.file.AtomicMoveNotSupportedException e) {
        Files.move(staging, checkout);
      }
      moved = true;
      // Last, and only now: a checkout without this file is one this class rebuilds.
      Files.writeString(checkout.resolve(READY_MARKER), resolvedRevision());
    } finally {
      if (!moved) {
        deleteQuietly(staging);
      }
    }
  }

  /** Clone the repository into {@code staging} and put the working tree at the wanted revision. */
  private void fetchInto(Path staging, String url) throws Exception {
    // The URL the user typed, with the SSH user filled in when the connection names one. The cache
    // key stays the typed URL: rewriting it here must not look like a different repository.
    String remote = transportUrl(url);
    CloneCommand clone =
        Git.cloneRepository()
            .setURI(remote)
            .setDirectory(staging.toFile())
            // Tags are how a deployment names a release. Following only the ones that point at the
            // default branch would miss a tag of any other commit.
            .setTagOption(TagOpt.FETCH_TAGS);
    configureTransport(clone);
    try (Git git = clone.call()) {
      // A clone leaves the remote's default branch checked out, which is not what was asked for
      // when the connection names a branch, a tag or a commit of its own.
      checkoutRevision(git, remote);
    }
  }

  private void checkoutRevision(Git git, String url) throws GitAPIException, GitCheckoutException {
    String revision = resolvedRevision();
    if (StringUtils.isEmpty(revision)) {
      return; // Whatever the remote calls its default branch: that is what the clone gave us.
    }

    if (!isKnown(git, revision)) {
      // A clone brings the default branch. Anything else - another branch, a tag of a commit that
      // branch does not contain, a commit id - has to be asked for by name.
      fetchRevision(git, url, revision);
    }
    if (!isKnown(git, revision)) {
      throw new GitCheckoutException(
          "The repository " + url + " has no branch, tag or commit called '" + revision + "'");
    }
    try {
      checkoutResolved(git, revision);
    } catch (IOException e) {
      throw new GitCheckoutException(
          "Unable to check out '" + revision + "' from " + url + ": " + e.getMessage(), e);
    }
  }

  /**
   * Land on the revision: a local branch when there is one, a remote-tracking branch checked out
   * under the same name, a tag, or a detached commit.
   */
  private void checkoutResolved(Git git, String revision) throws GitAPIException, IOException {
    Repository repository = git.getRepository();
    if (repository.findRef(Constants.R_HEADS + revision) != null) {
      git.checkout().setName(revision).call();
      return;
    }
    if (repository.findRef(Constants.R_TAGS + revision) != null) {
      git.checkout().setName(Constants.R_TAGS + revision).call();
      return;
    }
    String remote = Constants.R_REMOTES + Constants.DEFAULT_REMOTE_NAME + "/" + revision;
    if (repository.findRef(remote) != null) {
      git.checkout()
          .setCreateBranch(true)
          .setName(revision)
          .setStartPoint(Constants.DEFAULT_REMOTE_NAME + "/" + revision)
          .call();
      return;
    }
    git.checkout().setName(revision).call();
  }

  /**
   * Ask the remote for one revision.
   *
   * <p>A branch and a tag are fetched separately. One fetch with both specs fails the whole fetch
   * when either name is absent, so a tag called {@code v1} would be refused because there is no
   * branch of that name, and the other way around.
   */
  private void fetchRevision(Git git, String url, String revision) throws GitCheckoutException {
    if (looksLikeCommitId(revision)) {
      // A commit id is not a ref. The server has to allow fetching an arbitrary commit; one that
      // does not is reported as the revision being missing, which is what the caller checks next.
      fetchOne(git, url, new RefSpec(revision), revision);
      return;
    }
    GitCheckoutException branchFailure =
        fetchOne(
            git,
            url,
            new RefSpec("+refs/heads/" + revision + ":refs/remotes/origin/" + revision),
            revision);
    if (isKnown(git, revision)) {
      return;
    }
    if (branchFailure != null && !refWasMissing(branchFailure)) {
      throw branchFailure;
    }
    GitCheckoutException tagFailure =
        fetchOne(
            git, url, new RefSpec("+refs/tags/" + revision + ":refs/tags/" + revision), revision);
    if (isKnown(git, revision)) {
      return;
    }
    if (tagFailure != null && !refWasMissing(tagFailure)) {
      throw tagFailure;
    }
  }

  /**
   * Fetch one ref spec.
   *
   * @return the failure, or null when the fetch itself succeeded (the ref may still be absent)
   */
  private GitCheckoutException fetchOne(Git git, String url, RefSpec spec, String revision) {
    try {
      var fetch =
          git.fetch()
              .setRemote(Constants.DEFAULT_REMOTE_NAME)
              .setRefSpecs(spec)
              .setRemoveDeletedRefs(false);
      configureTransport(fetch);
      fetch.call();
      return null;
    } catch (Exception e) {
      return new GitCheckoutException(
          "Unable to fetch '" + revision + "' from " + url + ": " + e.getMessage(), e);
    }
  }

  /** A fetch that failed because the remote has no such ref, rather than because it is down. */
  private boolean refWasMissing(GitCheckoutException failure) {
    String message = failure.getMessage() == null ? "" : failure.getMessage().toLowerCase();
    return message.contains("does not have")
        || message.contains("couldn't find remote ref")
        || message.contains("not found")
        || message.contains("no such");
  }

  private boolean isKnown(Git git, String revision) {
    Repository repository = git.getRepository();
    try {
      if (repository.resolve(revision) != null) {
        return true;
      }
      return repository.findRef(Constants.R_HEADS + revision) != null
          || repository.findRef(Constants.R_TAGS + revision) != null
          || repository.findRef(Constants.R_REMOTES + "origin/" + revision) != null;
    } catch (IOException e) {
      return false;
    }
  }

  /** A full SHA-1 (40 hex digits) or SHA-256 (64) commit id. Anything else is a branch or a tag. */
  static boolean looksLikeCommitId(String revision) {
    if (revision == null || (revision.length() != 40 && revision.length() != 64)) {
      return false;
    }
    for (int i = 0; i < revision.length(); i++) {
      if (Character.digit(revision.charAt(i), 16) < 0) {
        return false;
      }
    }
    return true;
  }

  // --- credentials ----------------------------------------------------------------------------

  /**
   * Put the connection's authentication on a clone, a fetch or an {@code ls-remote}.
   *
   * <p>{@code setTimeout} takes a primitive, so an unset timeout means not calling it. Passing the
   * {@code Integer} of a null unboxes into an NPE on every connection which never set one.
   */
  private void configureTransport(TransportCommand<?, ?> command) throws GitCheckoutException {
    Integer timeout = timeoutSeconds();
    if (timeout != null) {
      command.setTimeout(timeout);
    }
    CredentialsProvider credentials = credentials();
    if (credentials != null) {
      command.setCredentialsProvider(credentials);
    }
    TransportConfigCallback ssh = sshCallback();
    if (ssh != null) {
      command.setTransportConfigCallback(ssh);
    }
  }

  private CredentialsProvider credentials() throws GitCheckoutException {
    if (connection.getAuthType() != GitAuthType.USERNAME_PASSWORD) {
      return null;
    }
    String user = Const.NVL(variables.resolve(connection.getUserName()), "").trim();
    if (user.isEmpty()) {
      throw new GitCheckoutException(
          "The git connection '" + connectionName() + "' has no user name");
    }
    return new UsernamePasswordCredentialsProvider(user, secret(connection.getPassword()));
  }

  /**
   * An SSH session that authenticates with the deploy key, or null when this connection does not
   * use one.
   *
   * <p>The factory belongs to this command. Installing it as the JVM-wide SSH session factory would
   * change every other git operation Hop is doing at the same time.
   */
  private TransportConfigCallback sshCallback() throws GitCheckoutException {
    if (connection.getAuthType() != GitAuthType.DEPLOY_KEY) {
      return null;
    }
    Path key = deployKey();
    String passphrase = secret(connection.getPrivateKeyPassphrase());
    File home = new File(System.getProperty("user.home", "."));
    SshdSessionFactoryBuilder builder =
        new SshdSessionFactoryBuilder()
            .setHomeDirectory(home)
            .setSshDirectory(new File(home, ".ssh"))
            .setPreferredAuthentications("publickey")
            .setDefaultIdentities(ignored -> List.of(key.toAbsolutePath()));
    if (!passphrase.isEmpty()) {
      builder.setKeyPasswordProvider(ignored -> new FixedPassphrase(passphrase));
    }
    String knownHosts = Const.NVL(variables.resolve(connection.getKnownHostsFile()), "").trim();
    if (!knownHosts.isEmpty()) {
      Path hosts = Paths.get(knownHosts);
      if (!Files.isReadable(hosts)) {
        throw new GitCheckoutException(
            "The known hosts file of git connection '"
                + connectionName()
                + "' is not readable: "
                + knownHosts);
      }
      builder.setDefaultKnownHostsFiles(ignored -> List.of(hosts.toAbsolutePath()));
    }
    if (connection.isAcceptUnknownHosts()) {
      builder.setServerKeyDatabase((ignoredHome, ignoredSsh) -> new AcceptAnyHost());
    }
    SshdSessionFactory factory = builder.build(null);
    return transport -> {
      if (transport instanceof SshTransport ssh) {
        ssh.setSshSessionFactory(factory);
      }
    };
  }

  /** The deploy key file, checked before any network call so a missing key fails on its own. */
  private Path deployKey() throws GitCheckoutException {
    String keyFile = Const.NVL(variables.resolve(connection.getPrivateKeyFile()), "").trim();
    if (keyFile.isEmpty()) {
      throw new GitCheckoutException(
          "The git connection '" + connectionName() + "' has no deploy key");
    }
    Path path = Paths.get(keyFile);
    if (!Files.isReadable(path)) {
      throw new GitCheckoutException(
          "The deploy key of git connection '"
              + connectionName()
              + "' is not readable: "
              + keyFile);
    }
    try {
      String pem = Files.readString(path, StandardCharsets.UTF_8);
      if (!pem.contains("PRIVATE KEY")) {
        throw new GitCheckoutException(
            "The deploy key of git connection '"
                + connectionName()
                + "' is not a private key: "
                + keyFile);
      }
    } catch (GitCheckoutException e) {
      throw e;
    } catch (IOException e) {
      throw new GitCheckoutException(
          "Unable to read the deploy key of git connection '"
              + connectionName()
              + "': "
              + e.getMessage(),
          e);
    }
    return path;
  }

  /**
   * The URL to hand to JGit: the one the connection names, with the SSH user filled in when the
   * connection says who to log in as and the URL itself does not.
   */
  String transportUrl(String url) {
    if (connection.getAuthType() != GitAuthType.DEPLOY_KEY || StringUtils.isEmpty(url)) {
      return url;
    }
    String user = Const.NVL(variables.resolve(connection.getSshUser()), "").trim();
    if (user.isEmpty()) {
      return url;
    }
    try {
      URIish uri = new URIish(url);
      if (user.equals(uri.getUser())) {
        return url;
      }
      // scp-style git@host:path has no scheme. setUser still rewrites it, and toString keeps the
      // scp form, which is what JGit expects to see back.
      return uri.setUser(user).toString();
    } catch (Exception e) {
      return url;
    }
  }

  // --- helpers --------------------------------------------------------------------------------

  private String connectionName() {
    return Const.NVL(connection.getName(), "<unnamed>");
  }

  private String resolvedUrl() {
    return Const.NVL(variables.resolve(connection.getRepositoryUrl()), "").trim();
  }

  private String resolvedRevision() {
    return Const.NVL(variables.resolve(connection.getRevision()), "").trim();
  }

  /** A password or passphrase, decrypted when Hop stored it encrypted, empty when unset. */
  private String secret(String value) {
    String resolved = Const.NVL(variables.resolve(value), "");
    if (resolved.isEmpty()) {
      return "";
    }
    return Const.NVL(Encr.decryptPasswordOptionallyEncrypted(resolved), "");
  }

  private Integer timeoutSeconds() {
    return positiveInt(connection.getTimeoutSeconds());
  }

  private Integer positiveInt(String value) {
    String text = Const.NVL(variables.resolve(value), "").trim();
    if (text.isEmpty()) {
      return null;
    }
    try {
      int parsed = Integer.parseInt(text);
      return parsed > 0 ? parsed : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** A stable, short folder name for a set of settings. */
  static String digest(String key) {
    try {
      MessageDigest sha = MessageDigest.getInstance("SHA-256");
      byte[] bytes = sha.digest(key.getBytes(StandardCharsets.UTF_8));
      StringBuilder builder = new StringBuilder(bytes.length * 2);
      for (byte b : bytes) {
        builder.append(String.format("%02x", b));
      }
      return builder.substring(0, 16);
    } catch (Exception e) {
      return Integer.toHexString(key.hashCode());
    }
  }

  private void deleteQuietly(Path path) {
    if (path == null || !Files.exists(path)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(path)) {
      List<Path> entries = new ArrayList<>(walk.toList());
      // Deepest first: a folder can only go once everything under it has.
      entries.sort((a, b) -> b.getNameCount() - a.getNameCount());
      for (Path entry : entries) {
        Files.deleteIfExists(entry);
      }
    } catch (Exception e) {
      // Best effort: a leftover staging folder costs at worst a rebuild on the next run.
    }
  }

  /**
   * The passphrase of one deploy key. A wrong passphrase fails the load instead of asking again.
   */
  private static final class FixedPassphrase implements KeyPasswordProvider {
    private final char[] passphrase;

    private FixedPassphrase(String passphrase) {
      this.passphrase = passphrase.toCharArray();
    }

    @Override
    public char[] getPassphrase(URIish uri, int attempt) {
      return attempt == 0 ? passphrase : null;
    }

    @Override
    public void setAttempts(int maxNumberOfAttempts) {
      // One try: the passphrase is a setting, not something a person is typing again.
    }

    @Override
    public boolean keyLoaded(URIish uri, int attempt, Exception error) {
      return false;
    }
  }

  /**
   * Accepts whatever host key the server offers.
   *
   * <p>Off unless the connection asks for it. A container which has no {@code known_hosts} cannot
   * connect otherwise, and turning it on is the connection saying the network it runs on is
   * trusted.
   */
  private static final class AcceptAnyHost implements ServerKeyDatabase {
    @Override
    public List<PublicKey> lookup(
        String connectAddress, InetSocketAddress remoteAddress, Configuration config) {
      return List.of();
    }

    @Override
    public boolean accept(
        String connectAddress,
        InetSocketAddress remoteAddress,
        PublicKey serverKey,
        Configuration config,
        CredentialsProvider provider) {
      return true;
    }
  }
}
