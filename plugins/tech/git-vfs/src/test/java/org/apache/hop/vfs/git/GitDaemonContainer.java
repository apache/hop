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

import java.nio.file.Path;
import java.time.Duration;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * A real git server, in a container, serving a repository the tests filled in.
 *
 * <p>A {@code git daemon} rather than a hosting service: it is the smallest thing which is a git
 * server rather than a git library, it answers over a real socket so the transport is genuinely
 * exercised, and it only serves reads, which is what a deployment clone is.
 *
 * <p>Started by hand rather than through the {@code @Container} machinery because the tests have to
 * put a repository into it <em>before</em> the first clone, and because a container whose entry
 * point is wrong exits immediately - which reads as a container which never became ready. {@link
 * #start()} waits for the port to answer rather than sleeping, and puts the daemon's own output in
 * the failure when it does not.
 */
public class GitDaemonContainer implements AutoCloseable {

  /** The port git daemon listens on inside the container. */
  private static final int DAEMON_PORT = 9418;

  /**
   * Becomes the owner of {@code /repos}, then starts git daemon.
   *
   * <p>The repository is bind-mounted from a directory which is private to the account that created
   * it. That account is uid 1000 on some machines and another uid on others, including the GitHub
   * Actions runner. A daemon running as a fixed uid can read the mount only where the numbers
   * happen to match, and otherwise reports the repository as not exported. {@code safe.directory}
   * skips git's ownership check and does not grant permission to traverse the directory. Git also
   * refuses to start for a uid that has no passwd entry, so the owner is added to the image's
   * passwd when it is not already there. The uid is read inside the container, where the mount
   * shows it, rather than from the host.
   */
  private static final String DAEMON_ENTRYPOINT =
      """
      #!/bin/sh
      set -eu
      uid=$(stat -c '%u' /repos)
      gid=$(stat -c '%g' /repos)
      if ! awk -F: -v id="$gid" '$3 == id { found=1 } END { exit !found }' /etc/group; then
        addgroup -g "$gid" hopgit
      fi
      group=$(awk -F: -v id="$gid" '$3 == id { print $1; exit }' /etc/group)
      if ! awk -F: -v id="$uid" '$3 == id { found=1 } END { exit !found }' /etc/passwd; then
        adduser -D -H -u "$uid" -G "$group" hopgit
      fi
      user=$(awk -F: -v id="$uid" '$3 == id { print $1; exit }' /etc/passwd)
      export HOME=/tmp
      exec su-exec "$user:$group" git daemon \\
        --verbose \\
        --export-all \\
        --base-path=/repos \\
        --reuseaddr \\
        --listen=0.0.0.0 \\
        --port=9418
      """;

  private final Path repositoriesFolder;
  private final String repositoryName;
  private final GenericContainer<?> container;

  private GitDaemonContainer(Path repositoriesFolder, String repositoryName) {
    this.repositoriesFolder = repositoriesFolder;
    this.repositoryName = repositoryName;
    this.container =
        new GenericContainer<>(image())
            .withExposedPorts(DAEMON_PORT)
            // Bind mounted rather than copied in, so a test can commit to the served repository and
            // then see the daemon serve the new revision.
            .withFileSystemBind(
                repositoriesFolder.toAbsolutePath().toString(), "/repos", BindMode.READ_WRITE)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
  }

  /**
   * Start a daemon serving one repository, which the caller has already created.
   *
   * @param repositoriesFolder a folder holding the repository, named {@code repositoryName}
   * @param repositoryName the name of the repository, the last segment of its URL
   * @throws IllegalStateException when the daemon did not come up, with its own output attached
   */
  public static GitDaemonContainer start(Path repositoriesFolder, String repositoryName) {
    GitDaemonContainer daemon = new GitDaemonContainer(repositoriesFolder, repositoryName);
    try {
      daemon.container.start();
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "The git daemon container did not start. Its output was:\n" + safeLogs(daemon.container),
          e);
    }
    return daemon;
  }

  /**
   * The daemon image.
   *
   * <p>Built rather than pulled, and not {@code alpine/git}: that image has the git client and no
   * server. {@code git-daemon} is a separate package on alpine, so an image installed from the
   * plain {@code git} package starts and exits with a usage message.
   *
   * <p>{@code --export-all} serves every repository under the base path. {@code --inform} and
   * {@code --listen} are not both accepted by this version of git, and an argument it does not know
   * makes the daemon print its usage and exit. The entry point drops to the owner of the mounted
   * repository; see {@link #DAEMON_ENTRYPOINT}.
   */
  private static ImageFromDockerfile image() {
    return new ImageFromDockerfile("hop-git-vfs-test-daemon:local", false)
        .withFileFromString("hop-git-daemon", DAEMON_ENTRYPOINT)
        .withDockerfileFromBuilder(
            builder ->
                builder
                    .from("alpine:3.22")
                    .run("apk add --no-cache git git-daemon su-exec")
                    .run("git config --system --add safe.directory '*'")
                    .run("mkdir -p /repos")
                    .copy("hop-git-daemon", "/usr/local/bin/hop-git-daemon")
                    .run("chmod 755 /usr/local/bin/hop-git-daemon")
                    .entryPoint("/usr/local/bin/hop-git-daemon"));
  }

  /**
   * The URL of the served repository, with the published port mapped back to the host.
   *
   * <p>The port is whatever the container published rather than a fixed one, so a second run, or a
   * git server the developer already has running, does not collide with this one.
   */
  public String url() {
    return "git://"
        + container.getHost()
        + ":"
        + container.getMappedPort(DAEMON_PORT)
        + "/"
        + repositoryName;
  }

  /** Everything the daemon logged, which is how a refused clone explains itself. */
  public String logs() {
    return safeLogs(container);
  }

  /** The folder the repository is served from, for a test which wants to change it. */
  public Path getRepositoriesFolder() {
    return repositoriesFolder;
  }

  @Override
  public void close() {
    container.stop();
  }

  private static String safeLogs(GenericContainer<?> container) {
    try {
      return container.getLogs();
    } catch (RuntimeException e) {
      return "(the container's output could not be read: " + e.getMessage() + ")";
    }
  }
}
