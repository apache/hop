/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.vfs.smb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.smb.metadata.SmbConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Samba with SMB1 disabled, NTLMv2 only, and mandatory signing. A second container requires SMB3
 * encryption.
 */
@Testcontainers(disabledWithoutDocker = true)
class SmbVfsContainerIT {

  private static final String PASSWORD = "hop-it-password";
  private static final Path DOCKER = Path.of("src/test/docker").toAbsolutePath();

  private static final ImageFromDockerfile IMAGE =
      new ImageFromDockerfile("hop-smb-it", false)
          .withDockerfilePath("Dockerfile")
          .withFileFromPath("Dockerfile", DOCKER.resolve("Dockerfile"))
          .withFileFromPath("smb.conf", DOCKER.resolve("smb.conf"))
          .withFileFromPath("entrypoint.sh", DOCKER.resolve("entrypoint.sh"));

  @Container static final GenericContainer<?> samba = container(false);

  @Container static final GenericContainer<?> encrypted = container(true);

  @BeforeAll
  static void initPasswords() throws HopException {
    HopLogStore.init();
    Encr.init("Hop");
  }

  @Test
  void readsAndWritesThroughTheNamedConnection() throws Exception {
    try (Opened manager =
        opened(
            connection("rooted", samba, "restricted"), "share", connection("share", samba, ""))) {
      FileObject folder = manager.resolveFile("rooted:///reports");
      folder.createFolder();
      FileObject file = manager.resolveFile("rooted:///reports/daily.csv");
      write(file, "alpha");
      try (InputStream in = file.getContent().getInputStream()) {
        assertEquals("alpha", new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
      file.refresh();
      assertTrue(file.getContent().getLastModifiedTime() > 0);

      write(file, "beta");
      try (OutputStream out = file.getContent().getOutputStream(true)) {
        out.write("!".getBytes(StandardCharsets.UTF_8));
      }
      try (InputStream in =
          manager.resolveFile("rooted:///reports/daily.csv").getContent().getInputStream()) {
        assertEquals("beta!", new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
      try (RandomAccessContent content =
          file.getContent().getRandomAccessContent(RandomAccessMode.READ)) {
        content.seek(1);
        assertEquals('e', content.readByte());
      }

      assertEquals(1, folder.getChildren().length);
      FileObject renamed = manager.resolveFile("rooted:///reports/moved.csv");
      file.moveTo(renamed);
      assertFalse(manager.resolveFile("rooted:///reports/daily.csv").exists());
      assertTrue(renamed.exists());

      assertFalse(manager.resolveFile("share:///reports/moved.csv").exists());
      assertTrue(manager.resolveFile("share:///restricted/reports/moved.csv").exists());
      assertFalse(manager.resolveFile("share:///ok-at-root.txt").exists());

      assertThrows(
          FileSystemException.class, () -> manager.resolveFile("rooted:///../../climbed.txt"));
      assertFalse(manager.resolveFile("share:///climbed.txt").exists());

      renamed.delete();
      assertFalse(renamed.exists());
    }
  }

  @Test
  void twoThreadsShareOneSession() throws Exception {
    try (Opened manager = opened(connection("finance", samba, ""), null, null)) {
      FileObject first = manager.resolveFile("finance:///one.txt");
      write(first, "one");
      ExecutorService pool = Executors.newFixedThreadPool(2);
      CountDownLatch start = new CountDownLatch(1);
      try (InputStream open = first.getContent().getInputStream()) {
        Future<?> reader =
            pool.submit(
                () -> {
                  start.await();
                  return open.readAllBytes();
                });
        Future<?> writer =
            pool.submit(
                () -> {
                  start.await();
                  write(manager.resolveFile("finance:///two.txt"), "two");
                  return null;
                });
        start.countDown();
        assertEquals("one", new String((byte[]) reader.get(), StandardCharsets.UTF_8));
        writer.get();
      } finally {
        pool.shutdownNow();
      }
      try (InputStream in =
          manager.resolveFile("finance:///two.txt").getContent().getInputStream()) {
        assertEquals("two", new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  void wrongPasswordDoesNotCreateAFile() throws Exception {
    SmbConnection connection = connection("finance", samba, "");
    connection.setPassword("not-the-password");
    try (Opened manager = opened(connection, null, null)) {
      FileObject file = manager.resolveFile("finance:///denied.txt");
      FileSystemException error =
          assertThrows(FileSystemException.class, () -> write(file, "nope"));
      assertFalse(error.getMessage().contains(PASSWORD));
      assertFalse(String.valueOf(error.getCause()).contains(PASSWORD));
    }
  }

  @Test
  void encryptedShareRoundTrips() throws Exception {
    SmbConnection connection = connection("secure", encrypted, "");
    connection.setEncryptData(true);
    try (Opened manager = opened(connection, null, null)) {
      FileObject file = manager.resolveFile("secure:///secret.txt");
      write(file, "cipher");
      try (InputStream in = file.getContent().getInputStream()) {
        assertEquals("cipher", new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
    }
  }

  private static GenericContainer<?> container(boolean encrypt) {
    GenericContainer<?> container =
        new GenericContainer<>(IMAGE)
            .withExposedPorts(445)
            .withEnv("SMB_PASSWORD", PASSWORD)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)));
    if (encrypt) {
      container.withEnv("SMB_ENCRYPT", "required");
    }
    return container;
  }

  private static SmbConnection connection(
      String name, GenericContainer<?> server, String basePath) {
    SmbConnection connection = new SmbConnection();
    connection.setName(name);
    connection.setHostname(server.getHost());
    connection.setPort(Integer.toString(server.getMappedPort(445)));
    connection.setShare("data");
    connection.setBasePath(basePath);
    connection.setAuthType(SmbAuthType.NTLM);
    connection.setUsername("hop");
    connection.setPassword("${SMB_PASSWORD}");
    connection.setRequireSigning(true);
    return connection;
  }

  private static Opened opened(SmbConnection first, String secondName, SmbConnection second)
      throws Exception {
    Variables variables = new Variables();
    variables.setVariable("SMB_PASSWORD", PASSWORD);
    DefaultFileSystemManager manager = new DefaultFileSystemManager();
    manager.addProvider(first.getName(), new SmbFileProvider(variables, first));
    if (second != null) {
      manager.addProvider(secondName, new SmbFileProvider(variables, second));
    }
    manager.init();
    return new Opened(manager);
  }

  private static final class Opened implements AutoCloseable {
    private final DefaultFileSystemManager manager;

    private Opened(DefaultFileSystemManager manager) {
      this.manager = manager;
    }

    private FileObject resolveFile(String uri) throws FileSystemException {
      return manager.resolveFile(uri);
    }

    @Override
    public void close() {
      manager.close();
    }
  }

  private static void write(FileObject file, String text) throws Exception {
    if (file.exists()) {
      file.delete();
    }
    file.createFile();
    try (OutputStream out = file.getContent().getOutputStream()) {
      out.write(text.getBytes(StandardCharsets.UTF_8));
    }
  }
}
