/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.pipeline.transforms.watchfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.vfs.plugin.VfsPlugin;
import org.apache.hop.core.vfs.plugin.VfsPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.vfs.sftp.SftpConnectionFileProvider;
import org.apache.hop.vfs.sftp.SftpVfsPlugin;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Opt-in tests against an actual SFTP server. Only UUID-owned children of the supplied root change.
 */
@EnabledIfEnvironmentVariable(named = "WATCHFILES_SFTP_HOST", matches = ".+")
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
@Timeout(40)
class WatchFilesSftpIntegrationTest {
  private static final String CONNECTION = "watchfiles-real-sftp";
  private static final String PRODUCER = "watchfiles-test-producer";
  @TempDir Path temporary;
  private String remotePath;
  private FileObject remote;
  private final ArrayList<LocalPipelineEngine> pipelines = new ArrayList<>();

  private static String required(String variable) {
    String value = System.getenv(variable);
    assertNotNull(value, "Missing external test environment variable " + variable);
    assertFalse(value.isEmpty(), "Empty external test environment variable " + variable);
    return value;
  }

  private static SftpConnection connection() {
    SftpConnection connection = new SftpConnection();
    connection.setName(CONNECTION);
    connection.setServerName("${SFTP_TEST_HOST}");
    connection.setServerPort(System.getenv().getOrDefault("WATCHFILES_SFTP_PORT", "22"));
    connection.setUsername(required("WATCHFILES_SFTP_USER"));
    connection.setPassword(required("WATCHFILES_SFTP_PASSWORD"));
    connection.setUserDirIsRoot(false);
    connection.setConnectionTimeout("2000");
    connection.setSessionTimeout("2000");
    return connection;
  }

  private static Variables variables() {
    Variables variables = new Variables();
    variables.setVariable("SFTP_TEST_HOST", required("WATCHFILES_SFTP_HOST"));
    return variables;
  }

  @BeforeAll
  static void registerPlugins() throws Exception {
    HopEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(
        WatchFilesMeta.class.getClassLoader(),
        new ArrayList<>(),
        null,
        WatchFilesMeta.class.getName(),
        TransformPluginType.class,
        Transform.class,
        true);
    registry.registerPluginClass(
        SftpVfsPlugin.class.getClassLoader(),
        new ArrayList<>(),
        null,
        SftpVfsPlugin.class.getName(),
        VfsPluginType.class,
        VfsPlugin.class,
        true);
    // Producer mutations have their own provider; the source must resolve its execution metadata.
    SftpConnection producer = connection();
    producer.setName(PRODUCER);
    HopVfs.getFileSystemManager()
        .addProvider(PRODUCER, new SftpConnectionFileProvider(variables(), producer));
  }

  @BeforeEach
  void createOwnedDirectory() throws Exception {
    String base = required("WATCHFILES_SFTP_ROOT");
    assertTrue(base.startsWith("/") && base.length() > 1, "Use an absolute dedicated test root");
    remotePath = base.replaceAll("/+$", "") + "/case-" + UUID.randomUUID();
    remote = HopVfs.getFileObject(PRODUCER + "://" + remotePath, variables());
    remote.createFolder();
  }

  @AfterEach
  void cleanupOwnedDirectory() throws Exception {
    try {
      for (LocalPipelineEngine pipeline : pipelines) {
        if (!pipeline.isFinished()) {
          pipeline.stopAll();
          pipeline.waitUntilFinished();
        }
      }
    } finally {
      if (remote != null) {
        remote.refresh();
        remote.deleteAll();
        remote.close();
      }
    }
  }

  private WatchFilesMeta meta(String initial) {
    WatchFilesMeta meta = new WatchFilesMeta();
    meta.setDirectory("${SFTP_TEST_ROOT}");
    meta.setWatchId("real-sftp");
    meta.setStateDirectory(temporary.resolve("state").toString());
    meta.setInitialScan(initial);
    meta.setStrategy("AUTO");
    meta.setIncludeSubdirectories(true);
    meta.setDeleted(true);
    meta.setIncludeWildcard(".*\\.csv");
    meta.setExcludeWildcard("ignore.*");
    meta.setPollingInterval("200");
    meta.setCheckpointInterval("100");
    meta.setMinimumAge("0");
    meta.setStabilityChecks("3");
    meta.setStabilityInterval("200");
    return meta;
  }

  private LocalPipelineEngine prepare(
      WatchFilesMeta meta,
      String sourceName,
      LinkedBlockingQueue<Object[]> rows,
      SftpConnection connection)
      throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(SftpConnection.class).save(connection);
    PipelineMeta metadata = new PipelineMeta();
    metadata.setName("External SFTP validation");
    metadata.setMetadataProvider(provider);
    metadata.addTransform(new TransformMeta(sourceName, meta));
    LocalPipelineEngine pipeline = new LocalPipelineEngine(metadata);
    pipeline.setVariable("SFTP_TEST_HOST", required("WATCHFILES_SFTP_HOST"));
    pipeline.setVariable("SFTP_TEST_ROOT", CONNECTION + "://" + remotePath);
    pipeline.setLogLevel(LogLevel.ERROR);
    pipelines.add(pipeline);
    pipeline.prepareExecution();
    pipeline
        .findRunThread(sourceName)
        .addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
                rows.add(row.clone());
              }
            });
    return pipeline;
  }

  private LocalPipelineEngine start(
      WatchFilesMeta meta, String name, LinkedBlockingQueue<Object[]> rows) throws Exception {
    LocalPipelineEngine pipeline = prepare(meta, name, rows, connection());
    pipeline.startThreads();
    return pipeline;
  }

  private void stop(LocalPipelineEngine pipeline) {
    long start = System.nanoTime();
    pipeline.stopAll();
    pipeline.waitUntilFinished();
    assertTrue(
        System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5), "Stop took over five seconds");
    assertEquals(0, pipeline.getErrors());
  }

  private void write(String relative, String content) throws Exception {
    try (FileObject file = remote.resolveFile(relative)) {
      file.getParent().createFolder();
      try (var stream = file.getContent().getOutputStream()) {
        stream.write(content.getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  private Object[] row(LinkedBlockingQueue<Object[]> rows, String name, String type)
      throws Exception {
    Object[] row = rows.poll(10, TimeUnit.SECONDS);
    assertNotNull(row, "Expected " + name + " " + type);
    assertEquals(name, row[1]);
    assertEquals(type, row[4]);
    assertFalse(row[3].toString().contains(required("WATCHFILES_SFTP_PASSWORD")));
    return row;
  }

  private void checkpointContains(String name) throws Exception {
    Path state = temporary.resolve("state/real-sftp.json");
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while ((!Files.exists(state) || !Files.readString(state).contains(name))
        && System.nanoTime() < deadline) {
      Thread.sleep(25);
    }
    assertTrue(Files.readString(state).contains(name), "Missing acknowledged checkpoint " + name);
    assertFalse(Files.readString(state).contains(required("WATCHFILES_SFTP_PASSWORD")));
  }

  @Test
  void existingFilesFiltersRecursiveNewDirectoryAndReaderCompatibleFilename() throws Exception {
    write("existing.csv", "existing");
    write("ignore.csv", "excluded");
    write("not-csv.txt", "excluded");
    LinkedBlockingQueue<Object[]> rows = new LinkedBlockingQueue<>();
    LocalPipelineEngine pipeline = start(meta("EMIT_EXISTING"), "Watch", rows);
    Object[] existing = row(rows, "existing.csv", "CREATED");
    assertEquals(8L, existing[5]);
    try (FileObject reader = HopVfs.getFileObject(existing[0].toString(), pipeline);
        var stream = reader.getContent().getInputStream()) {
      assertEquals("existing", new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    }
    write("new folder/árbol ü.csv", "nested");
    row(rows, "árbol ü.csv", "CREATED");
    assertNull(rows.poll(500, TimeUnit.MILLISECONDS));
    stop(pipeline);
  }

  @Test
  void growingRemoteFileWaitsForConsecutiveChecksAndMinimumAge() throws Exception {
    WatchFilesMeta meta = meta("IGNORE_EXISTING");
    meta.setMinimumAge("2000");
    meta.setStabilityChecks("4");
    LinkedBlockingQueue<Object[]> rows = new LinkedBlockingQueue<>();
    LocalPipelineEngine pipeline = start(meta, "Watch", rows);
    for (int index = 1; index <= 6; index++) {
      write("growing.csv", "x".repeat(index * 100));
      assertNull(rows.poll(150, TimeUnit.MILLISECONDS), "A growing file must remain pending");
    }
    Object[] stable = row(rows, "growing.csv", "CREATED");
    assertEquals(600L, stable[5]);
    assertTrue(System.currentTimeMillis() - ((java.util.Date) stable[6]).getTime() >= 2000);
    assertNull(rows.poll(500, TimeUnit.MILLISECONDS), "Hints/scans must coalesce");
    stop(pipeline);
  }

  @Test
  void restartRecoversOfflineCreateModifyDeleteWithoutReplayingUnchangedFiles() throws Exception {
    write("A.csv", "a");
    LinkedBlockingQueue<Object[]> firstRows = new LinkedBlockingQueue<>();
    LocalPipelineEngine first = start(meta("IGNORE_EXISTING"), "Old name", firstRows);
    assertNull(firstRows.poll(300, TimeUnit.MILLISECONDS));
    write("B.csv", "b");
    row(firstRows, "B.csv", "CREATED");
    checkpointContains("B.csv");
    stop(first);
    write("C.csv", "offline");
    LinkedBlockingQueue<Object[]> secondRows = new LinkedBlockingQueue<>();
    LocalPipelineEngine second = start(meta("COMPARE_WITH_STATE"), "Renamed source", secondRows);
    row(secondRows, "C.csv", "CREATED");
    assertNull(secondRows.poll(500, TimeUnit.MILLISECONDS), "A and B must not repeat");
    checkpointContains("C.csv");
    stop(second);
    write("A.csv", "changed offline");
    try (FileObject b = remote.resolveFile("B.csv")) {
      b.delete();
    }
    write("D.csv", "new offline");
    LinkedBlockingQueue<Object[]> thirdRows = new LinkedBlockingQueue<>();
    LocalPipelineEngine third = start(meta("COMPARE_WITH_STATE"), "Third name", thirdRows);
    Map<String, Object[]> events = new HashMap<>();
    for (int index = 0; index < 3; index++) {
      Object[] event = thirdRows.poll(10, TimeUnit.SECONDS);
      assertNotNull(event);
      events.put(event[1].toString(), event);
    }
    assertEquals("MODIFIED", events.get("A.csv")[4]);
    assertEquals(1L, events.get("A.csv")[10]);
    assertEquals("DELETED", events.get("B.csv")[4]);
    assertEquals(1L, events.get("B.csv")[5]);
    assertEquals("CREATED", events.get("D.csv")[4]);
    assertNull(thirdRows.poll(500, TimeUnit.MILLISECONDS));
    stop(third);
  }

  @Test
  void remoteRenameProducesDeleteAndCreate() throws Exception {
    write("old.csv", "rename");
    LinkedBlockingQueue<Object[]> rows = new LinkedBlockingQueue<>();
    LocalPipelineEngine pipeline = start(meta("IGNORE_EXISTING"), "Watch", rows);
    try (FileObject old = remote.resolveFile("old.csv");
        FileObject renamed = remote.resolveFile("new.csv")) {
      old.moveTo(renamed);
    }
    row(rows, "old.csv", "DELETED");
    row(rows, "new.csv", "CREATED");
    assertNull(rows.poll(500, TimeUnit.MILLISECONDS));
    stop(pipeline);
  }

  @Test
  void unreadableChildRetainsStateAndRecoveryFindsNewFile() throws Exception {
    write("blocked/known.csv", "known");
    LinkedBlockingQueue<Object[]> rows = new LinkedBlockingQueue<>();
    LocalPipelineEngine pipeline = start(meta("IGNORE_EXISTING"), "Watch", rows);
    chmod("000");
    try {
      assertNull(rows.poll(700, TimeUnit.MILLISECONDS), "Failed listing must not infer deletion");
      checkpointContains("known.csv");
    } finally {
      chmod("750");
    }
    write("blocked/recovered.csv", "recovered");
    row(rows, "recovered.csv", "CREATED");
    assertNull(rows.poll(500, TimeUnit.MILLISECONDS));
    stop(pipeline);
  }

  private void chmod(String mode) throws Exception {
    Session session =
        new JSch()
            .getSession(
                required("WATCHFILES_SFTP_USER"),
                required("WATCHFILES_SFTP_HOST"),
                Integer.parseInt(System.getenv().getOrDefault("WATCHFILES_SFTP_PORT", "22")));
    session.setPassword(required("WATCHFILES_SFTP_PASSWORD"));
    session.setConfig("StrictHostKeyChecking", "no");
    session.connect(2000);
    try {
      ChannelExec channel = (ChannelExec) session.openChannel("exec");
      channel.setCommand(
          "chmod " + mode + " '" + (remotePath + "/blocked").replace("'", "'\\''") + "'");
      channel.connect(2000);
      try {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!channel.isClosed() && System.nanoTime() < deadline) {
          Thread.sleep(10);
        }
        assertEquals(0, channel.getExitStatus(), "Remote test chmod failed");
      } finally {
        channel.disconnect();
      }
    } finally {
      session.disconnect();
    }
  }

  @Test
  void stopWakesLongRemotePollingWait() throws Exception {
    WatchFilesMeta meta = meta("COMPARE_WITH_STATE");
    meta.setPollingInterval("60000");
    LocalPipelineEngine pipeline = start(meta, "Watch", new LinkedBlockingQueue<>());
    Thread.sleep(150);
    stop(pipeline);
    // Reopening with the same identity also verifies that disposal released the state lock.
    stop(start(meta, "Again", new LinkedBlockingQueue<>()));
  }

  @Test
  void nativeRejectsSftpAndCorruptionIsPreservedWithoutLeakingLock() throws Exception {
    WatchFilesMeta meta = meta("COMPARE_WITH_STATE");
    meta.setStrategy("NATIVE");
    assertThrows(
        Exception.class, () -> prepare(meta, "Native", new LinkedBlockingQueue<>(), connection()));
    meta.setStrategy("POLLING");
    LocalPipelineEngine valid = start(meta, "Polling", new LinkedBlockingQueue<>());
    stop(valid);
    Path state = temporary.resolve("state/real-sftp.json");
    Files.writeString(state, "{broken");
    assertThrows(
        Exception.class, () -> prepare(meta, "Corrupt", new LinkedBlockingQueue<>(), connection()));
    assertEquals("{broken", Files.readString(state));
    try (JsonFileStateStore store =
        new JsonFileStateStore(state.getParent(), "real-sftp", "root", "scope", 100)) {
      assertThrows(java.io.IOException.class, store::load);
    }
  }

  @Test
  void sameNamedConnectionIsIsolatedBetweenPipelineExecutionContexts() throws Exception {
    LinkedBlockingQueue<Object[]> rows = new LinkedBlockingQueue<>();
    LocalPipelineEngine first = start(meta("IGNORE_EXISTING"), "Valid", rows);
    SftpConnection invalid = connection();
    invalid.setServerPort("65534");
    WatchFilesMeta second = meta("IGNORE_EXISTING");
    second.setWatchId("unreachable");
    assertThrows(
        Exception.class,
        () -> prepare(second, "Unreachable", new LinkedBlockingQueue<>(), invalid));
    write("still-watching.csv", "isolated");
    row(rows, "still-watching.csv", "CREATED");
    stop(first);
  }

  @Test
  void connectionLossRetainsCheckpointAndReconnectReconcilesOfflineChanges() throws Exception {
    write("known.csv", "known");
    LinkedBlockingQueue<Object[]> rows = new LinkedBlockingQueue<>();
    try (TcpRelay relay = new TcpRelay()) {
      SftpConnection proxied = connection();
      proxied.setServerName("127.0.0.1");
      proxied.setServerPort(Integer.toString(relay.port()));
      // JSch can retry timed-out reads before closing a session. The bounded stop expectation
      // includes those retries; it is not a promise that every provider returns after one timeout.
      proxied.setSessionTimeout("500");
      LocalPipelineEngine pipeline = prepare(meta("IGNORE_EXISTING"), "Watch", rows, proxied);
      pipeline.startThreads();
      relay.disconnect();
      try (FileObject known = remote.resolveFile("known.csv")) {
        known.delete();
      }
      write("offline.csv", "offline");
      assertNull(
          rows.poll(2600, TimeUnit.MILLISECONDS), "An outage must not produce false deletion");
      checkpointContains("known.csv");
      relay.resume();
      row(rows, "known.csv", "DELETED");
      row(rows, "offline.csv", "CREATED");
      checkpointContains("offline.csv");
      assertNull(rows.poll(500, TimeUnit.MILLISECONDS));
      relay.blackhole();
      Thread.sleep(300);
      long stoppingAt = System.nanoTime();
      stop(pipeline);
      System.out.println(
          "Blackholed SFTP stop completed in "
              + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stoppingAt)
              + " ms");
    }
  }

  /** A test-only byte relay disrupts this source's SSH connection without stopping shared sshd. */
  private static class TcpRelay implements AutoCloseable {
    private final ServerSocket listener =
        new ServerSocket(0, 5, java.net.InetAddress.getLoopbackAddress());
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final ExecutorService pipes = Executors.newFixedThreadPool(4);
    private final Thread acceptor;
    private volatile boolean accepting = true;
    private volatile boolean blackholed;

    TcpRelay() throws IOException {
      acceptor = new Thread(this::accept, "watchfiles-test-relay");
      acceptor.start();
    }

    int port() {
      return listener.getLocalPort();
    }

    private void accept() {
      while (!listener.isClosed()) {
        try {
          Socket client = listener.accept();
          if (!accepting) {
            client.close();
            continue;
          }
          Socket upstream = new Socket();
          try {
            upstream.connect(
                new InetSocketAddress(
                    required("WATCHFILES_SFTP_HOST"),
                    Integer.parseInt(System.getenv().getOrDefault("WATCHFILES_SFTP_PORT", "22"))),
                2000);
            sockets.add(client);
            sockets.add(upstream);
            if (!accepting) {
              closeSocket(client);
              closeSocket(upstream);
              continue;
            }
            pipes.submit(() -> pump(client, upstream));
            pipes.submit(() -> pump(upstream, client));
          } catch (IOException e) {
            closeSocket(client);
            closeSocket(upstream);
          }
        } catch (IOException e) {
          if (!listener.isClosed()) {
            throw new IllegalStateException("Test relay accept failed", e);
          }
        }
      }
    }

    private void pump(Socket source, Socket destination) {
      try {
        byte[] buffer = new byte[8192];
        int count;
        while ((count = source.getInputStream().read(buffer)) != -1) {
          if (!blackholed) {
            destination.getOutputStream().write(buffer, 0, count);
          }
        }
      } catch (IOException ignored) {
        /* Connection loss is the fault being injected. */
      } finally {
        closeSocket(source);
        closeSocket(destination);
      }
    }

    private void closeSocket(Socket socket) {
      sockets.remove(socket);
      try {
        socket.close();
      } catch (IOException ignored) {
        /* Test cleanup is best effort. */
      }
    }

    void disconnect() {
      accepting = false;
      sockets.forEach(this::closeSocket);
    }

    void resume() {
      accepting = true;
    }

    void blackhole() {
      blackholed = true;
    }

    @Override
    public void close() throws Exception {
      listener.close();
      disconnect();
      acceptor.join(2500);
      pipes.shutdownNow();
      assertFalse(acceptor.isAlive(), "Test relay acceptor leaked");
      assertTrue(pipes.awaitTermination(3, TimeUnit.SECONDS), "Test relay pipes leaked");
    }
  }
}
