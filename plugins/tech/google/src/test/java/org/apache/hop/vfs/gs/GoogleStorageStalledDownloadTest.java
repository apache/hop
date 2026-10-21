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
 *
 */

package org.apache.hop.vfs.gs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.NoCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.vfs.gs.config.GoogleCloudConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.bp.Duration;

/**
 * Reproduces a Text File Input (or any VFS reader) hanging forever on a {@code gs://} file.
 *
 * <p>The transform reads through {@link ReadChannelInputStream}, which wraps {@code
 * Storage.reader(...)}. In the JSON client that reader ends in {@code
 * ApiaryUnbufferedReadableByteChannel.read}, a {@code do { ... } while (true)} loop: when reading
 * the response body fails with an exception the client considers retryable - a {@code
 * SocketTimeoutException} or {@code SocketException} - it drops the response and opens the download
 * again. Opening the download is bounded by the configured retry settings, but the loop around the
 * body read counts nothing, so neither the number of attempts nor the total timeout is applied.
 *
 * <p>The result: an endpoint that answers with headers and then stops sending the body keeps the
 * reading thread busy forever, one read timeout per round. The same stall before the headers fails
 * after the configured attempts, which is what the user expects. On top of that, the client's
 * {@code close()} waits for the lock that the stuck {@code read()} holds, so closing the stream
 * from another thread cannot break the loop either.
 *
 * <p>Most tests here read through a stream the watchdog does not watch, to pin what the client does
 * on its own - the reason {@link GoogleStorageStallWatchdog} exists. If one of them starts failing,
 * the client may have been fixed. {@link #theWatchdogReportsAndThenEndsAStalledDownload()} checks
 * what Hop makes of the same stall.
 */
class GoogleStorageStalledDownloadTest {

  private static final byte[] CONTENT =
      "id;name\n1;one\n2;two\n3;three\n".getBytes(StandardCharsets.UTF_8);

  /** Two attempts allowed: a well-behaved client gives up after the second stalled download. */
  private static final int MAX_ATTEMPTS = 2;

  /** The configuration expresses this in whole seconds; milliseconds keep the test fast. */
  private static final int READ_TIMEOUT_MS = 250;

  /** How many more downloads than allowed we wait for before calling the read unbounded. */
  private static final int EXTRA_DOWNLOADS = 4;

  private enum Behaviour {
    HEALTHY,
    STALL_BEFORE_HEADERS,
    STALL_AFTER_HEADERS
  }

  private HttpServer server;
  private ExecutorService handlers;
  private volatile Behaviour behaviour = Behaviour.HEALTHY;
  private final AtomicInteger downloads = new AtomicInteger();
  private final CountDownLatch release = new CountDownLatch(1);
  private final List<String> logged = new CopyOnWriteArrayList<>();
  private final List<String> stalls = new CopyOnWriteArrayList<>();

  /** Overrides the configured total timeout when set. */
  private Duration totalTimeout;

  /** Looks for stalls every 50 milliseconds, interrupting the way it does in production. */
  private final GoogleStorageStallWatchdog watchdog =
      new GoogleStorageStallWatchdog(
          stalls::add,
          System::nanoTime,
          java.time.Duration.ofMillis(50),
          GoogleStorageStallWatchdog.daemonInterrupter());

  @BeforeEach
  void startEndpoint() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    // A stalled exchange parks its handler thread, so every request needs a thread of its own or
    // the next download would queue behind the stalled one and never reach the endpoint.
    handlers =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r, "fake-gcs-handler");
              t.setDaemon(true);
              return t;
            });
    server.setExecutor(handlers);
    server.createContext("/", this::handle);
    server.start();
  }

  @AfterEach
  void stopEndpoint() {
    watchdog.shutdown();
    release.countDown();
    server.stop(0);
    handlers.shutdownNow();
  }

  private void handle(HttpExchange exchange) throws IOException {
    downloads.incrementAndGet();
    switch (behaviour) {
      case HEALTHY:
        objectHeaders(exchange);
        exchange.sendResponseHeaders(200, CONTENT.length);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(CONTENT);
        }
        break;
      case STALL_BEFORE_HEADERS:
        awaitRelease();
        exchange.close();
        break;
      case STALL_AFTER_HEADERS:
        objectHeaders(exchange);
        // The headers are flushed here, so the client sees a successful open...
        exchange.sendResponseHeaders(200, CONTENT.length);
        // ...and then the body never arrives.
        awaitRelease();
        exchange.close();
        break;
    }
  }

  private static void objectHeaders(HttpExchange exchange) {
    exchange.getResponseHeaders().add("Content-Type", "text/plain");
    exchange.getResponseHeaders().add("x-goog-generation", "1");
    exchange.getResponseHeaders().add("x-goog-metageneration", "1");
    exchange.getResponseHeaders().add("x-goog-stored-content-length", "" + CONTENT.length);
  }

  private void awaitRelease() {
    try {
      release.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Sanity check of the fake endpoint: a normal object is read to the end with one download. */
  @Test
  void aHealthyDownloadIsReadToTheEnd() throws Exception {
    behaviour = Behaviour.HEALTHY;

    try (InputStream in = openStream()) {
      assertArrayEquals(CONTENT, readAll(in));
    }
    assertEquals(1, downloads.get());
  }

  /** Stalling before the headers is bounded: the read fails after the configured attempts. */
  @Test
  void aStallBeforeTheHeadersGivesUpAfterTheConfiguredAttempts() {
    behaviour = Behaviour.STALL_BEFORE_HEADERS;

    assertThrows(
        IOException.class,
        () -> {
          try (InputStream in = openStream()) {
            readAll(in);
          }
        });
    assertEquals(MAX_ATTEMPTS, downloads.get());
  }

  /**
   * The reported hang. The same stall after the headers is retried without any limit: the client
   * opens the download over and over, far beyond the configured attempts, and the read never
   * returns or fails.
   */
  @Test
  void aStallAfterTheHeadersIsRetriedForever() throws Exception {
    behaviour = Behaviour.STALL_AFTER_HEADERS;

    InputStream in = openStream();
    StuckReader reader = StuckReader.start(in);

    int expected = MAX_ATTEMPTS + EXTRA_DOWNLOADS;
    assertTrue(
        awaitDownloads(expected, 30_000),
        "expected the client to keep re-opening the download, saw only " + downloads.get());

    assertTrue(
        reader.thread.isAlive(),
        "the read finished or failed, so the retry loop is bounded after all: "
            + reader.outcome.get());
    assertNull(reader.outcome.get(), "the read should neither return nor fail");
    assertTrue(
        downloads.get() > MAX_ATTEMPTS,
        downloads.get() + " downloads were made although only " + MAX_ATTEMPTS + " are allowed");

    // The loop is no longer silent: every re-open follows a timeout that is logged.
    assertTrue(
        logged.size() >= expected - 1,
        "expected a logged timeout per re-open, got " + logged.size() + ": " + logged);
    assertTrue(
        logged.get(0).contains("No response in time while accessing gs://bucket/file.txt"),
        logged.get(0));
  }

  /**
   * What Hop makes of the same stall: the watchdog reports it, then ends it once the limit is
   * reached. The read fails with an error naming the file, the reading thread is not left
   * interrupted, and later reads fail too rather than pretend the file ended.
   */
  @Test
  void theWatchdogReportsAndThenEndsAStalledDownload() throws Exception {
    behaviour = Behaviour.STALL_AFTER_HEADERS;
    InputStream in = openWatchedStream();

    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicBoolean leftInterrupted = new AtomicBoolean(true);
    AtomicReference<Throwable> laterRead = new AtomicReference<>();
    Thread reader =
        new Thread(
            () -> {
              try {
                readAll(in);
              } catch (Throwable t) {
                failure.set(t);
              }
              leftInterrupted.set(Thread.currentThread().isInterrupted());
              try {
                in.read();
              } catch (Throwable t) {
                laterRead.set(t);
              }
            },
            "gcs-watched-reader");
    reader.setDaemon(true);
    reader.start();
    reader.join(15_000);

    assertFalse(reader.isAlive(), "the watchdog did not end the stalled read");
    assertInstanceOf(IOException.class, failure.get(), "the read should fail");
    String message = failure.get().getMessage();
    assertTrue(message.startsWith("No progress reading gs://bucket/file.txt for "), message);
    assertTrue(message.contains("giving up"), message);
    assertFalse(leftInterrupted.get(), "the interrupt that ended the read leaked out of it");
    assertInstanceOf(
        IOException.class, laterRead.get(), "a later read must fail too, not report end of file");
    assertTrue(
        stalls.stream().anyMatch(line -> line.endsWith("still waiting.")),
        "the stall should be reported before the read is ended: " + stalls);
  }

  /**
   * The total timeout does not end it either. The client only looks at it when an error is recorded
   * while opening the download, against a budget that starts afresh with every re-open; the failed
   * body reads in between are never counted against it. Here it is one second, and the read is
   * still going after three.
   */
  @Test
  void aStallAfterTheHeadersOutlivesTheTotalTimeout() throws Exception {
    behaviour = Behaviour.STALL_AFTER_HEADERS;
    totalTimeout = Duration.ofSeconds(1);

    InputStream in = openStream();
    StuckReader reader = StuckReader.start(in);
    TimeUnit.SECONDS.sleep(3);

    assertTrue(
        reader.thread.isAlive(),
        "the read ended, so the total timeout was applied after all: " + reader.outcome.get());
    assertNull(reader.outcome.get(), "the read should neither return nor fail");
    assertTrue(
        downloads.get() > MAX_ATTEMPTS,
        "expected the download to be re-opened beyond the configured attempts, saw "
            + downloads.get());
  }

  /**
   * Closing the stream from another thread (a pipeline stop, say) does not break the loop either:
   * the client's close() waits for the lock its own stuck read() holds, so the closing thread hangs
   * as well.
   */
  @Test
  void closingAStalledDownloadFromAnotherThreadBlocksToo() throws Exception {
    behaviour = Behaviour.STALL_AFTER_HEADERS;

    InputStream in = openStream();
    StuckReader reader = StuckReader.start(in);
    assertTrue(awaitDownloads(MAX_ATTEMPTS + 1, 30_000), "the read never got going");

    Thread closer = new Thread(StuckReader.quietly(in::close), "gcs-closer");
    closer.setDaemon(true);
    closer.start();
    closer.join(10L * READ_TIMEOUT_MS);

    assertTrue(closer.isAlive(), "close() returned while the read was stuck");
    assertTrue(reader.thread.isAlive(), "close() broke the stuck read");
  }

  /** A stream the watchdog leaves alone: what the client does by itself. */
  private InputStream openStream() {
    return new ReadChannelInputStream(
        storage().reader(BlobId.of("bucket", "file.txt")),
        GoogleStorageStallWatchdog.Transfer.untracked("gs://bucket/file.txt"));
  }

  /** A watched stream, reported after four read timeouts and ended after eight. */
  private InputStream openWatchedStream() {
    GoogleStorageStallWatchdog.Limits limits =
        new GoogleStorageStallWatchdog.Limits(
            java.time.Duration.ofMillis(4L * READ_TIMEOUT_MS),
            java.time.Duration.ofMillis(8L * READ_TIMEOUT_MS),
            java.time.Duration.ofMillis(4L * READ_TIMEOUT_MS));
    return new ReadChannelInputStream(
        storage().reader(BlobId.of("bucket", "file.txt")),
        watchdog.reading("gs://bucket/file.txt", limits));
  }

  /**
   * Builds the client the way {@link GoogleStorageFileSystem#setupStorage()} does, pointed at the
   * local endpoint. Retry delays are collapsed and the timeouts set in milliseconds to keep the
   * test fast; the attempt count - the budget the reader should respect - is configured as a user
   * would.
   */
  private Storage storage() {
    GoogleCloudConfig config = new GoogleCloudConfig();
    config.setMaxAttempts(Integer.toString(MAX_ATTEMPTS));

    RetrySettings.Builder prompt =
        GoogleStorageFileSystem.buildRetrySettings(config).toBuilder()
            .setInitialRetryDelay(Duration.ofMillis(1))
            .setMaxRetryDelay(Duration.ofMillis(2));
    if (totalTimeout != null) {
      prompt.setTotalTimeout(totalTimeout);
    }

    return GoogleStorageFileSystem.buildStorageOptions(config, logged::add)
        .setRetrySettings(prompt.build())
        .setTransportOptions(
            HttpTransportOptions.newBuilder()
                .setConnectTimeout(READ_TIMEOUT_MS)
                .setReadTimeout(READ_TIMEOUT_MS)
                .build())
        .setHost("http://127.0.0.1:" + server.getAddress().getPort())
        .setProjectId("hop-test")
        .setCredentials(NoCredentials.getInstance())
        .build()
        .getService();
  }

  private boolean awaitDownloads(int count, long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (downloads.get() < count) {
      if (System.currentTimeMillis() > deadline) {
        return false;
      }
      TimeUnit.MILLISECONDS.sleep(20);
    }
    return true;
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    in.transferTo(out);
    return out.toByteArray();
  }

  /** A transform thread reading the stream to the end, recording how the read ended, if ever. */
  private static final class StuckReader {
    private final AtomicReference<String> outcome = new AtomicReference<>();
    private Thread thread;

    static StuckReader start(InputStream in) {
      StuckReader reader = new StuckReader();
      reader.thread =
          new Thread(
              () -> {
                try {
                  reader.outcome.set("returned " + readAll(in).length + " bytes");
                } catch (Exception e) {
                  reader.outcome.set("failed with " + e);
                }
              },
              "gcs-reader");
      reader.thread.setDaemon(true);
      reader.thread.start();
      return reader;
    }

    static Runnable quietly(IoAction action) {
      return () -> {
        try {
          action.run();
        } catch (IOException ignored) {
          // only whether close() returns matters here
        }
      };
    }
  }

  @FunctionalInterface
  private interface IoAction {
    void run() throws IOException;
  }
}
