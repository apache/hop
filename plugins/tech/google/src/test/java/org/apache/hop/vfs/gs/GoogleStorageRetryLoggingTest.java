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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hop.vfs.gs.config.GoogleCloudConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.bp.Duration;

/**
 * The storage client retries timeouts and temporary errors without telling anyone, which makes a
 * flaky connection indistinguishable from a hang. Each retry is now logged; these tests check what
 * a user gets to see, against a local endpoint and the real client.
 */
class GoogleStorageRetryLoggingTest {

  private static final String URI = "gs://bucket/file.txt";

  private static final byte[] CONTENT = "id;name\n1;one\n".getBytes(StandardCharsets.UTF_8);

  private static final int READ_TIMEOUT_MS = 250;

  private static final String UNAVAILABLE =
      "{\"error\":{\"code\":503,\"message\":\"Backend Error\",\"errors\":[{\"domain\":\"global\","
          + "\"reason\":\"backendError\",\"message\":\"Backend Error\"}]}}";

  private static final String OBJECT =
      "{\"kind\":\"storage#object\",\"bucket\":\"bucket\",\"name\":\"file.txt\","
          + "\"generation\":\"1\",\"metageneration\":\"1\",\"size\":\"14\"}";

  private static final String NOT_FOUND =
      "{\"error\":{\"code\":404,\"message\":\"No such object: bucket/file.txt\",\"errors\":"
          + "[{\"domain\":\"global\",\"reason\":\"notFound\",\"message\":\"No such object\"}]}}";

  private enum Answer {
    CONTENT,
    UNAVAILABLE,
    NOT_FOUND,
    STALL_AFTER_HEADERS
  }

  private HttpServer server;
  private ExecutorService handlers;

  /** The answers to the next requests, in order; once used up the object is served normally. */
  private final Queue<Answer> script = new ConcurrentLinkedQueue<>();

  private final CountDownLatch release = new CountDownLatch(1);
  private final List<String> logged = new CopyOnWriteArrayList<>();

  @BeforeEach
  void startEndpoint() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
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
    release.countDown();
    server.stop(0);
    handlers.shutdownNow();
  }

  private void handle(HttpExchange exchange) throws IOException {
    Answer answer = script.poll();
    switch (answer == null ? Answer.CONTENT : answer) {
      case CONTENT:
        if (!String.valueOf(exchange.getRequestURI().getQuery()).contains("alt=media")) {
          // A metadata call rather than a download.
          json(exchange, 200, OBJECT);
          break;
        }
        objectHeaders(exchange);
        exchange.sendResponseHeaders(200, CONTENT.length);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(CONTENT);
        }
        break;
      case UNAVAILABLE:
        json(exchange, 503, UNAVAILABLE);
        break;
      case NOT_FOUND:
        json(exchange, 404, NOT_FOUND);
        break;
      case STALL_AFTER_HEADERS:
        objectHeaders(exchange);
        exchange.sendResponseHeaders(200, CONTENT.length);
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        exchange.close();
        break;
    }
  }

  private static void objectHeaders(HttpExchange exchange) {
    exchange.getResponseHeaders().add("Content-Type", "text/plain");
    exchange.getResponseHeaders().add("x-goog-generation", "1");
    exchange.getResponseHeaders().add("x-goog-metageneration", "1");
  }

  private static void json(HttpExchange exchange, int code, String json) throws IOException {
    byte[] body = json.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");
    exchange.sendResponseHeaders(code, body.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(body);
    }
  }

  @Test
  void eachRetryOfATemporaryErrorIsLoggedWithTheObject() throws Exception {
    script.add(Answer.UNAVAILABLE);
    script.add(Answer.UNAVAILABLE);

    assertArrayEquals(CONTENT, read(storage(this::log)));

    assertEquals(2, logged.size(), "one line per retry: " + logged);
    for (String line : logged) {
      assertTrue(line.contains("Temporary error while accessing " + URI), line);
      assertTrue(line.contains("HTTP 503"), line);
      assertTrue(line.contains("trying again"), line);
    }
  }

  @Test
  void aTimeoutIsLoggedAsATimeout() throws Exception {
    script.add(Answer.STALL_AFTER_HEADERS);

    assertArrayEquals(CONTENT, read(storage(this::log)));

    assertEquals(1, logged.size(), "one line for the one timeout: " + logged);
    String line = logged.get(0);
    assertTrue(line.contains("No response in time while accessing " + URI), line);
    assertTrue(line.contains("Read timed out"), line);
    assertTrue(line.contains("the read timeout 20 seconds"), line);
  }

  @Test
  void anErrorThatIsNotRetriedIsNotLogged() {
    script.add(Answer.NOT_FOUND);

    assertThrows(IOException.class, () -> read(storage(this::log)));
    assertEquals(List.of(), logged);
  }

  @Test
  void aCallOutsideAnyObjectIsLoggedWithoutOne() {
    script.add(Answer.UNAVAILABLE);

    storage(this::log).get(BlobId.of("bucket", "file.txt"));

    assertEquals(1, logged.size(), "one line per retry: " + logged);
    String line = logged.get(0);
    assertTrue(line.startsWith("Temporary error, trying again: HTTP 503"), line);
  }

  /** The log is called from inside the client's retry decision; it must not be able to break it. */
  @Test
  void aFailingLogDoesNotChangeTheRetries() throws Exception {
    script.add(Answer.UNAVAILABLE);
    script.add(Answer.UNAVAILABLE);

    Storage storage =
        storage(
            message -> {
              throw new IllegalStateException("the logging system is not initialized");
            });

    assertArrayEquals(CONTENT, read(storage));
  }

  private void log(String message) {
    logged.add(message);
  }

  private static byte[] read(Storage storage) throws IOException {
    try (InputStream in =
        new ReadChannelInputStream(
            storage.reader(BlobId.of("bucket", "file.txt")),
            GoogleStorageStallWatchdog.Transfer.untracked(URI))) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      in.transferTo(out);
      return out.toByteArray();
    }
  }

  /**
   * Builds the client the way {@link GoogleStorageFileSystem#setupStorage()} does, pointed at the
   * local endpoint, with retry delays and timeouts shortened to keep the test fast. The timeouts
   * named in the log come from the configuration, which keeps its defaults.
   */
  private Storage storage(LoggingStorageRetryStrategy.RetryLog retryLog) {
    GoogleCloudConfig config = new GoogleCloudConfig();
    RetrySettings prompt =
        GoogleStorageFileSystem.buildRetrySettings(config).toBuilder()
            .setInitialRetryDelay(Duration.ofMillis(1))
            .setMaxRetryDelay(Duration.ofMillis(2))
            .build();

    return GoogleStorageFileSystem.buildStorageOptions(config, retryLog)
        .setRetrySettings(prompt)
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
}
