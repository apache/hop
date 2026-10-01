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

package org.apache.hop.marketplace.resolve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers the streaming download loop: size reporting, progress callbacks, cancellation and partial
 * file cleanup. These are the guarantees the marketplace progress dialog is built on.
 */
class MavenRepositoryClientDownloadTest {

  private static final MavenCoordinates COORDS =
      new MavenCoordinates("org.apache.hop", "hop-datavault", "1.0.0");

  @TempDir private Path tempDir;

  /** Records everything a listener is told, so assertions can inspect the whole sequence. */
  private static class RecordingListener implements ITransferListener {
    private final List<Long> progress = new ArrayList<>();
    private String label;
    private long startedTotal = Long.MIN_VALUE;
    private long cancelAfterBytes = -1;

    @Override
    public void started(String label, long totalBytes) {
      this.label = label;
      this.startedTotal = totalBytes;
    }

    @Override
    public void transferred(long bytesSoFar, long totalBytes) {
      progress.add(bytesSoFar);
    }

    @Override
    public boolean isCancelled() {
      return cancelAfterBytes >= 0
          && !progress.isEmpty()
          && progress.get(progress.size() - 1) >= cancelAfterBytes;
    }
  }

  @Test
  void reportsTotalSizeAndProgressFromContentLength() throws Exception {
    byte[] payload = payload(200_000);
    MavenRepositoryClient client = clientReturning(payload, Map.of("content-length", "200000"));
    RecordingListener listener = new RecordingListener();
    Path target = tempDir.resolve("plugin.zip");

    Path result = client.downloadZip(repo(), COORDS, target, listener);

    assertEquals(target, result);
    assertEquals(200_000L, Files.size(target), "whole payload must reach disk");
    assertEquals(200_000L, listener.startedTotal, "size comes from Content-Length");
    assertEquals("org.apache.hop:hop-datavault:1.0.0", listener.label);
    assertFalse(listener.progress.isEmpty(), "progress must be reported per chunk");
    assertEquals(
        200_000L,
        listener.progress.get(listener.progress.size() - 1),
        "final callback must land exactly on the total, so the bar reaches 100%");
    assertTrue(
        listener.progress.size() > 2,
        "a 200KB payload spans several 64KB chunks; got " + listener.progress.size());
  }

  @Test
  void unknownSizeWhenContentLengthMissing() throws Exception {
    byte[] payload = payload(1000);
    MavenRepositoryClient client = clientReturning(payload, Map.of());
    RecordingListener listener = new RecordingListener();
    Path target = tempDir.resolve("plugin.zip");

    client.downloadZip(repo(), COORDS, target, listener);

    assertEquals(-1L, listener.startedTotal, "no Content-Length must report -1, not 0");
    assertEquals(1000L, Files.size(target));
  }

  @Test
  void unknownSizeWhenResponseIsEncoded() throws Exception {
    // Content-Length would describe the gzipped length, not what we write to disk.
    byte[] payload = payload(1000);
    MavenRepositoryClient client =
        clientReturning(payload, Map.of("content-length", "400", "content-encoding", "gzip"));
    RecordingListener listener = new RecordingListener();

    client.downloadZip(repo(), COORDS, tempDir.resolve("plugin.zip"), listener);

    assertEquals(-1L, listener.startedTotal, "encoded responses must not trust Content-Length");
  }

  @Test
  void cancelStopsTransferAndRemovesPartialFile() throws Exception {
    byte[] payload = payload(500_000);
    MavenRepositoryClient client = clientReturning(payload, Map.of("content-length", "500000"));
    RecordingListener listener = new RecordingListener();
    listener.cancelAfterBytes = 64 * 1024;
    Path target = tempDir.resolve("plugin.zip");

    HopException e =
        assertThrows(
            HopException.class, () -> client.downloadZip(repo(), COORDS, target, listener));

    assertTrue(e.getMessage().contains("cancelled"), "message was: " + e.getMessage());
    assertFalse(
        Files.exists(target),
        "a truncated zip must never survive — the unzip step would fail confusingly");
  }

  @Test
  void failedDownloadRemovesPartialFile() throws Exception {
    MavenRepositoryClient client = clientFailingMidStream(payload(300_000));
    Path target = tempDir.resolve("plugin.zip");

    assertThrows(
        HopException.class,
        () -> client.downloadZip(repo(), COORDS, target, ITransferListener.NONE));

    assertFalse(Files.exists(target), "partial download left behind after an I/O failure");
  }

  @Test
  void downloadWorksWithoutAListener() throws Exception {
    // The legacy signature must keep working for the CLI and for existing callers.
    MavenRepositoryClient client = clientReturning(payload(5000), Map.of("content-length", "5000"));
    Path target = tempDir.resolve("plugin.zip");

    Path result = client.downloadZip(repo(), COORDS, target);

    assertNotNull(result);
    assertEquals(5000L, Files.size(target));
  }

  private static byte[] payload(int size) {
    byte[] data = new byte[size];
    for (int i = 0; i < size; i++) {
      data[i] = (byte) (i % 251);
    }
    return data;
  }

  private static MarketplaceRepository repo() {
    return new MarketplaceRepository("test", "https://example.com/repository/hop/");
  }

  private static MavenRepositoryClient clientReturning(byte[] payload, Map<String, String> headers)
      throws Exception {
    return new MavenRepositoryClient(
        log(), stubClient(new ByteArrayInputStream(payload), headers, 200));
  }

  /** A stream that dies part way through, simulating a dropped connection. */
  private static MavenRepositoryClient clientFailingMidStream(byte[] payload) throws Exception {
    InputStream failing =
        new InputStream() {
          private int served;

          @Override
          public int read() throws IOException {
            return read(new byte[1], 0, 1);
          }

          @Override
          public int read(byte[] b, int off, int len) throws IOException {
            if (served >= 128 * 1024) {
              throw new IOException("connection reset");
            }
            int n = Math.min(len, payload.length - served);
            System.arraycopy(payload, served, b, off, n);
            served += n;
            return n;
          }
        };
    return new MavenRepositoryClient(
        log(), stubClient(failing, Map.of("content-length", String.valueOf(payload.length)), 200));
  }

  @SuppressWarnings("unchecked")
  private static HttpClient stubClient(
      InputStream body, Map<String, String> headers, int statusCode) throws Exception {
    HttpResponse<InputStream> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(statusCode);
    when(response.body()).thenReturn(body);
    Map<String, List<String>> headerMap = new java.util.LinkedHashMap<>();
    headers.forEach((k, v) -> headerMap.put(k, List.of(v)));
    when(response.headers()).thenReturn(HttpHeaders.of(headerMap, (a, b) -> true));

    HttpClient client = mock(HttpClient.class);
    when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn((HttpResponse<Object>) (HttpResponse<?>) response);
    return client;
  }

  /**
   * Mocked rather than a real {@link org.apache.hop.core.logging.LogChannel}, which would need the
   * central log store initialised for these plain unit tests.
   */
  private static ILogChannel log() {
    return mock(ILogChannel.class);
  }
}
