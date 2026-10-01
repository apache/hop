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

package org.apache.hop.beam.transforms.splunk;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.splunk.SplunkWriteError;
import org.apache.beam.sdk.transforms.Create;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.core.coder.SplunkWriteErrorCoder;
import org.apache.hop.beam.core.fn.SplunkWriteFailureFn;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Loopback HEC collector. Not a Splunk server. */
class BeamSplunkIOTest {
  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void writeErrorCoderKeepsTheStatusThatTheSchemaCoderDrops() throws Exception {
    SplunkWriteError original =
        SplunkWriteError.newBuilder()
            .withStatusCode(400)
            .withStatusMessage("Bad Request")
            .withPayload("secret-event-body")
            .create();
    var bytes = new ByteArrayOutputStream();
    SplunkWriteErrorCoder.of().encode(original, bytes);
    SplunkWriteError decoded =
        SplunkWriteErrorCoder.of().decode(new ByteArrayInputStream(bytes.toByteArray()));
    assertEquals(400, decoded.statusCode());
    assertEquals("secret-event-body", decoded.payload());
    String message = SplunkWriteFailureFn.message(decoded);
    assertEquals("Splunk HEC rejected an event with HTTP status 400", message);
    assertFalse(message.contains("secret-event-body"));
    assertFalse(message.contains("Bad Request"));
    assertEquals(
        "Splunk HEC write failed without an HTTP status",
        SplunkWriteFailureFn.message(SplunkWriteError.newBuilder().create()));
  }

  @Test
  @Timeout(60)
  void hecPostKeepsTheEventAndTokenAndCountsConversionNotWrites() throws Exception {
    try (Collector collector = new Collector(200)) {
      var result = publish(collector, "hello-event", true);
      assertEquals(1, collector.bodies.size());
      assertEquals("Splunk secret-token", collector.authorizations.get(0));
      String body = collector.bodies.get(0);
      assertTrue(body.contains("\"event\":\"hello-event\""), body);
      assertTrue(body.contains("\"index\":\"main\""), body);
      assertEquals(1, metric(result, org.apache.hop.pipeline.Pipeline.METRIC_NAME_READ));
      assertEquals(1, metric(result, org.apache.hop.pipeline.Pipeline.METRIC_NAME_OUTPUT));
      assertEquals(0, metric(result, org.apache.hop.pipeline.Pipeline.METRIC_NAME_WRITTEN));
    }
  }

  @Test
  @Timeout(60)
  void clientErrorFailsThePipelineOnceWithoutEchoingTheEventOrToken() throws Exception {
    try (Collector collector = new Collector(400)) {
      RuntimeException error =
          assertThrows(
              RuntimeException.class, () -> publish(collector, "secret-event-body", false));
      String message = String.valueOf(error);
      assertTrue(message.contains("HTTP status 400"), message);
      assertFalse(message.contains("secret-event-body"), message);
      assertFalse(message.contains("secret-token"), message);
      assertEquals(1, collector.bodies.size());
    }
  }

  private static org.apache.beam.sdk.PipelineResult publish(
      Collector collector, String event, boolean gzip) throws Exception {
    var rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("body"));
    var meta = new BeamSplunkOutputMeta();
    meta.setHecUrl(collector.url);
    meta.setToken("secret-token");
    meta.setEventField("body");
    meta.setBatchCount("1");
    meta.setIndex("main");
    meta.setEnableGzip(gzip);
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(Create.of(new HopRow(new Object[] {event})).withCoder(new HopRowCoder()))
        .apply(meta.buildOutputTransform(new Variables(), "splunk", rowMeta));
    var result = pipeline.run();
    result.waitUntilFinish();
    return result;
  }

  private static long metric(org.apache.beam.sdk.PipelineResult result, String name) {
    var metrics =
        result
            .metrics()
            .queryMetrics(
                org.apache.beam.sdk.metrics.MetricsFilter.builder()
                    .addNameFilter(
                        org.apache.beam.sdk.metrics.MetricNameFilter.named(name, "splunk"))
                    .build());
    long total = 0;
    for (var counter : metrics.getCounters()) total += counter.getAttempted();
    return total;
  }

  private static final class Collector implements AutoCloseable {
    final HttpServer server;
    final String url;
    final CopyOnWriteArrayList<String> bodies = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<String> authorizations = new CopyOnWriteArrayList<>();
    final AtomicInteger status;

    Collector(int statusCode) throws Exception {
      status = new AtomicInteger(statusCode);
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      url = "http://127.0.0.1:" + server.getAddress().getPort();
      server.createContext(
          "/services/collector/event",
          exchange -> {
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            byte[] raw = exchange.getRequestBody().readAllBytes();
            if ("gzip".equalsIgnoreCase(exchange.getRequestHeaders().getFirst("Content-Encoding")))
              raw = new GZIPInputStream(new ByteArrayInputStream(raw)).readAllBytes();
            bodies.add(new String(raw, StandardCharsets.UTF_8));
            byte[] response =
                (status.get() >= 400
                        ? "{\"text\":\"Error\",\"code\":6}"
                        : "{\"text\":\"Success\",\"code\":0}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status.get(), response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
          });
      server.start();
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }
}
