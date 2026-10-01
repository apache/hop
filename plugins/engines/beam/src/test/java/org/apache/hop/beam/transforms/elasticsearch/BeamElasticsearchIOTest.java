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
package org.apache.hop.beam.transforms.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BeamElasticsearchIOTest {
  @BeforeAll
  static void init() throws Exception {
    BeamHop.init();
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"7.17.22", "8.13.4"})
  void sourceUsesRealScrollIoAndReturnsJsonDocuments(String version) throws Exception {
    try (FakeElasticsearch server = new FakeElasticsearch(version)) {
      BeamElasticsearchInputMeta meta = new BeamElasticsearchInputMeta();
      set(meta, "Hosts", server.address());
      set(meta, "Index", "documents");
      set(meta, "Query", "{\"query\":{\"term\":{\"kind\":\"${KIND}\"}}}");
      Variables variables = new Variables();
      variables.setVariable("KIND", "test");
      Pipeline pipeline = pipeline();
      PipelineMeta hopPipeline = new PipelineMeta();
      TransformMeta transform = new TransformMeta("BeamElasticsearchInput", "elastic-read", meta);
      hopPipeline.addTransform(transform);
      Map<String, PCollection<HopRow>> collections = new java.util.HashMap<>();
      meta.handleTransform(
          new LogChannel("test"),
          variables,
          "direct",
          null,
          null,
          null,
          hopPipeline,
          transform,
          collections,
          pipeline,
          new RowMeta(),
          List.of(),
          null,
          null);
      PCollection<HopRow> output = collections.get("elastic-read");
      assertNotNull(output, "source handler must register its output");
      assertInstanceOf(HopRowCoder.class, output.getCoder());
      PAssert.that(output)
          .satisfies(
              rows -> {
                List<String> documents = new ArrayList<>();
                for (HopRow row : rows) {
                  assertEquals(1, row.getRow().length);
                  documents.add((String) row.getRow()[0]);
                }
                assertEquals(
                    List.of("{\"name\":\"first\"}", "{\"name\":\"second\"}").stream()
                        .sorted()
                        .toList(),
                    documents.stream().sorted().toList());
                return null;
              });
      pipeline.run().waitUntilFinish();
      assertTrue(
          server.requests.stream().anyMatch(r -> r.path().equals("/")), "Beam version discovery");
      assertTrue(
          server.requests.stream()
              .anyMatch(
                  r ->
                      r.path().equals("/documents/_search")
                          && r.body().contains("\"kind\":\"test\"")));
      assertTrue(
          server.requests.stream()
              .anyMatch(r -> r.method().equals("DELETE") && r.path().equals("/_search/scroll")),
          "scroll resources are released");
    }
  }

  @Test
  void sinkUsesRealBulkIoWithSelectedPayloadField() throws Exception {
    try (FakeElasticsearch server = new FakeElasticsearch("7.17.22")) {
      org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler handler =
          (org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler)
              Class.forName(
                      "org.apache.hop.beam.transforms.elasticsearch.BeamElasticsearchOutputMeta")
                  .getConstructor()
                  .newInstance();
      set(handler, "Hosts", server.address());
      set(handler, "Index", "documents");
      set(handler, "JsonField", "payload");
      RowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("ignored"));
      rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("payload"));
      Pipeline pipeline = pipeline();
      PCollection<HopRow> rows =
          pipeline.apply(
              org.apache.beam.sdk.transforms.Create.of(
                      new HopRow(new Object[] {"not-json", "{\"name\":\"one\"}"}),
                      new HopRow(new Object[] {"ignored", "{\"name\":\"two\"}"}))
                  .withCoder(new HopRowCoder()));
      TransformMeta transform =
          new TransformMeta(
              "BeamElasticsearchOutput",
              "elastic-write",
              (org.apache.hop.pipeline.transform.ITransformMeta) handler);
      Map<String, PCollection<HopRow>> collections = new java.util.HashMap<>();
      handler.handleTransform(
          new LogChannel("test"),
          new Variables(),
          "direct",
          null,
          null,
          null,
          new PipelineMeta(),
          transform,
          collections,
          pipeline,
          rowMeta,
          List.of(),
          rows,
          null);
      assertTrue(handler.isOutput());
      assertFalse(handler.isInput());
      assertTrue(collections.isEmpty(), "sink must not advertise downstream rows");
      org.apache.beam.sdk.PipelineResult result = pipeline.run();
      result.waitUntilFinish();
      long written =
          java.util.stream.StreamSupport.stream(
                  result
                      .metrics()
                      .queryMetrics(
                          org.apache.beam.sdk.metrics.MetricsFilter.builder()
                              .addNameFilter(
                                  org.apache.beam.sdk.metrics.MetricNameFilter.named(
                                      org.apache.hop.pipeline.Pipeline.METRIC_NAME_WRITTEN,
                                      "elastic-write"))
                              .build())
                      .getCounters()
                      .spliterator(),
                  false)
              .mapToLong(c -> c.getAttempted())
              .sum();
      assertEquals(2, written, "written counts successful bulk responses, not queued rows");
      List<String> payloads =
          server.requests.stream()
              .filter(r -> r.path().equals("/documents/_bulk"))
              .flatMap(r -> r.body().lines())
              .filter(line -> line.contains("\"name\""))
              .sorted()
              .toList();
      assertEquals(List.of("{\"name\":\"one\"}", "{\"name\":\"two\"}"), payloads);
      assertTrue(
          server.requests.stream()
              .filter(r -> r.path().endsWith("/_bulk"))
              .allMatch(r -> !r.body().contains("not-json") && !r.body().contains("ignored")));
    }
  }

  @Test
  void sinkBulkBodyKeepsNumericMeaning() throws Exception {
    String document =
        "{\"decimal\":0.123456789012345678901234567890,"
            + "\"id\":9007199254740993,"
            + "\"tiny\":1.23e-400,"
            + "\"huge\":1.23e+309,"
            + "\"wide\":123456789012345678901234567890,"
            + "\"nested\":{\"n\":0.100000000000000000000000000001},"
            + "\"list\":[9007199254740993]}";
    try (FakeElasticsearch server = new FakeElasticsearch("7.17.22")) {
      org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler handler =
          (org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler)
              Class.forName(
                      "org.apache.hop.beam.transforms.elasticsearch.BeamElasticsearchOutputMeta")
                  .getConstructor()
                  .newInstance();
      set(handler, "Hosts", server.address());
      set(handler, "Index", "documents");
      set(handler, "JsonField", "payload");
      RowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("payload"));
      Pipeline pipeline = pipeline();
      PCollection<HopRow> rows =
          pipeline.apply(
              org.apache.beam.sdk.transforms.Create.of(new HopRow(new Object[] {document}))
                  .withCoder(new HopRowCoder()));
      handler.handleTransform(
          new LogChannel("test"),
          new Variables(),
          "direct",
          null,
          null,
          null,
          new PipelineMeta(),
          new TransformMeta(
              "BeamElasticsearchOutput",
              "elastic-write",
              (org.apache.hop.pipeline.transform.ITransformMeta) handler),
          new java.util.HashMap<>(),
          pipeline,
          rowMeta,
          List.of(),
          rows,
          null);
      pipeline.run().waitUntilFinish();
      String body =
          server.requests.stream()
              .filter(r -> r.path().equals("/documents/_bulk"))
              .map(r -> r.body())
              .filter(b -> b.contains("\"decimal\""))
              .findFirst()
              .orElse("");
      assertFalse(body.isEmpty());
      BeamElasticsearchJsonFnTest.assertNumber(body, "decimal", "0.123456789012345678901234567890");
      BeamElasticsearchJsonFnTest.assertNumber(body, "id", "9007199254740993");
      BeamElasticsearchJsonFnTest.assertNumber(body, "tiny", "1.23e-400");
      BeamElasticsearchJsonFnTest.assertNumber(body, "huge", "1.23e+309");
      BeamElasticsearchJsonFnTest.assertNumber(body, "wide", "123456789012345678901234567890");
      BeamElasticsearchJsonFnTest.assertNumber(body, "n", "0.100000000000000000000000000001");
      BeamElasticsearchJsonFnTest.assertNumber(body, "list", "9007199254740993");
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"1.3.0", "2.19.0", "3.0.0"})
  void beamRejectsOpenSearchNativeMajorVersions(String version) throws Exception {
    try (FakeElasticsearch server = new FakeElasticsearch(version)) {
      BeamElasticsearchInputMeta meta = new BeamElasticsearchInputMeta();
      meta.setHosts(server.address());
      meta.setIndex("documents");
      Pipeline pipeline = pipeline();
      meta.handleTransform(
          null,
          new Variables(),
          "direct",
          null,
          null,
          null,
          new PipelineMeta(),
          new TransformMeta("BeamElasticsearchInput", "read", meta),
          new java.util.HashMap<>(),
          pipeline,
          new RowMeta(),
          List.of(),
          null,
          null);
      Exception error = assertThrows(Exception.class, () -> pipeline.run().waitUntilFinish());
      String causes = "";
      for (Throwable t = error; t != null; t = t.getCause()) {
        causes += t.getMessage();
      }
      assertTrue(causes.contains("only compatible with Elasticsearch [5, 6, 7, 8, 9]"), causes);
      assertTrue(server.requests.stream().anyMatch(r -> r.path().equals("/")));
      assertFalse(server.requests.stream().anyMatch(r -> r.path().equals("/documents/_search")));
    }
  }

  @Test
  void bulkItemFailuresFailThePipelineInsteadOfLosingDocuments() throws Exception {
    try (FakeElasticsearch server = new FakeElasticsearch("7.17.22")) {
      server.bulkErrors = true;
      BeamElasticsearchOutputMeta meta = BeamElasticsearchOutputMetaTest.configured();
      meta.setHosts(server.address());
      RowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("payload"));
      Pipeline pipeline = pipeline();
      PCollection<HopRow> rows =
          pipeline.apply(
              org.apache.beam.sdk.transforms.Create.of(new HopRow(new Object[] {"{\"x\":1}"}))
                  .withCoder(new HopRowCoder()));
      meta.handleTransform(
          null,
          new Variables(),
          "direct",
          null,
          null,
          null,
          new PipelineMeta(),
          new TransformMeta("BeamElasticsearchOutput", "write", meta),
          new java.util.HashMap<>(),
          pipeline,
          rowMeta,
          List.of(),
          rows,
          null);
      Exception error = assertThrows(Exception.class, () -> pipeline.run().waitUntilFinish());
      String causes = "";
      for (Throwable t = error; t != null; t = t.getCause()) {
        causes += t.getMessage();
      }
      assertTrue(causes.contains("Error writing to Elasticsearch"), causes);
      assertTrue(server.requests.stream().anyMatch(r -> r.path().endsWith("/_bulk")));
    }
  }

  static void set(Object meta, String property, String value) throws Exception {
    meta.getClass().getMethod("set" + property, String.class).invoke(meta, value);
  }

  static Pipeline pipeline() {
    DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
    options.setTargetParallelism(1);
    return Pipeline.create(options);
  }

  record Request(String method, String path, String body) {}

  static class FakeElasticsearch implements AutoCloseable {
    final HttpServer server;
    final List<Request> requests = new CopyOnWriteArrayList<>();
    final AtomicInteger scrolls = new AtomicInteger();
    final String version;
    volatile boolean bulkErrors;

    FakeElasticsearch(String version) throws Exception {
      this.version = version;
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext(
          "/",
          exchange -> {
            try {
              InputStream input = exchange.getRequestBody();
              if ("gzip".equals(exchange.getRequestHeaders().getFirst("Content-Encoding"))) {
                input = new GZIPInputStream(input);
              }
              String body = new String(input.readAllBytes(), StandardCharsets.UTF_8);
              String path = exchange.getRequestURI().getPath();
              requests.add(new Request(exchange.getRequestMethod(), path, body));
              String response;
              if (path.equals("/")) {
                response = "{\"version\":{\"number\":\"" + version + "\"}}";
              } else if (path.endsWith("/_stats")) {
                response =
                    "{\"_all\":{\"primaries\":{\"store\":{\"size_in_bytes\":100},\"docs\":{\"count\":2}}}}";
              } else if (path.endsWith("/_count")) {
                response = "{\"count\":2}";
              } else if (path.equals("/documents/_search")) {
                response =
                    new ObjectMapper().readTree(body).path("slice").path("id").asInt() == 0
                        ? hits("first")
                        : "{\"_scroll_id\":\"empty-slice\",\"hits\":{\"hits\":[]}}";
              } else if (path.equals("/_search/scroll")
                  && exchange.getRequestMethod().equals("DELETE")) {
                response = "{\"succeeded\":true,\"num_freed\":1}";
              } else if (path.equals("/_search/scroll")) {
                response =
                    scrolls.getAndIncrement() == 0
                        ? hits("second")
                        : "{\"_scroll_id\":\"test-scroll\",\"hits\":{\"hits\":[]}}";
              } else if (path.endsWith("/_bulk")) {
                if (bulkErrors) {
                  response =
                      "{\"errors\":true,\"items\":[{\"index\":{\"_index\":\"documents\",\"_id\":\"test-id\",\"status\":400,\"error\":{\"type\":\"mapper_parsing_exception\",\"reason\":\"test mapping rejection\"}}}]}";
                  byte[] failure = response.getBytes(StandardCharsets.UTF_8);
                  exchange.getResponseHeaders().set("Content-Type", "application/json");
                  exchange.sendResponseHeaders(200, failure.length);
                  exchange.getResponseBody().write(failure);
                  return;
                }
                long documents = body.lines().count() / 2;
                response =
                    "{\"errors\":false,\"items\":["
                        + java.util.stream.LongStream.range(0, documents)
                            .mapToObj(
                                i ->
                                    "{\"index\":{\"_index\":\"documents\",\"_id\":\""
                                        + i
                                        + "\",\"status\":201}}")
                            .collect(java.util.stream.Collectors.joining(","))
                        + "]}";
              } else {
                exchange.sendResponseHeaders(404, -1);
                return;
              }
              byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
              exchange.getResponseHeaders().set("Content-Type", "application/json");
              exchange.getResponseHeaders().set("X-Elastic-Product", "Elasticsearch");
              exchange.sendResponseHeaders(200, bytes.length);
              exchange.getResponseBody().write(bytes);
            } finally {
              exchange.close();
            }
          });
      server.start();
    }

    String hits(String name) {
      return "{\"_scroll_id\":\"test-scroll\",\"hits\":{\"hits\":[{\"_index\":\"documents\",\"_id\":\""
          + name
          + "\",\"_source\":{\"name\":\""
          + name
          + "\"}}]}}";
    }

    String address() {
      return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }
}
