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

package org.apache.hop.lineage.openlineage;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.RunLifecycleLineagePayload;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Integration tests that drive the sink through its public {@link
 * org.apache.hop.lineage.spi.ILineageSink} contract (init / accept / shutdown) against a WireMock
 * OpenLineage collector.
 */
class OpenLineageSinkTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ENDPOINT = "/api/v1/lineage";
  private static final String PIPELINE_RUN = "11111111-1111-1111-1111-111111111111";

  @RegisterExtension
  static WireMockExtension wireMock =
      WireMockExtension.newInstance().options(wireMockConfig().dynamicPort()).build();

  @BeforeAll
  static void initLogStore() {
    HopLogStore.init();
  }

  @BeforeEach
  void stubOk() {
    wireMock.resetAll();
    wireMock.stubFor(
        post(urlEqualTo(ENDPOINT)).willReturn(aResponse().withStatus(201).withBody("{}")));
  }

  private Variables variables(String namespace) {
    Variables variables = new Variables();
    variables.setVariable(
        OpenLineageSinkConfig.VAR_URL, "http://localhost:" + wireMock.getPort() + ENDPOINT);
    if (namespace != null) {
      variables.setVariable(OpenLineageSinkConfig.VAR_NAMESPACE, namespace);
    }
    return variables;
  }

  private static LineageContext pipelineCtx(String name) {
    return LineageContext.builder()
        .subjectType(LineageSubjectType.PIPELINE)
        .pipelineName(name)
        .logChannelId(PIPELINE_RUN)
        .build();
  }

  @Test
  void deliversStartFileCompleteEvents() throws Exception {
    OpenLineageLineageSink sink = new OpenLineageLineageSink();
    sink.init(variables("hop-it"), new LogChannel("OpenLineageIT"));

    LineageContext ctx = pipelineCtx("integration-pipeline");
    sink.accept(
        List.of(
            new LineageEvent(
                "start",
                System.currentTimeMillis(),
                LineageEventKind.RUN_LIFECYCLE,
                ctx,
                new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null)),
            new LineageEvent(
                "file",
                System.currentTimeMillis(),
                LineageEventKind.FILE_IO,
                ctx,
                new FileIoLineagePayload(
                    FileIoOperation.READ, "file:///tmp/sample.csv", null, 100L, true, null)),
            new LineageEvent(
                "complete",
                System.currentTimeMillis(),
                LineageEventKind.RUN_LIFECYCLE,
                ctx,
                new RunLifecycleLineagePayload(RunLifecyclePhase.FINISHED, null))));
    sink.shutdown();

    wireMock.verify(3, postRequestedFor(urlEqualTo(ENDPOINT)));
    JsonNode firstBody =
        MAPPER.readTree(
            wireMock.findAll(postRequestedFor(urlEqualTo(ENDPOINT))).get(0).getBodyAsString());
    assertEquals("START", firstBody.get("eventType").asText());
    assertEquals("integration-pipeline", firstBody.get("job").get("name").asText());
  }

  @Test
  void postsBodyMatchingMapperOutput() throws Exception {
    Variables variables = variables("hop");
    OpenLineageLineageSink sink = new OpenLineageLineageSink();
    sink.init(variables, new LogChannel("OpenLineageBody"));

    LineageContext ctx = pipelineCtx("body-check");
    LineageEvent event =
        new LineageEvent(
            "body",
            1_700_000_000_000L,
            LineageEventKind.RUN_LIFECYCLE,
            ctx,
            new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null));
    sink.accept(List.of(event));
    sink.shutdown();

    OpenLineageEventMapper mapper =
        new OpenLineageEventMapper(
            "hop", OpenLineageSinkConfig.fromVariables(variables).getProducer());
    String expected = MAPPER.writeValueAsString(mapper.map(event).orElseThrow());
    wireMock.verify(postRequestedFor(urlEqualTo(ENDPOINT)).withRequestBody(equalToJson(expected)));
  }

  @Test
  void acceptReturnsQuicklyWhileHttpRunsAsync() throws Exception {
    wireMock.resetAll();
    wireMock.stubFor(
        post(urlEqualTo(ENDPOINT))
            .willReturn(aResponse().withFixedDelay(500).withStatus(201).withBody("{}")));

    OpenLineageLineageSink sink = new OpenLineageLineageSink();
    sink.init(variables(null), new LogChannel("OpenLineagePerf"));

    LineageEvent event =
        new LineageEvent(
            "perf",
            System.currentTimeMillis(),
            LineageEventKind.RUN_LIFECYCLE,
            pipelineCtx("perf"),
            new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null));

    long start = System.nanoTime();
    sink.accept(List.of(event));
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue(elapsedMs < 100, "accept took " + elapsedMs + "ms");

    sink.shutdown();
  }

  @Test
  void retriesTransientFailureThenSucceeds() throws Exception {
    wireMock.resetAll();
    wireMock.stubFor(
        post(urlEqualTo(ENDPOINT))
            .inScenario("retry")
            .whenScenarioStateIs(STARTED)
            .willReturn(aResponse().withStatus(503))
            .willSetStateTo("recovered"));
    wireMock.stubFor(
        post(urlEqualTo(ENDPOINT))
            .inScenario("retry")
            .whenScenarioStateIs("recovered")
            .willReturn(aResponse().withStatus(201).withBody("{}")));

    Variables variables = variables(null);
    variables.setVariable(OpenLineageSinkConfig.VAR_RETRY_BACKOFF_MS, "20");
    OpenLineageLineageSink sink = new OpenLineageLineageSink();
    sink.init(variables, new LogChannel("OpenLineageRetry"));

    sink.accept(
        List.of(
            new LineageEvent(
                "retry",
                System.currentTimeMillis(),
                LineageEventKind.RUN_LIFECYCLE,
                pipelineCtx("retry-pipeline"),
                new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null))));
    sink.shutdown();

    // One 503 + one 201 = the event was redelivered after the transient failure.
    wireMock.verify(2, postRequestedFor(urlEqualTo(ENDPOINT)));
  }
}
